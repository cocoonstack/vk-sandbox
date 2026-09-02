package provider

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/go-logr/logr/funcr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"

	"github.com/cocoonstack/sandbox-operator/pkg/sandboxd"
)

// sandboxGVR is the owner CR resource this provider authorizes against.
var (
	sandboxGVR = schema.GroupVersionResource{Group: "agents.x-k8s.io", Version: "v1beta1", Resource: "sandboxes"}

	errTestReleaseFailed = errors.New("sandboxd unreachable")
)

// TestDeleteWithoutAuthorityPreservesAndAdopts is the core contract: with the
// owner CR alive, pod deletion must NOT release the sandbox, and the same-key
// replacement pod adopts the preserved claim without a second sandboxd claim.
func TestDeleteWithoutAuthorityPreservesAndAdopts(t *testing.T) {
	ctx := t.Context()
	sd := &fakeSandboxd{}
	p := newTestProvider(t, sd, dynWith(t, ownerSandbox("ns1", "sb-owner", "owner-uid", false)), "")

	pod := sandboxPod("ns1", "sb-pod", "uid-1", "sb-owner", "owner-uid")
	if err := p.CreatePod(ctx, pod); err != nil {
		t.Fatalf("CreatePod: %v", err)
	}
	if sd.claimCount() != 1 {
		t.Fatalf("want 1 sandboxd claim, got %d", sd.claimCount())
	}
	c1, ok := p.claimFor("ns1/sb-pod")
	if !ok {
		t.Fatal("claim not recorded")
	}

	if err := p.DeletePod(ctx, pod); err != nil {
		t.Fatalf("DeletePod: %v", err)
	}
	if got := sd.releaseCount(); got != 0 {
		t.Fatalf("pod deletion released the sandbox (%d releases): pod deletion is not VM authority", got)
	}
	if _, ok := p.claimFor("ns1/sb-pod"); !ok {
		t.Fatal("claim dropped on unauthorized delete; must be preserved for adopt-in-place")
	}

	pod2 := sandboxPod("ns1", "sb-pod", "uid-2", "sb-owner", "owner-uid")
	if err := p.CreatePod(ctx, pod2); err != nil {
		t.Fatalf("CreatePod replacement: %v", err)
	}
	if sd.claimCount() != 1 {
		t.Fatalf("replacement pod re-claimed (claims=%d); must adopt preserved claim", sd.claimCount())
	}
	c2, _ := p.claimFor("ns1/sb-pod")
	if c2.ID != c1.ID || c2.PodUID != "uid-2" {
		t.Fatalf("adopt-in-place broken: got id=%q uid=%q want id=%q uid=uid-2", c2.ID, c2.PodUID, c1.ID)
	}
}

// TestDeleteReleasesWhenOwnerGone: a structured NotFound naming the owner
// authorizes release; the claim and pod entries are dropped.
func TestDeleteReleasesWhenOwnerGone(t *testing.T) {
	ctx := t.Context()
	sd := &fakeSandboxd{}
	dyn := dynWith(t, ownerSandbox("ns1", "sb-owner", "owner-uid", false))
	p := newTestProvider(t, sd, dyn, "")

	pod := sandboxPod("ns1", "sb-pod", "uid-1", "sb-owner", "owner-uid")
	if err := p.CreatePod(ctx, pod); err != nil {
		t.Fatalf("CreatePod: %v", err)
	}
	if err := dyn.Tracker().Delete(sandboxGVR, "ns1", "sb-owner"); err != nil {
		t.Fatalf("delete owner: %v", err)
	}
	if err := p.DeletePod(ctx, pod); err != nil {
		t.Fatalf("DeletePod: %v", err)
	}
	if got := sd.releaseCount(); got != 1 {
		t.Fatalf("owner gone must authorize release; releases=%d", got)
	}
	if _, ok := p.claimFor("ns1/sb-pod"); ok {
		t.Fatal("claim must be dropped after authorized release")
	}
}

// TestDeleteReleasesOnOwnerTeardown: deletionTimestamp on the owner authorizes.
func TestDeleteReleasesOnOwnerTeardown(t *testing.T) {
	ctx := t.Context()
	sd := &fakeSandboxd{}
	p := newTestProvider(t, sd, dynWith(t, ownerSandbox("ns1", "sb-owner", "owner-uid", true)), "")

	pod := sandboxPod("ns1", "sb-pod", "uid-1", "sb-owner", "owner-uid")
	if err := p.CreatePod(ctx, pod); err != nil {
		t.Fatalf("CreatePod: %v", err)
	}
	if err := p.DeletePod(ctx, pod); err != nil {
		t.Fatalf("DeletePod: %v", err)
	}
	if got := sd.releaseCount(); got != 1 {
		t.Fatalf("owner teardown must authorize release; releases=%d", got)
	}
}

// TestDeletePreservesWhenOwnerUnverifiable: no dynamic client → preserve.
func TestDeletePreservesWhenOwnerUnverifiable(t *testing.T) {
	ctx := t.Context()
	sd := &fakeSandboxd{}
	p := newTestProvider(t, sd, nil, "")

	pod := sandboxPod("ns1", "sb-pod", "uid-1", "sb-owner", "owner-uid")
	if err := p.CreatePod(ctx, pod); err != nil {
		t.Fatalf("CreatePod: %v", err)
	}
	if err := p.DeletePod(ctx, pod); err != nil {
		t.Fatalf("DeletePod: %v", err)
	}
	if got := sd.releaseCount(); got != 0 {
		t.Fatalf("unverifiable owner must preserve; releases=%d", got)
	}
	if _, ok := p.claimFor("ns1/sb-pod"); !ok {
		t.Fatal("claim must survive unverifiable delete")
	}
}

// TestBarePodDeleteReleases: with no controller owner, the pod is its own
// authority and deletion releases.
func TestBarePodDeleteReleases(t *testing.T) {
	ctx := t.Context()
	sd := &fakeSandboxd{}
	p := newTestProvider(t, sd, dynWith(t), "")

	pod := sandboxPod("ns1", "bare", "uid-1", "", "")
	if err := p.CreatePod(ctx, pod); err != nil {
		t.Fatalf("CreatePod: %v", err)
	}
	if err := p.DeletePod(ctx, pod); err != nil {
		t.Fatalf("DeletePod: %v", err)
	}
	if got := sd.releaseCount(); got != 1 {
		t.Fatalf("bare pod delete must release; releases=%d", got)
	}
}

// TestStaleUIDDeleteIgnored: a DeletePod bearing a previous generation's UID
// must be a no-op.
func TestStaleUIDDeleteIgnored(t *testing.T) {
	ctx := t.Context()
	sd := &fakeSandboxd{}
	p := newTestProvider(t, sd, dynWith(t), "")

	cur := sandboxPod("ns1", "sb-pod", "uid-current", "", "")
	if err := p.CreatePod(ctx, cur); err != nil {
		t.Fatalf("CreatePod: %v", err)
	}
	stale := sandboxPod("ns1", "sb-pod", "uid-old", "", "")
	if err := p.DeletePod(ctx, stale); err != nil {
		t.Fatalf("DeletePod stale: %v", err)
	}
	if got := sd.releaseCount(); got != 0 {
		t.Fatalf("stale-UID delete must be ignored; releases=%d", got)
	}
	if _, ok := p.claimFor("ns1/sb-pod"); !ok {
		t.Fatal("claim must survive a stale-UID delete")
	}
}

// TestOrphanScanAuditOnly: the scan reports but never releases, and a failed
// list skips the cycle instead of reading as an empty node.
func TestOrphanScanAuditOnly(t *testing.T) {
	ctx := t.Context()
	sd := &fakeSandboxd{}
	p := newTestProvider(t, sd, dynWith(t), "")

	pod := sandboxPod("ns1", "sb-pod", "uid-1", "", "")
	if err := p.CreatePod(ctx, pod); err != nil {
		t.Fatalf("CreatePod: %v", err)
	}
	sd.mu.Lock()
	sd.live = append(sd.live, sandboxd.SandboxSummary{ID: "sb_orphan"})
	sd.mu.Unlock()

	orphans, stale, ok := p.OrphanScan(ctx)
	if !ok {
		t.Fatal("scan should succeed")
	}
	if len(orphans) != 1 || orphans[0] != "sb_orphan" {
		t.Fatalf("want orphan [sb_orphan], got %v", orphans)
	}
	if len(stale) != 0 {
		t.Fatalf("want no stale claims, got %v", stale)
	}
	if got := sd.releaseCount(); got != 0 {
		t.Fatalf("orphan scan released a sandbox (%d): background reconciliation is audit-only", got)
	}

	sd.mu.Lock()
	sd.listErr = errors.New("sandboxd down")
	sd.mu.Unlock()
	if _, _, ok := p.OrphanScan(ctx); ok {
		t.Fatal("failed list must skip the cycle")
	}
	if got := sd.releaseCount(); got != 0 {
		t.Fatalf("failed-list cycle must not release; releases=%d", got)
	}
}

// TestOrphanScanExternalClaimsAndLogDedup pins the #3 contract: a row whose
// claim_ref this provider does not hold is an apiserver-direct claim — not an
// orphan candidate — and every verdict (external, orphan, stale) is logged on
// first sight, not every cycle.
func TestOrphanScanExternalClaimsAndLogDedup(t *testing.T) {
	ctx := t.Context()
	sd := &fakeSandboxd{}
	p := newTestProvider(t, sd, dynWith(t), "")
	logLines := 0
	p.log = funcr.New(func(string, string) { logLines++ }, funcr.Options{})

	sd.mu.Lock()
	sd.live = append(sd.live,
		sandboxd.SandboxSummary{ID: "sb_ext", ClaimRef: "default/api-direct"},
		sandboxd.SandboxSummary{ID: "sb_orphan"})
	sd.mu.Unlock()
	p.claims["ns1/stale-pod"] = Claim{ID: "sb_stale"}

	for cycle := range 3 {
		orphans, stale, ok := p.OrphanScan(ctx)
		if !ok {
			t.Fatalf("cycle %d: scan failed", cycle)
		}
		if len(orphans) != 1 || orphans[0] != "sb_orphan" {
			t.Fatalf("cycle %d: want orphans [sb_orphan], got %v", cycle, orphans)
		}
		if len(stale) != 1 || stale[0] != "ns1/stale-pod" {
			t.Fatalf("cycle %d: want stale [ns1/stale-pod], got %v", cycle, stale)
		}
	}
	if logLines != 3 {
		t.Fatalf("verdict log lines = %d, want 3 (one per verdict, not per cycle)", logLines)
	}
}

// TestStateRoundTrip: claims persist across a provider restart so release
// credentials survive.
func TestStateRoundTrip(t *testing.T) {
	ctx := t.Context()
	statePath := filepath.Join(t.TempDir(), "claims.json")
	sd := &fakeSandboxd{}
	p1 := newTestProvider(t, sd, dynWith(t), statePath)

	pod := sandboxPod("ns1", "sb-pod", "uid-1", "", "")
	if err := p1.CreatePod(ctx, pod); err != nil {
		t.Fatalf("CreatePod: %v", err)
	}
	c1, _ := p1.claimFor("ns1/sb-pod")

	p2 := newTestProvider(t, sd, dynWith(t), statePath)
	c2, ok := p2.claimFor("ns1/sb-pod")
	if !ok {
		t.Fatal("claim lost across restart")
	}
	if c2.ID != c1.ID || c2.Token != c1.Token {
		t.Fatalf("restart lost claim identity: got %+v want %+v", c2, c1)
	}
}

// TestPluralResource pins the es/ies rules that prevented the sandboxs 404.
func TestPluralResource(t *testing.T) {
	cases := map[string]string{
		"sandbox":    "sandboxes",
		"cocoonset":  "cocoonsets",
		"replicaset": "replicasets",
		"policy":     "policies",
		"gateway":    "gateways",
		"batch":      "batches",
	}
	for in, want := range cases {
		if got := pluralResource(in); got != want {
			t.Errorf("pluralResource(%q) = %q, want %q", in, got, want)
		}
	}
}

// TestRuntimeMismatchRejected: a pod asking for a different runtime never
// reaches sandboxd.
func TestRuntimeMismatchRejected(t *testing.T) {
	ctx := t.Context()
	sd := &fakeSandboxd{}
	p := newTestProvider(t, sd, dynWith(t), "")

	pod := sandboxPod("ns1", "other", "uid-1", "", "")
	pod.Annotations[AnnRuntime] = "vk-cocoon"
	if err := p.CreatePod(ctx, pod); err == nil {
		t.Fatal("expected runtime mismatch error")
	}
	if sd.claimCount() != 0 {
		t.Fatalf("mismatched pod must not claim; claims=%d", sd.claimCount())
	}
}

func TestLoadStateReadsIndentedFileFromOlderBuild(t *testing.T) {
	path := t.TempDir() + "/claims.json"
	old := `{
  "claims": {
    "ns/pod-1": {
      "id": "sb_1",
      "token": "tok",
      "podUID": "u1"
    }
  }
}`
	if err := os.WriteFile(path, []byte(old), 0o600); err != nil {
		t.Fatalf("setup: %v", err)
	}
	p, err := New(t.Context(), Config{StatePath: path, Logger: logr.Discard()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if got, ok := p.claimFor("ns/pod-1"); !ok || got.ID != "sb_1" || got.Token != "tok" {
		t.Fatalf("indented state from an older build did not load: %+v ok=%v", got, ok)
	}
}

func TestConcurrentSaveStateNeverLosesAClaim(t *testing.T) {
	path := t.TempDir() + "/claims.json"
	p, err := New(t.Context(), Config{StatePath: path, Logger: logr.Discard()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	const writers = 32
	var wg sync.WaitGroup
	for w := range writers {
		wg.Go(func() {
			key := fmt.Sprintf("ns/pod-%d", w)
			p.mu.Lock()
			p.claims[key] = Claim{ID: fmt.Sprintf("sb_%d", w), Token: "tok", PodUID: "u"}
			p.mu.Unlock()
			p.saveState()
		})
	}
	wg.Wait()

	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var st stateFile
	if err := json.Unmarshal(b, &st); err != nil {
		t.Fatalf("claims table is corrupt after concurrent saves: %v", err)
	}
	if len(st.Claims) != writers {
		t.Fatalf("a stale snapshot overwrote a newer one: persisted %d claims, want %d", len(st.Claims), writers)
	}
}

func TestGetPodStatusStartTimeIsStable(t *testing.T) {
	p, err := New(t.Context(), Config{Logger: logr.Discard()})
	if err != nil {
		t.Fatal(err)
	}
	pod := &corev1.Pod{Namespace: "ns", Name: "p", UID: "u"}
	p.pods["ns/p"] = pod
	p.claims["ns/p"] = Claim{ID: "sb_1", Token: "t", Address: "10.0.0.5:7777", ClaimedAt: metav1.Now()}

	first, _ := p.GetPodStatus(t.Context(), "ns", "p")
	time.Sleep(20 * time.Millisecond)
	second, _ := p.GetPodStatus(t.Context(), "ns", "p")

	if !first.StartTime.Equal(second.StartTime) {
		t.Fatalf("StartTime moves between reads: %v then %v", first.StartTime, second.StartTime)
	}
}

func TestStartTimeIsStableForAClaimTableFromAnOlderBuild(t *testing.T) {
	path := t.TempDir() + "/claims.json"
	old := `{"claims":{"ns/p":{"id":"sb_1","token":"t","address":"10.0.0.5:7777","podUID":"u"}}}`
	if err := os.WriteFile(path, []byte(old), 0o600); err != nil {
		t.Fatal(err)
	}
	p, err := New(t.Context(), Config{StatePath: path, Logger: logr.Discard()})
	if err != nil {
		t.Fatal(err)
	}
	p.pods["ns/p"] = &corev1.Pod{Namespace: "ns", Name: "p", UID: "u"}

	first, _ := p.GetPodStatus(t.Context(), "ns", "p")
	time.Sleep(20 * time.Millisecond)
	second, _ := p.GetPodStatus(t.Context(), "ns", "p")
	if !first.StartTime.Equal(second.StartTime) {
		t.Fatalf("StartTime still moves after loading a pre-ClaimedAt table: %v then %v", first.StartTime, second.StartTime)
	}
}

func TestClaimedAtBackfillIsPersisted(t *testing.T) {
	path := t.TempDir() + "/claims.json"
	old := `{"claims":{"ns/p":{"id":"sb_1","token":"t","podUID":"u"}}}`
	if err := os.WriteFile(path, []byte(old), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := New(t.Context(), Config{StatePath: path, Logger: logr.Discard()}); err != nil {
		t.Fatal(err)
	}

	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), "claimedAt") {
		t.Fatalf("backfill was not written back: %s", b)
	}

	first, err := New(t.Context(), Config{StatePath: path, Logger: logr.Discard()})
	if err != nil {
		t.Fatal(err)
	}
	second, err := New(t.Context(), Config{StatePath: path, Logger: logr.Discard()})
	if err != nil {
		t.Fatal(err)
	}
	a, _ := first.claimFor("ns/p")
	c, _ := second.claimFor("ns/p")
	if !a.ClaimedAt.Equal(&c.ClaimedAt) {
		t.Fatalf("ClaimedAt changed across restarts: %v then %v", a.ClaimedAt, c.ClaimedAt)
	}
}

func TestSaveStateRecreatesADeletedStateDir(t *testing.T) {
	dir := t.TempDir() + "/nested"
	p, err := New(t.Context(), Config{StatePath: dir + "/claims.json", Logger: logr.Discard()})
	if err != nil {
		t.Fatal(err)
	}
	p.claims["ns/p"] = Claim{ID: "sb_1", Token: "t"}
	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
	p.saveState()

	if _, err := os.Stat(dir + "/claims.json"); err != nil {
		t.Fatalf("a deleted state dir permanently broke persistence: %v", err)
	}
}

func TestNewRefusesAnUnwritableClaimsPath(t *testing.T) {
	dir := t.TempDir()
	current := `{"claims":{"ns/p":{"id":"sb_1","token":"t","podUID":"u","claimedAt":"2026-01-01T00:00:00Z"}}}`
	if err := os.WriteFile(dir+"/claims.json", []byte(current), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })

	if _, err := New(t.Context(), Config{StatePath: dir + "/claims.json", Logger: logr.Discard()}); err == nil {
		t.Fatal("New accepted a state path it cannot write")
	}
}

func TestEveryClaimPathStampsClaimedAt(t *testing.T) {
	sd := &fakeSandboxd{}
	p, err := New(t.Context(), Config{NodeName: "n", Client: sd, StatePath: t.TempDir() + "/c.json", Logger: logr.Discard()})
	if err != nil {
		t.Fatal(err)
	}
	pod := sandboxPod("ns", "p", "u1", "", "")

	if err := p.CreatePod(t.Context(), pod); err != nil {
		t.Fatalf("CreatePod: %v", err)
	}
	fresh, _ := p.claimFor("ns/p")
	if fresh.ClaimedAt.IsZero() {
		t.Error("a fresh claim carries no ClaimedAt")
	}

	p.forgetPod("ns/p")
	p.mu.Lock()
	c := p.claims["ns/p"]
	c.ClaimedAt = metav1.Time{}
	p.claims["ns/p"] = c
	p.mu.Unlock()

	if err := p.CreatePod(t.Context(), sandboxPod("ns", "p", "u2", "", "")); err != nil {
		t.Fatalf("adopt CreatePod: %v", err)
	}
	adopted, _ := p.claimFor("ns/p")
	if adopted.ClaimedAt.IsZero() {
		t.Error("an adopted claim carries no ClaimedAt")
	}

	st, err := p.GetPodStatus(t.Context(), "ns", "p")
	if err != nil || st == nil {
		t.Fatalf("GetPodStatus: %v", err)
	}
	if st.StartTime.IsZero() || st.Phase != corev1.PodRunning {
		t.Errorf("status = %v / %v", st.Phase, st.StartTime)
	}
}

func TestNewFailsWhenTheClaimedAtMigrationCannotBePersisted(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/claims.json"
	legacy := `{"claims":{"ns/p":{"id":"sb_1","token":"t","podUID":"u"}}}`
	if err := os.WriteFile(path, []byte(legacy), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })

	if _, err := New(t.Context(), Config{StatePath: path, Logger: logr.Discard()}); err == nil {
		t.Fatal("New succeeded even though the claimedAt migration could not be written")
	}
}

func TestCreatePodReturnsTheSandboxWhenTheClaimCannotBePersisted(t *testing.T) {
	dir := t.TempDir()
	sd := &fakeSandboxd{}
	p, err := New(t.Context(), Config{NodeName: "n", Client: sd, StatePath: dir + "/claims.json", Logger: logr.Discard()})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })

	var pushed bool
	p.NotifyPods(t.Context(), func(*corev1.Pod) { pushed = true })

	err = p.CreatePod(t.Context(), sandboxPod("ns", "p", "u1", "", ""))
	if err == nil {
		t.Fatal("CreatePod reported success though the release credential was never stored")
	}
	if pushed {
		t.Error("the Pod was published as Running despite the failure")
	}
	if len(sd.releases) != 1 {
		t.Fatalf("the sandbox was not returned: releases=%v", sd.releases)
	}
	if _, ok := p.claimFor("ns/p"); ok {
		t.Error("a returned sandbox must not stay in the claims table")
	}
}

func TestCreatePodKeepsTheCredentialWhenTheUndoReleaseAlsoFails(t *testing.T) {
	dir := t.TempDir()
	sd := &fakeSandboxd{releaseErr: errTestReleaseFailed}
	p, err := New(t.Context(), Config{NodeName: "n", Client: sd, StatePath: dir + "/claims.json", Logger: logr.Discard()})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })

	if err := p.CreatePod(t.Context(), sandboxPod("ns", "p", "u1", "", "")); err == nil {
		t.Fatal("CreatePod reported success")
	}
	p.mu.RLock()
	_, held := p.claims["ns/p"]
	_, pending := p.tentative["ns/p"]
	p.mu.RUnlock()
	if !held {
		t.Error("the release credential was discarded even though the sandbox is still claimed")
	}
	if !pending {
		t.Error("a claim that never reached disk must stay tentative")
	}
	if _, ok := p.claimFor("ns/p"); ok {
		t.Error("a tentative claim must not be visible to Running or adoption")
	}
}

func TestATentativeClaimIsNeverReportedRunning(t *testing.T) {
	dir := t.TempDir()
	sd := &fakeSandboxd{releaseErr: errTestReleaseFailed}
	p, err := New(t.Context(), Config{NodeName: "n", Client: sd, StatePath: dir + "/claims.json", Logger: logr.Discard()})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })

	if err := p.CreatePod(t.Context(), sandboxPod("ns", "p", "u1", "", "")); err == nil {
		t.Fatal("CreatePod reported success")
	}

	st, err := p.GetPodStatus(t.Context(), "ns", "p")
	if err != nil {
		t.Fatal(err)
	}
	if st != nil && st.Phase == corev1.PodRunning {
		t.Fatal("a Pod whose release credential never reached disk was reported Running")
	}
}

func TestAStrandedClaimIsReturnedBeforeItsKeyIsReused(t *testing.T) {
	dir := t.TempDir()
	sd := &fakeSandboxd{releaseErr: errTestReleaseFailed}
	p, err := New(t.Context(), Config{NodeName: "n", Client: sd, StatePath: dir + "/claims.json", Logger: logr.Discard()})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })

	_ = p.CreatePod(t.Context(), sandboxPod("ns", "p", "u1", "", ""))
	p.mu.RLock()
	stranded := p.claims["ns/p"]
	p.mu.RUnlock()
	if stranded.ID == "" {
		t.Fatal("setup: expected a stranded claim")
	}

	if err := p.CreatePod(t.Context(), sandboxPod("ns", "p", "u2", "", "")); err == nil {
		t.Fatal("a replacement claimed over a stranded sandbox instead of failing")
	}
	p.mu.RLock()
	still := p.claims["ns/p"]
	p.mu.RUnlock()
	if still.ID != stranded.ID {
		t.Fatalf("the stranded credential was overwritten: %q -> %q", stranded.ID, still.ID)
	}

	sd.releaseErr = nil
	if err := p.CreatePod(t.Context(), sandboxPod("ns", "p", "u3", "", "")); err == nil {
		t.Log("create succeeded after the stranded sandbox was returned")
	}
	if len(sd.releases) == 0 {
		t.Error("the stranded sandbox was never returned")
	}
}

func TestATentativeClaimCanStillBeReleased(t *testing.T) {
	dir := t.TempDir()
	sd := &fakeSandboxd{releaseErr: errTestReleaseFailed}
	p, err := New(t.Context(), Config{NodeName: "n", Client: sd, StatePath: dir + "/claims.json", Logger: logr.Discard()})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })

	pod := sandboxPod("ns", "p", "u1", "", "")
	_ = p.CreatePod(t.Context(), pod)

	sd.releaseErr = nil
	if err := p.DeletePod(t.Context(), pod); err != nil {
		t.Fatalf("DeletePod: %v", err)
	}
	if len(sd.releases) != 1 {
		t.Fatalf("a tentative claim was never released: releases=%v", sd.releases)
	}
}

func TestStartupDropsAClaimTheNodeNoLongerHolds(t *testing.T) {
	path := t.TempDir() + "/claims.json"
	stale := `{"claims":{"ns/p":{"id":"sb_gone","token":"t","podUID":"u","claimedAt":"2026-01-01T00:00:00Z"}}}`
	if err := os.WriteFile(path, []byte(stale), 0o600); err != nil {
		t.Fatal(err)
	}
	sd := &fakeSandboxd{live: []sandboxd.SandboxSummary{{ID: "sb_other"}}}
	p, err := New(t.Context(), Config{StatePath: path, Lister: sd, Logger: logr.Discard()})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := p.heldClaimFor("ns/p"); ok {
		t.Fatal("a claim for a sandbox the node does not hold survived startup")
	}
}

func TestStartupKeepsTheTableWhenTheNodeCannotBeListed(t *testing.T) {
	path := t.TempDir() + "/claims.json"
	current := `{"claims":{"ns/p":{"id":"sb_live","token":"t","podUID":"u","claimedAt":"2026-01-01T00:00:00Z"}}}`
	if err := os.WriteFile(path, []byte(current), 0o600); err != nil {
		t.Fatal(err)
	}
	sd := &fakeSandboxd{listErr: errTestReleaseFailed}
	p, err := New(t.Context(), Config{StatePath: path, Lister: sd, Logger: logr.Discard()})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := p.heldClaimFor("ns/p"); !ok {
		t.Fatal("an unreadable sandboxd wiped the claims table")
	}
}

func TestCommitWritesOnlyItsOwnTentativeClaim(t *testing.T) {
	path := t.TempDir() + "/claims.json"
	p, err := New(t.Context(), Config{StatePath: path, Logger: logr.Discard()})
	if err != nil {
		t.Fatal(err)
	}
	p.mu.Lock()
	for _, k := range []string{"ns/a", "ns/b"} {
		p.claims[k] = Claim{ID: "sb_" + k, Token: "t", ClaimedAt: metav1.Now()}
		p.tentative[k] = struct{}{}
	}
	p.mu.Unlock()

	if err := p.commitClaim("ns/a"); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var st stateFile
	if err := json.Unmarshal(b, &st); err != nil {
		t.Fatal(err)
	}
	if _, ok := st.Claims["ns/a"]; !ok {
		t.Error("the committing claim was not written")
	}
	if _, ok := st.Claims["ns/b"]; ok {
		t.Error("a commit made someone else's tentative claim durable")
	}
}

func TestAnUnverifiedClaimIsNotAdoptedOrReportedRunning(t *testing.T) {
	path := t.TempDir() + "/claims.json"
	stale := `{"claims":{"ns/p":{"id":"sb_released","token":"t","podUID":"u","claimedAt":"2026-01-01T00:00:00Z"}}}`
	if err := os.WriteFile(path, []byte(stale), 0o600); err != nil {
		t.Fatal(err)
	}
	sd := &fakeSandboxd{listErr: errTestReleaseFailed}
	p, err := New(t.Context(), Config{NodeName: "n", Client: sd, Lister: sd, StatePath: path, Logger: logr.Discard()})
	if err != nil {
		t.Fatal(err)
	}

	if _, ok := p.claimFor("ns/p"); ok {
		t.Error("an unverified claim was offered for adoption")
	}
	p.pods["ns/p"] = sandboxPod("ns", "p", "u", "", "")
	st, err := p.GetPodStatus(t.Context(), "ns", "p")
	if err != nil {
		t.Fatal(err)
	}
	if st != nil && st.Phase == corev1.PodRunning {
		t.Error("an unverified claim was reported Running")
	}

	if _, ok := p.heldClaimFor("ns/p"); !ok {
		t.Error("an unverified claim must stay reachable for release")
	}

	sd.listErr = nil
	if !p.VerifyClaimsAgainstNode(t.Context()) {
		t.Fatal("verification did not run")
	}
	if _, ok := p.heldClaimFor("ns/p"); ok {
		t.Error("a released sandbox's row survived verification")
	}
}

func TestVerificationClearsTheQuarantineForALiveSandbox(t *testing.T) {
	path := t.TempDir() + "/claims.json"
	live := `{"claims":{"ns/p":{"id":"sb_live","token":"t","podUID":"u","claimedAt":"2026-01-01T00:00:00Z"}}}`
	if err := os.WriteFile(path, []byte(live), 0o600); err != nil {
		t.Fatal(err)
	}
	sd := &fakeSandboxd{listErr: errTestReleaseFailed}
	p, err := New(t.Context(), Config{NodeName: "n", Client: sd, Lister: sd, StatePath: path, Logger: logr.Discard()})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := p.claimFor("ns/p"); ok {
		t.Fatal("setup: expected the claim to start quarantined")
	}

	sd.listErr = nil
	sd.live = []sandboxd.SandboxSummary{{ID: "sb_live"}}
	p.VerifyClaimsAgainstNode(t.Context())

	if _, ok := p.claimFor("ns/p"); !ok {
		t.Error("a claim the node still holds stayed quarantined after a successful listing")
	}
}

func TestQuarantineLiftsWithoutTheOrphanScan(t *testing.T) {
	path := t.TempDir() + "/claims.json"
	live := `{"claims":{"ns/p":{"id":"sb_live","token":"t","podUID":"u","claimedAt":"2026-01-01T00:00:00Z"}}}`
	if err := os.WriteFile(path, []byte(live), 0o600); err != nil {
		t.Fatal(err)
	}
	sd := &fakeSandboxd{listErr: errTestReleaseFailed}
	p, err := New(t.Context(), Config{NodeName: "n", Client: sd, Lister: sd, StatePath: path, Logger: logr.Discard()})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := p.claimFor("ns/p"); ok {
		t.Fatal("setup: the claim should start quarantined")
	}

	done := make(chan struct{})
	go func() {
		p.RunClaimVerification(t.Context(), 5*time.Millisecond)
		close(done)
	}()

	sd.mu.Lock()
	sd.listErr = nil
	sd.live = []sandboxd.SandboxSummary{{ID: "sb_live"}}
	sd.mu.Unlock()

	deadline := time.After(2 * time.Second)
	for {
		if _, ok := p.claimFor("ns/p"); ok {
			break
		}
		select {
		case <-deadline:
			t.Fatal("the quarantine never lifted")
		case <-time.After(5 * time.Millisecond):
		}
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Error("verification kept running after the table was vouched for")
	}
}

func TestARestartDoesNotClaimOverAnUnverifiedSandbox(t *testing.T) {
	path := t.TempDir() + "/claims.json"
	loaded := `{"claims":{"ns/p":{"id":"sb_prev","token":"t","podUID":"u","claimedAt":"2026-01-01T00:00:00Z"}}}`
	if err := os.WriteFile(path, []byte(loaded), 0o600); err != nil {
		t.Fatal(err)
	}
	sd := &fakeSandboxd{listErr: errTestReleaseFailed}
	p, err := New(t.Context(), Config{NodeName: "n", Client: sd, Lister: sd, StatePath: path, Logger: logr.Discard()})
	if err != nil {
		t.Fatal(err)
	}

	if err := p.CreatePod(t.Context(), sandboxPod("ns", "p", "u", "", "")); err == nil {
		t.Fatal("a create claimed over an unverified sandbox instead of refusing")
	}
	if sd.claims != 0 {
		t.Errorf("a second sandbox was claimed: claims=%d", sd.claims)
	}
	held, ok := p.heldClaimFor("ns/p")
	if !ok || held.ID != "sb_prev" {
		t.Errorf("the previous credential was lost: %+v ok=%v", held, ok)
	}
}

func TestVerificationLeavesRowsItWasNeverAskedToJudge(t *testing.T) {
	p, err := New(t.Context(), Config{NodeName: "n", Logger: logr.Discard()})
	if err != nil {
		t.Fatal(err)
	}
	hook := &listAfterHook{}
	p.lister = hook
	hook.onList = func() {
		p.mu.Lock()
		p.claims["ns/new"] = Claim{ID: "sb_new", Token: "t"}
		p.mu.Unlock()
	}
	p.mu.Lock()
	p.claims["ns/old"] = Claim{ID: "sb_old", Token: "t"}
	p.quarantined["ns/old"] = struct{}{}
	p.mu.Unlock()

	if !p.VerifyClaimsAgainstNode(t.Context()) {
		t.Fatal("verification did not run")
	}
	if _, ok := p.heldClaimFor("ns/new"); !ok {
		t.Error("a row created during the listing was judged by it")
	}
	if _, ok := p.heldClaimFor("ns/old"); ok {
		t.Error("a quarantined row the node does not hold survived")
	}
}

func TestAVouchedForSandboxIsAdoptedOnTheSamePass(t *testing.T) {
	path := t.TempDir() + "/claims.json"
	loaded := `{"claims":{"ns/p":{"id":"sb_prev","token":"t","podUID":"u","claimedAt":"2026-01-01T00:00:00Z"}}}`
	if err := os.WriteFile(path, []byte(loaded), 0o600); err != nil {
		t.Fatal(err)
	}
	sd := &fakeSandboxd{listErr: errTestReleaseFailed}
	p, err := New(t.Context(), Config{NodeName: "n", Client: sd, Lister: sd, StatePath: path, Logger: logr.Discard()})
	if err != nil {
		t.Fatal(err)
	}

	sd.mu.Lock()
	sd.listErr = nil
	sd.live = []sandboxd.SandboxSummary{{ID: "sb_prev"}}
	sd.mu.Unlock()

	if err := p.CreatePod(t.Context(), sandboxPod("ns", "p", "u", "", "")); err != nil {
		t.Fatalf("CreatePod: %v", err)
	}
	if sd.claims != 0 {
		t.Errorf("a new sandbox was claimed instead of adopting the vouched-for one: claims=%d", sd.claims)
	}
	c, ok := p.claimFor("ns/p")
	if !ok || c.ID != "sb_prev" {
		t.Errorf("the vouched-for sandbox was not adopted: %+v ok=%v", c, ok)
	}
}

func TestClaimCarriesItsPodKey(t *testing.T) {
	sd := &fakeSandboxd{}
	p, err := New(t.Context(), Config{NodeName: "n", Client: sd, Logger: logr.Discard()})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.CreatePod(t.Context(), sandboxPod("ns", "p", "u", "", "")); err != nil {
		t.Fatal(err)
	}
	if sd.lastSpec.ClaimRef != "ns/p" {
		t.Errorf("ClaimRef = %q, want %q", sd.lastSpec.ClaimRef, "ns/p")
	}
}

func TestAdoptionDoesNotResurrectARowVerificationRemoved(t *testing.T) {
	sd := &fakeSandboxd{}
	p, err := New(t.Context(), Config{NodeName: "n", Client: sd, Logger: logr.Discard()})
	if err != nil {
		t.Fatal(err)
	}
	p.mu.Lock()
	p.claims["ns/p"] = Claim{ID: "sb_gone", Token: "t"}
	p.mu.Unlock()

	p.mu.Lock()
	delete(p.claims, "ns/p")
	p.mu.Unlock()

	if _, ok := p.adoptExistingClaim("ns/p", sandboxPod("ns", "p", "u", "", "")); ok {
		t.Fatal("adoption resurrected a row verification had removed")
	}
	if _, ok := p.heldClaimFor("ns/p"); ok {
		t.Error("the removed row came back")
	}
}

func TestAbsentTTLAnnotationRequestsTheFullLease(t *testing.T) {
	sd := &fakeSandboxd{}
	p, err := New(t.Context(), Config{NodeName: "n", Client: sd, Logger: logr.Discard()})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.CreatePod(t.Context(), sandboxPod("ns", "p", "u", "", "")); err != nil {
		t.Fatal(err)
	}
	if sd.lastSpec.TTLSeconds != defaultClaimTTLSeconds {
		t.Errorf("TTLSeconds = %d, want %d", sd.lastSpec.TTLSeconds, defaultClaimTTLSeconds)
	}
}

func TestClaimRecordsTheLeaseDeadline(t *testing.T) {
	want := time.Now().Add(24 * time.Hour).UTC().Truncate(time.Second)
	sd := &fakeSandboxd{deadline: want}
	p, err := New(t.Context(), Config{NodeName: "n", Client: sd, Logger: logr.Discard()})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.CreatePod(t.Context(), sandboxPod("ns", "p", "u", "", "")); err != nil {
		t.Fatal(err)
	}
	c, ok := p.claimFor("ns/p")
	if !ok || !c.Deadline.Time.Equal(want) {
		t.Errorf("Deadline = %v ok=%v, want %v", c.Deadline, ok, want)
	}
}

func TestAPodPastItsLeaseIsNotReportedRunning(t *testing.T) {
	p, err := New(t.Context(), Config{NodeName: "n", Logger: logr.Discard()})
	if err != nil {
		t.Fatal(err)
	}
	p.mu.Lock()
	p.pods["ns/p"] = sandboxPod("ns", "p", "u", "", "")
	p.claims["ns/p"] = Claim{
		ID: "sb_reaped", Token: "t",
		ClaimedAt: metav1.NewTime(time.Now().Add(-10 * time.Minute)),
		Deadline:  metav1.NewTime(time.Now().Add(-5 * time.Minute)),
	}
	p.mu.Unlock()

	st, err := p.GetPodStatus(t.Context(), "ns", "p")
	if err != nil {
		t.Fatal(err)
	}
	if st.Phase == corev1.PodRunning {
		t.Fatal("a pod whose sandbox lease ended was still reported Running")
	}
	if st.Phase != corev1.PodFailed || st.Reason != ReasonLeaseExpired {
		t.Errorf("phase/reason = %v/%v", st.Phase, st.Reason)
	}
	if _, ok := p.heldClaimFor("ns/p"); !ok {
		t.Error("the expired claim's credential must stay reachable for release")
	}
}

func TestAClaimWithNoKnownDeadlineStaysRunning(t *testing.T) {
	p, err := New(t.Context(), Config{NodeName: "n", Logger: logr.Discard()})
	if err != nil {
		t.Fatal(err)
	}
	p.mu.Lock()
	p.pods["ns/p"] = sandboxPod("ns", "p", "u", "", "")
	p.claims["ns/p"] = Claim{ID: "sb_old", Token: "t", ClaimedAt: metav1.Now()}
	p.mu.Unlock()

	st, err := p.GetPodStatus(t.Context(), "ns", "p")
	if err != nil {
		t.Fatal(err)
	}
	if st.Phase != corev1.PodRunning {
		t.Errorf("phase = %v, want Running", st.Phase)
	}
}

func TestLeaseWatchPublishesFailedForAReapedSandbox(t *testing.T) {
	p, err := New(t.Context(), Config{NodeName: "n", Logger: logr.Discard()})
	if err != nil {
		t.Fatal(err)
	}
	got := make(chan *corev1.Pod, 8)
	p.NotifyPods(t.Context(), func(pod *corev1.Pod) { got <- pod })

	p.mu.Lock()
	p.pods["ns/p"] = sandboxPod("ns", "p", "u", "", "")
	p.claims["ns/p"] = Claim{
		ID: "sb_reaped", Token: "t",
		ClaimedAt: metav1.NewTime(time.Now().Add(-2 * time.Hour)),
		Deadline:  metav1.NewTime(time.Now().Add(-time.Hour)),
	}
	p.mu.Unlock()

	go p.RunLeaseWatch(t.Context(), 5*time.Millisecond)

	select {
	case pod := <-got:
		if pod.Status.Phase != corev1.PodFailed || pod.Status.Reason != ReasonLeaseExpired {
			t.Fatalf("published %v/%v, want Failed/%s", pod.Status.Phase, pod.Status.Reason, ReasonLeaseExpired)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("lease expiry was never published")
	}
}

func TestAnExpiredClaimIsReplacedNotAdopted(t *testing.T) {
	sd := &fakeSandboxd{}
	p, err := New(t.Context(), Config{NodeName: "n", Client: sd, Logger: logr.Discard()})
	if err != nil {
		t.Fatal(err)
	}
	p.mu.Lock()
	p.claims["ns/p"] = Claim{
		ID: "sb_reaped", Token: "t",
		ClaimedAt: metav1.NewTime(time.Now().Add(-2 * time.Hour)),
		Deadline:  metav1.NewTime(time.Now().Add(-time.Hour)),
	}
	p.mu.Unlock()

	if err := p.CreatePod(t.Context(), sandboxPod("ns", "p", "u", "", "")); err != nil {
		t.Fatalf("CreatePod: %v", err)
	}
	if sd.claims != 1 {
		t.Fatalf("claims = %d, want a fresh claim", sd.claims)
	}
	c, ok := p.claimFor("ns/p")
	if !ok || c.ID == "sb_reaped" {
		t.Fatalf("claim = %+v ok=%v, want the reaped row replaced", c, ok)
	}
}

func TestVerificationBackfillsALegacyDeadlineFromTheListing(t *testing.T) {
	path := t.TempDir() + "/claims.json"
	legacy := `{"claims":{"ns/p":{"id":"sb_live","token":"t","podUID":"u","claimedAt":"2026-01-01T00:00:00Z"}}}`
	if err := os.WriteFile(path, []byte(legacy), 0o600); err != nil {
		t.Fatal(err)
	}
	want := time.Now().Add(3 * time.Hour).UTC().Truncate(time.Second)
	sd := &fakeSandboxd{live: []sandboxd.SandboxSummary{{ID: "sb_live", Deadline: want}}}
	p, err := New(t.Context(), Config{NodeName: "n", Client: sd, Lister: sd, StatePath: path, Logger: logr.Discard()})
	if err != nil {
		t.Fatal(err)
	}
	c, ok := p.claimFor("ns/p")
	if !ok || !c.Deadline.Time.Equal(want) {
		t.Fatalf("Deadline = %v ok=%v, want %v", c.Deadline, ok, want)
	}
}

func TestTheWatchConfirmsWithTheNodeBeforePublishingFailure(t *testing.T) {
	sd := &fakeSandboxd{live: []sandboxd.SandboxSummary{{
		ID: "sb_archived", Deadline: time.Now().Add(6 * time.Hour),
	}}}
	p, err := New(t.Context(), Config{NodeName: "n", Client: sd, Lister: sd, Logger: logr.Discard()})
	if err != nil {
		t.Fatal(err)
	}
	notified := make(chan *corev1.Pod, 4)
	p.NotifyPods(t.Context(), func(pod *corev1.Pod) { notified <- pod })
	p.mu.Lock()
	p.pods["ns/p"] = sandboxPod("ns", "p", "u", "", "")
	p.claims["ns/p"] = Claim{ID: "sb_archived", Token: "t", Deadline: metav1.NewTime(time.Now().Add(-time.Hour))}
	p.mu.Unlock()

	p.publishExpiredLeases(t.Context())
	select {
	case pod := <-notified:
		t.Fatalf("a live, listed claim was published %v", pod.Status.Phase)
	default:
	}
	c, _ := p.heldClaimFor("ns/p")
	if !c.Deadline.After(time.Now()) {
		t.Fatalf("the node's lease was not adopted: %v", c.Deadline)
	}

	sd.mu.Lock()
	sd.live = nil
	sd.mu.Unlock()
	p.refreshDeadline("ns/p", "sb_archived", time.Now().Add(-time.Minute))
	p.publishExpiredLeases(t.Context())
	select {
	case pod := <-notified:
		if pod.Status.Phase != corev1.PodFailed {
			t.Fatalf("published %v, want Failed", pod.Status.Phase)
		}
	default:
		t.Fatal("a confirmed-gone claim was never published as Failed")
	}
}

func TestTheWatchPublishesNothingWhenTheNodeCannotBeListed(t *testing.T) {
	sd := &fakeSandboxd{listErr: errTestReleaseFailed}
	p, err := New(t.Context(), Config{NodeName: "n", Client: sd, Lister: sd, Logger: logr.Discard()})
	if err != nil {
		t.Fatal(err)
	}
	notified := make(chan *corev1.Pod, 4)
	p.NotifyPods(t.Context(), func(pod *corev1.Pod) { notified <- pod })
	p.mu.Lock()
	p.pods["ns/p"] = sandboxPod("ns", "p", "u", "", "")
	p.claims["ns/p"] = Claim{ID: "sb_x", Token: "t", Deadline: metav1.NewTime(time.Now().Add(-time.Hour))}
	p.mu.Unlock()

	p.publishExpiredLeases(t.Context())
	select {
	case pod := <-notified:
		t.Fatalf("published %v with an unlistable node", pod.Status.Phase)
	default:
	}
}

func TestAStillListedExpiredClaimIsAdoptedNotReplaced(t *testing.T) {
	sd := &fakeSandboxd{live: []sandboxd.SandboxSummary{{ID: "sb_archived"}}}
	p, err := New(t.Context(), Config{NodeName: "n", Client: sd, Lister: sd, Logger: logr.Discard()})
	if err != nil {
		t.Fatal(err)
	}
	p.mu.Lock()
	p.claims["ns/p"] = Claim{ID: "sb_archived", Token: "t", ClaimedAt: metav1.Now(), Deadline: metav1.NewTime(time.Now().Add(-time.Hour))}
	p.mu.Unlock()

	if err := p.CreatePod(t.Context(), sandboxPod("ns", "p", "u", "", "")); err != nil {
		t.Fatalf("CreatePod: %v", err)
	}
	if sd.claims != 0 {
		t.Fatalf("claims = %d; a live archived claim was replaced", sd.claims)
	}
	c, ok := p.claimFor("ns/p")
	if !ok || c.ID != "sb_archived" || !c.Deadline.IsZero() {
		t.Fatalf("claim = %+v ok=%v, want the archived row adopted with its keep-forever lease", c, ok)
	}
}

func TestTheWatchDoesNotTerminalizeAReplacementPod(t *testing.T) {
	p, err := New(t.Context(), Config{NodeName: "n", Logger: logr.Discard()})
	if err != nil {
		t.Fatal(err)
	}
	hook := &listAfterHook{}
	p.lister = hook
	notified := make(chan *corev1.Pod, 4)
	p.NotifyPods(t.Context(), func(pod *corev1.Pod) { notified <- pod })
	p.mu.Lock()
	p.pods["ns/p"] = sandboxPod("ns", "p", "u-old", "", "")
	p.claims["ns/p"] = Claim{ID: "sb_old", Token: "t", Deadline: metav1.NewTime(time.Now().Add(-time.Hour))}
	p.mu.Unlock()
	hook.onList = func() {
		p.mu.Lock()
		p.pods["ns/p"] = sandboxPod("ns", "p", "u-new", "", "")
		p.claims["ns/p"] = Claim{ID: "sb_new", Token: "t2", Deadline: metav1.NewTime(time.Now().Add(time.Hour))}
		p.mu.Unlock()
	}

	p.publishExpiredLeases(t.Context())
	select {
	case pod := <-notified:
		t.Fatalf("the replacement pod was published %v with the predecessor's expiry", pod.Status.Phase)
	default:
	}
}

// listAfterHook lets a test slip a claim in between the listing and the lock.
type listAfterHook struct {
	onList func()
}

func (l *listAfterHook) Sandboxes(_ context.Context) ([]sandboxd.SandboxSummary, error) {
	if l.onList != nil {
		l.onList()
	}
	return nil, nil
}

// fakeSandboxd implements SandboxdClient + Lister with call accounting.
type fakeSandboxd struct {
	mu         sync.Mutex
	claims     int
	releases   []string
	listErr    error
	live       []sandboxd.SandboxSummary
	claimErr   error
	releaseErr error
	lastSpec   sandboxd.ClaimSpec
	deadline   time.Time
}

func (f *fakeSandboxd) Claim(_ context.Context, spec sandboxd.ClaimSpec) (sandboxd.ClaimResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lastSpec = spec
	if f.claimErr != nil {
		return sandboxd.ClaimResult{}, f.claimErr
	}
	f.claims++
	id := fmt.Sprintf("sb_%06d", f.claims)
	f.live = append(f.live, sandboxd.SandboxSummary{ID: id})
	return sandboxd.ClaimResult{ID: id, Token: "tok-" + id, OwnerAddr: "10.99.0.5:7777", Deadline: f.deadline}, nil
}

func (f *fakeSandboxd) Release(_ context.Context, id, _ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.releaseErr != nil {
		return f.releaseErr
	}
	f.releases = append(f.releases, id)
	return nil
}

func (f *fakeSandboxd) Sandboxes(_ context.Context) ([]sandboxd.SandboxSummary, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.listErr != nil {
		return nil, f.listErr
	}
	return append([]sandboxd.SandboxSummary(nil), f.live...), nil
}

func (f *fakeSandboxd) releaseCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.releases)
}

func (f *fakeSandboxd) claimCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.claims
}

// ownerSandbox builds the unstructured owner CR the fake dynamic client serves.
func ownerSandbox(ns, name string, uid types.UID, deleting bool) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetAPIVersion("agents.x-k8s.io/v1beta1")
	u.SetKind("Sandbox")
	u.SetNamespace(ns)
	u.SetName(name)
	u.SetUID(uid)
	if deleting {
		now := metav1.Now()
		u.SetDeletionTimestamp(&now)
		u.SetFinalizers([]string{"keep"})
	}
	return u
}

// dynWith returns a fake dynamic client. The recorded dead-end applies here
// literally: seeding objects through the constructor naive-pluralizes the kind
// (Sandbox → "sandboxs"), parking them under a GVR nobody queries, so Get sees
// a structured NotFound and misreads the owner as deleted. Objects MUST be
// seeded with Tracker().Create under the explicit GVR.
func dynWith(t *testing.T, objs ...*unstructured.Unstructured) *dynamicfake.FakeDynamicClient {
	t.Helper()
	scheme := runtime.NewScheme()
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme,
		map[schema.GroupVersionResource]string{sandboxGVR: "SandboxList"})
	for _, o := range objs {
		if err := dyn.Tracker().Create(sandboxGVR, o, o.GetNamespace()); err != nil {
			t.Fatalf("seed %s/%s: %v", o.GetNamespace(), o.GetName(), err)
		}
	}
	return dyn
}

func sandboxPod(ns, name string, uid types.UID, ownerName string, ownerUID types.UID) *corev1.Pod {
	pod := &corev1.Pod{
		Namespace: ns, Name: name, UID: uid,
		Annotations: map[string]string{AnnTemplate: "base:24.04"},
		Spec:        corev1.PodSpec{Containers: []corev1.Container{{Name: "agent", Image: "img"}}},
	}
	if ownerName != "" {
		pod.OwnerReferences = []metav1.OwnerReference{{
			APIVersion: "agents.x-k8s.io/v1beta1", Kind: "Sandbox",
			Name: ownerName, UID: ownerUID, Controller: new(true),
		}}
	}
	return pod
}

func newTestProvider(t *testing.T, sd *fakeSandboxd, dyn *dynamicfake.FakeDynamicClient, statePath string) *Provider {
	t.Helper()
	cfg := Config{NodeName: "vk-test", Client: sd, Lister: sd, StatePath: statePath, Logger: logr.Discard()}
	if dyn != nil {
		cfg.Dynamic = dyn
	}
	p, err := New(t.Context(), cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return p
}
