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
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"

	"github.com/cocoonstack/sandbox-operator/pkg/scale/sandboxd"
)

// sandboxGVR is the owner CR resource this provider authorizes against.
var sandboxGVR = schema.GroupVersionResource{Group: "agents.x-k8s.io", Version: "v1beta1", Resource: "sandboxes"}

// TestDeleteWithoutAuthorityPreservesAndAdopts is the core contract: with the
// owner CR alive, pod deletion must NOT release the sandbox, and the same-key
// replacement pod adopts the preserved claim without a second sandboxd claim.
func TestDeleteWithoutAuthorityPreservesAndAdopts(t *testing.T) {
	ctx := context.Background()
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
	ctx := context.Background()
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
	ctx := context.Background()
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
	ctx := context.Background()
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
	ctx := context.Background()
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
	ctx := context.Background()
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
	ctx := context.Background()
	sd := &fakeSandboxd{}
	p := newTestProvider(t, sd, dynWith(t), "")

	pod := sandboxPod("ns1", "sb-pod", "uid-1", "", "")
	if err := p.CreatePod(ctx, pod); err != nil {
		t.Fatalf("CreatePod: %v", err)
	}
	// One extra live sandbox nobody claims → orphan candidate.
	sd.mu.Lock()
	sd.live = append(sd.live, ListedSandbox{ID: "sb_orphan"})
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

	// Failed list is NOT an empty list: cycle must be skipped.
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

// TestStateRoundTrip: claims persist across a provider restart so release
// credentials survive.
func TestStateRoundTrip(t *testing.T) {
	ctx := context.Background()
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
	ctx := context.Background()
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
	p, err := New(Config{StatePath: path, Logger: logr.Discard()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if got, ok := p.claimFor("ns/pod-1"); !ok || got.ID != "sb_1" || got.Token != "tok" {
		t.Fatalf("indented state from an older build did not load: %+v ok=%v", got, ok)
	}
}

func TestConcurrentSaveStateNeverLosesAClaim(t *testing.T) {
	path := t.TempDir() + "/claims.json"
	p, err := New(Config{StatePath: path, Logger: logr.Discard()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// Each writer adds a claim and persists. Whichever save lands last must
	// still describe every claim added before it, or a release credential is
	// gone and its microVM leaks until sandboxd's TTL reaps it.
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
	p, err := New(Config{Logger: logr.Discard()})
	if err != nil {
		t.Fatal(err)
	}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "p", UID: "u"}}
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
	p, err := New(Config{StatePath: path, Logger: logr.Discard()})
	if err != nil {
		t.Fatal(err)
	}
	p.pods["ns/p"] = &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "p", UID: "u"}}

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
	if _, err := New(Config{StatePath: path, Logger: logr.Discard()}); err != nil {
		t.Fatal(err)
	}

	// Without persistence the next restart backfills a NEW timestamp, so the
	// reported start time would jump on every restart.
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), "claimedAt") {
		t.Fatalf("backfill was not written back: %s", b)
	}

	first, err := New(Config{StatePath: path, Logger: logr.Discard()})
	if err != nil {
		t.Fatal(err)
	}
	second, err := New(Config{StatePath: path, Logger: logr.Discard()})
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
	p, err := New(Config{StatePath: dir + "/claims.json", Logger: logr.Discard()})
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
	// Not just on migration: any unwritable state path must stop the node before
	// it takes a Pod, because every claim it made would leak on restart.
	dir := t.TempDir()
	current := `{"claims":{"ns/p":{"id":"sb_1","token":"t","podUID":"u","claimedAt":"2026-01-01T00:00:00Z"}}}`
	if err := os.WriteFile(dir+"/claims.json", []byte(current), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })

	if _, err := New(Config{StatePath: dir + "/claims.json", Logger: logr.Discard()}); err == nil {
		t.Fatal("New accepted a state path it cannot write")
	}
}

func TestEveryClaimPathStampsClaimedAt(t *testing.T) {
	// runningStatus keeps a zero-value fallback; this pins the invariant that no
	// production path actually needs it, so the reported start time is always the
	// real claim time.
	sd := &fakeSandboxd{}
	p, err := New(Config{NodeName: "n", Client: sd, StatePath: t.TempDir() + "/c.json", Logger: logr.Discard()})
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

	// Adopt-in-place: drop the pod entry, keep the claim, then recreate.
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
	// Readable but not writable: the migration would otherwise stay in memory and
	// re-pick a new timestamp on every restart, silently.
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })

	if _, err := New(Config{StatePath: path, Logger: logr.Discard()}); err == nil {
		t.Fatal("New succeeded even though the claimedAt migration could not be written")
	}
}

func TestCreatePodReturnsTheSandboxWhenTheClaimCannotBePersisted(t *testing.T) {
	dir := t.TempDir()
	sd := &fakeSandboxd{}
	p, err := New(Config{NodeName: "n", Client: sd, StatePath: dir + "/claims.json", Logger: logr.Discard()})
	if err != nil {
		t.Fatal(err)
	}
	// Break persistence after startup, the way a full disk or a read-only
	// remount would.
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
	sd := &fakeSandboxd{releaseErr: errors.New("sandboxd unreachable")}
	p, err := New(Config{NodeName: "n", Client: sd, StatePath: dir + "/claims.json", Logger: logr.Discard()})
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
	// The credential is the only way to reach that sandbox; dropping it would
	// strand the microVM until sandboxd's TTL.
	if _, ok := p.claimFor("ns/p"); !ok {
		t.Error("the release credential was discarded even though the sandbox is still claimed")
	}
}

// fakeSandboxd implements SandboxdClient + Lister with call accounting.
type fakeSandboxd struct {
	mu         sync.Mutex
	claims     int
	releases   []string
	listErr    error
	live       []ListedSandbox
	claimErr   error
	releaseErr error
}

func (f *fakeSandboxd) Claim(_ context.Context, _ sandboxd.ClaimSpec) (sandboxd.ClaimResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.claimErr != nil {
		return sandboxd.ClaimResult{}, f.claimErr
	}
	f.claims++
	id := fmt.Sprintf("sb_%06d", f.claims)
	f.live = append(f.live, ListedSandbox{ID: id})
	return sandboxd.ClaimResult{ID: id, Token: "tok-" + id, OwnerAddr: "10.99.0.5:7777"}, nil
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

func (f *fakeSandboxd) ListSandboxes(_ context.Context) ([]ListedSandbox, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.listErr != nil {
		return nil, f.listErr
	}
	return append([]ListedSandbox(nil), f.live...), nil
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
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ns, Name: name, UID: uid,
			Annotations: map[string]string{AnnTemplate: "base:24.04"},
		},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "agent", Image: "img"}}},
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
	p, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return p
}
