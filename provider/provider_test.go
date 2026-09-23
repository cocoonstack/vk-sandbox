package provider

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-logr/logr"
	"github.com/go-logr/logr/funcr"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/cocoonstack/sandbox-operator/pkg/sandboxd"
)

var (
	sandboxGVR = schema.GroupVersionResource{Group: "agents.x-k8s.io", Version: "v1beta1", Resource: "sandboxes"}

	errTestReleaseFailed = errors.New("sandboxd unreachable")
	errTestWrongToken    = errors.New("token does not own the sandbox")
)

func TestClaimAddressesOmitsAddressless(t *testing.T) {
	p := &Provider{claims: map[string]Claim{
		"ns/a": {ID: "sb_a", Address: "10.0.0.5:7777"},
		"ns/b": {ID: "sb_b"},
	}}
	got := p.ClaimAddresses()
	if len(got) != 1 || got["sb_a"] != "10.0.0.5:7777" {
		t.Fatalf("ClaimAddresses() = %v, want only sb_a", got)
	}
}

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

	var notified *corev1.Pod
	p.NotifyPods(ctx, func(pod *corev1.Pod) { notified = pod })
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
	if notified == nil || notified.UID != "uid-2" || notified.Status.Phase != corev1.PodRunning || notified.Annotations[AnnClaimID] != c1.ID {
		t.Fatalf("the adopting Pod was not published Running on the adopted claim: %v", notified)
	}
}

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

func TestOnlyAnExpiredOwnerReadyReasonAuthorizesTheRelease(t *testing.T) {
	for _, tc := range []struct {
		status, reason string
		releases       int
	}{
		{"False", "SandboxExpired", 1},
		{"True", "DependenciesReady", 0},
		{"False", "DependenciesNotReady", 0},
		{"False", "SandboxSuspended", 0},
	} {
		t.Run(tc.reason, func(t *testing.T) {
			ctx := t.Context()
			sd := &fakeSandboxd{}
			owner := ownerSandbox("ns1", "sb-owner", "owner-uid", false)
			if err := unstructured.SetNestedSlice(owner.Object, []any{
				map[string]any{"type": "Suspended", "status": "False", "reason": "NotSuspended"},
				map[string]any{"type": "Ready", "status": tc.status, "reason": tc.reason},
			}, "status", "conditions"); err != nil {
				t.Fatalf("set conditions: %v", err)
			}
			p := newTestProvider(t, sd, dynWith(t, owner), "")

			pod := sandboxPod("ns1", "sb-pod", "uid-1", "sb-owner", "owner-uid")
			if err := p.CreatePod(ctx, pod); err != nil {
				t.Fatalf("CreatePod: %v", err)
			}
			if err := p.DeletePod(ctx, pod); err != nil {
				t.Fatalf("DeletePod: %v", err)
			}
			_, kept := p.claimFor("ns1/sb-pod")
			if got := sd.releaseCount(); got != tc.releases || kept != (tc.releases == 0) {
				t.Fatalf("owner Ready=%s/%s: releases=%d claim kept=%v, want %d releases", tc.status, tc.reason, got, kept, tc.releases)
			}
		})
	}
}

func TestDeleteReleasesWhenTheOwnerWasRecreated(t *testing.T) {
	ctx := t.Context()
	sd := &fakeSandboxd{}
	p := newTestProvider(t, sd, dynWith(t, ownerSandbox("ns1", "sb-owner", "owner-uid-2", false)), "")

	pod := sandboxPod("ns1", "sb-pod", "uid-1", "sb-owner", "owner-uid")
	if err := p.CreatePod(ctx, pod); err != nil {
		t.Fatalf("CreatePod: %v", err)
	}
	if err := p.DeletePod(ctx, pod); err != nil {
		t.Fatalf("DeletePod: %v", err)
	}
	if got := sd.releaseCount(); got != 1 {
		t.Fatalf("an owner under a new UID means the referenced generation is gone; releases=%d", got)
	}
}

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

func TestBarePodDeleteReleases(t *testing.T) {
	ctx := t.Context()
	sd := &fakeSandboxd{}
	p := newTestProvider(t, sd, dynWith(t), "")

	pod := sandboxPod("ns1", "bare", "uid-1", "", "")
	if err := p.CreatePod(ctx, pod); err != nil {
		t.Fatalf("CreatePod: %v", err)
	}
	sd.releaseErr = errTestReleaseFailed
	if err := p.DeletePod(ctx, pod); err == nil {
		t.Fatal("DeletePod reported success though the release failed")
	}
	if _, ok := p.heldClaimFor("ns1/bare"); !ok {
		t.Fatal("a failed release discarded the credential the delete retry needs")
	}
	sd.releaseErr = nil
	if err := p.DeletePod(ctx, pod); err != nil {
		t.Fatalf("DeletePod: %v", err)
	}
	if got := sd.releaseCount(); got != 1 {
		t.Fatalf("bare pod delete must release; releases=%d", got)
	}
}

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

func TestOrphanScanSkipsSandboxesQueuedForRelease(t *testing.T) {
	ctx := t.Context()
	sd := &fakeSandboxd{live: []sandboxd.SandboxSummary{{ID: "sb_leaving", ClaimRef: "ns1/gone"}}}
	p := newTestProvider(t, sd, dynWith(t), "")
	logLines := 0
	p.log = funcr.New(func(string, string) { logLines++ }, funcr.Options{})
	p.mu.Lock()
	p.releasing = append(p.releasing, Claim{ID: "sb_leaving", Token: "t"})
	p.mu.Unlock()

	orphans, stale, ok := p.OrphanScan(ctx)
	if !ok || len(orphans) != 0 || len(stale) != 0 || logLines != 0 {
		t.Fatalf("a sandbox queued for release was judged: orphans=%v stale=%v ok=%v logs=%d", orphans, stale, ok, logLines)
	}
}

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
	fi, err := os.Stat(statePath)
	if err != nil {
		t.Fatal(err)
	}
	if perm := fi.Mode().Perm(); perm != 0o600 {
		t.Fatalf("the claims table holds release tokens but is mode %v", perm)
	}

	p2 := newTestProvider(t, sd, dynWith(t), statePath)
	c2, ok := p2.claimFor("ns1/sb-pod")
	if !ok {
		t.Fatal("claim lost across restart")
	}
	if c2.ID != c1.ID || c2.Token != c1.Token {
		t.Fatalf("restart lost claim identity: got %+v want %+v", c2, c1)
	}
}

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

func TestInvalidClaimAnnotationsFailTheCreate(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*corev1.Pod)
	}{
		{"missing template", func(pod *corev1.Pod) { delete(pod.Annotations, AnnTemplate) }},
		{"non-integer ttl", func(pod *corev1.Pod) { pod.Annotations[AnnTTLSeconds] = "1h" }},
		{"negative ttl", func(pod *corev1.Pod) { pod.Annotations[AnnTTLSeconds] = "-5" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sd := &fakeSandboxd{}
			p := newTestProvider(t, sd, dynWith(t), "")
			pod := sandboxPod("ns1", "sb-pod", "uid-1", "", "")
			tc.mutate(pod)
			if err := p.CreatePod(t.Context(), pod); err == nil || sd.claimCount() != 0 {
				t.Fatalf("CreatePod = %v with %d claims, want a failed create and no claim", err, sd.claimCount())
			}
		})
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
	synctest.Test(t, func(t *testing.T) {
		p, err := New(t.Context(), Config{Logger: logr.Discard()})
		if err != nil {
			t.Fatal(err)
		}
		p.pods["ns/p"] = &corev1.Pod{Namespace: "ns", Name: "p", UID: "u"}
		p.claims["ns/p"] = Claim{ID: "sb_1", Token: "t", Address: "10.0.0.5:7777", ClaimedAt: metav1.Now()}

		first, _ := p.GetPodStatus(t.Context(), "ns", "p")
		time.Sleep(time.Second)
		second, _ := p.GetPodStatus(t.Context(), "ns", "p")
		if !first.StartTime.Equal(second.StartTime) {
			t.Fatalf("StartTime moves between reads: %v then %v", first.StartTime, second.StartTime)
		}
	})
}

func TestStartTimeIsStableForAClaimTableFromAnOlderBuild(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
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
		time.Sleep(time.Second)
		second, _ := p.GetPodStatus(t.Context(), "ns", "p")
		if !first.StartTime.Equal(second.StartTime) {
			t.Fatalf("StartTime still moves after loading a pre-ClaimedAt table: %v then %v", first.StartTime, second.StartTime)
		}
	})
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
	blockStateWrites(t, dir)

	if _, err := New(t.Context(), Config{StatePath: dir + "/claims.json", Logger: logr.Discard()}); err == nil {
		t.Fatal("New accepted a state path it cannot write")
	}
}

func TestNewRefusesAMalformedClaimsTable(t *testing.T) {
	path := t.TempDir() + "/claims.json"
	const torn = `{"claims":{"ns/p":{"id":"sb_1","tok`
	if err := os.WriteFile(path, []byte(torn), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := New(t.Context(), Config{StatePath: path, Logger: logr.Discard()}); err == nil {
		t.Fatal("New accepted a claims table it cannot decode")
	}
	if b, err := os.ReadFile(path); err != nil || string(b) != torn {
		t.Fatalf("a claims table New could not decode was overwritten: %q err=%v", b, err)
	}
}

func TestEveryClaimPathStampsClaimedAt(t *testing.T) {
	sd := &fakeSandboxd{}
	p, err := New(t.Context(), Config{Client: sd, StatePath: t.TempDir() + "/c.json", Logger: logr.Discard()})
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

	if err := p.CreatePod(t.Context(), sandboxPod("ns", "p", "u1", "", "")); err != nil {
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
	blockStateWrites(t, dir)

	if _, err := New(t.Context(), Config{StatePath: path, Logger: logr.Discard()}); err == nil {
		t.Fatal("New succeeded even though the claimedAt migration could not be written")
	}
}

func TestCreatePodReturnsTheSandboxWhenTheClaimCannotBePersisted(t *testing.T) {
	dir := t.TempDir()
	sd := &fakeSandboxd{}
	p, err := New(t.Context(), Config{Client: sd, StatePath: dir + "/claims.json", Logger: logr.Discard()})
	if err != nil {
		t.Fatal(err)
	}
	blockStateWrites(t, dir)

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
	if cached, _ := p.GetPod(t.Context(), "ns", "p"); cached != nil {
		t.Error("a Pod whose sandbox was returned stays cached, so virtual-kubelet calls UpdatePod instead of retrying CreatePod")
	}
}

func TestCreatePodKeepsTheCredentialWhenTheUndoReleaseAlsoFails(t *testing.T) {
	dir := t.TempDir()
	sd := &fakeSandboxd{releaseErr: errTestReleaseFailed}
	p, err := New(t.Context(), Config{Client: sd, StatePath: dir + "/claims.json", Logger: logr.Discard()})
	if err != nil {
		t.Fatal(err)
	}
	blockStateWrites(t, dir)

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
	p, err := New(t.Context(), Config{Client: sd, StatePath: dir + "/claims.json", Logger: logr.Discard()})
	if err != nil {
		t.Fatal(err)
	}
	blockStateWrites(t, dir)

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
	p, err := New(t.Context(), Config{Client: sd, StatePath: dir + "/claims.json", Logger: logr.Discard()})
	if err != nil {
		t.Fatal(err)
	}
	blockStateWrites(t, dir)

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
	_ = p.CreatePod(t.Context(), sandboxPod("ns", "p", "u3", "", ""))
	if len(sd.releases) == 0 || sd.releases[0] != stranded.ID {
		t.Errorf("the stranded sandbox was not the first one returned: releases=%v", sd.releases)
	}
}

func TestAFailedClaimLeavesThePodForTheCreateRetry(t *testing.T) {
	sd := &fakeSandboxd{claimErr: sandboxd.ErrNodeAtCapacity}
	p := newTestProvider(t, sd, dynWith(t), "")
	pod := sandboxPod("ns1", "sb-pod", "uid-1", "", "")

	if err := p.CreatePod(t.Context(), pod); !errors.Is(err, sandboxd.ErrNodeAtCapacity) {
		t.Fatalf("CreatePod = %v, want the capacity miss", err)
	}
	if cached, _ := p.GetPod(t.Context(), "ns1", "sb-pod"); cached != nil {
		t.Fatal("a Pod whose claim failed is cached, so virtual-kubelet calls UpdatePod instead of retrying CreatePod")
	}
	if _, ok := p.heldClaimFor("ns1/sb-pod"); ok {
		t.Fatal("a failed claim left a row")
	}

	sd.claimErr = nil
	if err := p.CreatePod(t.Context(), pod); err != nil {
		t.Fatalf("CreatePod retry: %v", err)
	}
	if _, ok := p.claimFor("ns1/sb-pod"); !ok || sd.claimCount() != 1 {
		t.Fatalf("the retry did not claim: ok=%v claims=%d", ok, sd.claimCount())
	}
}

func TestDeletingAPodThatNeverClaimedReleasesNothing(t *testing.T) {
	sd := &fakeSandboxd{claimErr: sandboxd.ErrNodeAtCapacity}
	p := newTestProvider(t, sd, dynWith(t), "")
	pod := sandboxPod("ns1", "sb-pod", "uid-1", "", "")
	if err := p.CreatePod(t.Context(), pod); err == nil {
		t.Fatal("setup: expected the claim to fail")
	}
	if err := p.DeletePod(t.Context(), pod); err != nil || sd.releaseCount() != 0 {
		t.Fatalf("DeletePod = %v with releases %v, want a clean delete that releases nothing", err, sd.releases)
	}
}

func TestTentativeClaimRetriesThroughUpdatePod(t *testing.T) {
	dir := t.TempDir()
	sd := &fakeSandboxd{releaseErr: errTestReleaseFailed}
	p, err := New(t.Context(), Config{Client: sd, StatePath: dir + "/claims.json", Logger: logr.Discard()})
	if err != nil {
		t.Fatal(err)
	}
	blockStateWrites(t, dir)
	var notified *corev1.Pod
	p.NotifyPods(t.Context(), func(pod *corev1.Pod) { notified = pod })
	pod := sandboxPod("ns", "p", "u1", "", "")
	if err := p.CreatePod(t.Context(), pod); err == nil {
		t.Fatal("CreatePod succeeded without persisting its claim")
	}
	cached, err := p.GetPod(t.Context(), pod.Namespace, pod.Name)
	if err != nil || cached == nil {
		t.Fatalf("GetPod = %v, err = %v", cached, err)
	}
	strandedID := cached.Annotations[AnnClaimID]
	if strandedID == "" || pod.Annotations[AnnClaimID] != "" {
		t.Fatal("the provider Pod must differ from the Kubernetes Pod so virtual-kubelet calls UpdatePod")
	}
	if err := p.UpdatePod(t.Context(), pod); err == nil {
		t.Fatal("UpdatePod succeeded while the stranded claim could not be released")
	}
	if sd.claimCount() != 1 || notified != nil {
		t.Fatal("failed compensation must neither claim again nor publish Running")
	}
	if err := os.Remove(dir + "/claims.json.tmp"); err != nil {
		t.Fatal(err)
	}
	sd.releaseErr = nil
	if err := p.UpdatePod(t.Context(), pod); err != nil {
		t.Fatalf("UpdatePod after recovery: %v", err)
	}
	c, ok := p.claimFor("ns/p")
	if !ok || c.ID == strandedID || sd.claimCount() != 2 || sd.releaseCount() != 1 || sd.releases[0] != strandedID {
		t.Fatalf("retry did not replace the stranded claim: claim=%+v releases=%v", c, sd.releases)
	}
	if notified == nil || notified.Status.Phase != corev1.PodRunning || notified.Annotations[AnnClaimID] != c.ID {
		t.Fatalf("retry did not publish the settled claim: %v", notified)
	}
	data, err := os.ReadFile(dir + "/claims.json")
	if err != nil {
		t.Fatal(err)
	}
	var persisted stateFile
	if err := json.Unmarshal(data, &persisted); err != nil {
		t.Fatal(err)
	}
	if persisted.Claims["ns/p"].ID != c.ID || persisted.Claims["ns/p"].Token != c.Token {
		t.Fatal("the retry did not persist the replacement release credential")
	}
}

func TestATentativeClaimCanStillBeReleased(t *testing.T) {
	dir := t.TempDir()
	sd := &fakeSandboxd{releaseErr: errTestReleaseFailed}
	p, err := New(t.Context(), Config{Client: sd, StatePath: dir + "/claims.json", Logger: logr.Discard()})
	if err != nil {
		t.Fatal(err)
	}
	blockStateWrites(t, dir)

	pod := sandboxPod("ns", "p", "u1", "", "")
	_ = p.CreatePod(t.Context(), pod)

	cached, err := p.GetPod(t.Context(), pod.Namespace, pod.Name)
	if err != nil || cached == nil {
		t.Fatalf("a deleted Kubernetes Pod must remain discoverable for release: %v", err)
	}
	sd.releaseErr = nil
	if err := p.DeletePod(t.Context(), cached); err != nil {
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
	p, err := New(t.Context(), Config{Client: sd, Lister: sd, StatePath: path, Logger: logr.Discard()})
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
	p, err := New(t.Context(), Config{Client: sd, Lister: sd, StatePath: path, Logger: logr.Discard()})
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
	synctest.Test(t, func(t *testing.T) {
		path := t.TempDir() + "/claims.json"
		live := `{"claims":{"ns/p":{"id":"sb_live","token":"t","podUID":"u","claimedAt":"2026-01-01T00:00:00Z"}}}`
		if err := os.WriteFile(path, []byte(live), 0o600); err != nil {
			t.Fatal(err)
		}
		sd := &fakeSandboxd{listErr: errTestReleaseFailed}
		p, err := New(t.Context(), Config{Client: sd, Lister: sd, StatePath: path, Logger: logr.Discard()})
		if err != nil {
			t.Fatal(err)
		}

		done := make(chan struct{})
		go func() {
			p.RunClaimVerification(t.Context(), time.Second)
			close(done)
		}()
		time.Sleep(time.Second)
		synctest.Wait()
		if _, ok := p.claimFor("ns/p"); ok {
			t.Fatal("the quarantine lifted while sandboxd could not be listed")
		}

		sd.mu.Lock()
		sd.listErr = nil
		sd.live = []sandboxd.SandboxSummary{{ID: "sb_live"}}
		sd.mu.Unlock()
		time.Sleep(time.Second)
		synctest.Wait()
		if _, ok := p.claimFor("ns/p"); !ok {
			t.Fatal("the quarantine never lifted")
		}
		select {
		case <-done:
		default:
			t.Error("verification kept running after the table was vouched for")
		}
	})
}

func TestARestartDoesNotClaimOverAnUnverifiedSandbox(t *testing.T) {
	path := t.TempDir() + "/claims.json"
	loaded := `{"claims":{"ns/p":{"id":"sb_prev","token":"t","podUID":"u","claimedAt":"2026-01-01T00:00:00Z"}}}`
	if err := os.WriteFile(path, []byte(loaded), 0o600); err != nil {
		t.Fatal(err)
	}
	sd := &fakeSandboxd{listErr: errTestReleaseFailed}
	p, err := New(t.Context(), Config{Client: sd, Lister: sd, StatePath: path, Logger: logr.Discard()})
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
	p, err := New(t.Context(), Config{Logger: logr.Discard()})
	if err != nil {
		t.Fatal(err)
	}
	hook := &listAfterHook{}
	p.lister = hook
	hook.onList = func() {
		p.mu.Lock()
		p.claims["ns/new"] = Claim{ID: "sb_new", Token: "t"}
		p.claims["ns/replaced"] = Claim{ID: "sb_fresh", Token: "t"}
		delete(p.quarantined, "ns/replaced")
		p.mu.Unlock()
	}
	p.mu.Lock()
	p.claims["ns/old"] = Claim{ID: "sb_old", Token: "t"}
	p.quarantined["ns/old"] = struct{}{}
	p.claims["ns/replaced"] = Claim{ID: "sb_gone", Token: "t"}
	p.quarantined["ns/replaced"] = struct{}{}
	p.mu.Unlock()

	if !p.VerifyClaimsAgainstNode(t.Context()) {
		t.Fatal("verification did not run")
	}
	if _, ok := p.heldClaimFor("ns/new"); !ok {
		t.Error("a row created during the listing was judged by it")
	}
	if c, ok := p.heldClaimFor("ns/replaced"); !ok || c.ID != "sb_fresh" {
		t.Errorf("a row replaced during the listing was judged by it: %+v ok=%v", c, ok)
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
	p, err := New(t.Context(), Config{Client: sd, Lister: sd, StatePath: path, Logger: logr.Discard()})
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

func TestTheClaimCarriesThePodAnnotationsAndKey(t *testing.T) {
	sd := &fakeSandboxd{}
	p, err := New(t.Context(), Config{Client: sd, Logger: logr.Discard()})
	if err != nil {
		t.Fatal(err)
	}
	pod := sandboxPod("ns", "p", "u", "", "")
	pod.Annotations[AnnNet] = "egress"
	pod.Annotations[AnnSize] = "large"
	pod.Annotations[AnnTTLSeconds] = "600"
	if err := p.CreatePod(t.Context(), pod); err != nil {
		t.Fatal(err)
	}
	want := sandboxd.ClaimSpec{Template: "base:24.04", Net: "egress", Size: "large", TTLSeconds: 600, ClaimRef: "ns/p"}
	if sd.lastSpec != want {
		t.Errorf("ClaimSpec = %+v, want %+v", sd.lastSpec, want)
	}
}

func TestAClaimIsPublishedReadyAtTheHostOfItsOwnerAddress(t *testing.T) {
	sd := &fakeSandboxd{}
	p := newTestProvider(t, sd, dynWith(t), "")
	var notified *corev1.Pod
	p.NotifyPods(t.Context(), func(pod *corev1.Pod) { notified = pod })

	if err := p.CreatePod(t.Context(), sandboxPod("ns1", "sb-pod", "uid-1", "", "")); err != nil {
		t.Fatalf("CreatePod: %v", err)
	}
	c, ok := p.claimFor("ns1/sb-pod")
	if !ok || notified == nil {
		t.Fatalf("the claim was not published: claim=%+v ok=%v pod=%v", c, ok, notified)
	}
	st := notified.Status
	conditions := []corev1.PodCondition{
		{Type: corev1.PodInitialized, Status: corev1.ConditionTrue},
		{Type: corev1.PodReady, Status: corev1.ConditionTrue},
		{Type: corev1.PodScheduled, Status: corev1.ConditionTrue},
	}
	if st.PodIP != "10.99.0.5" || !slices.Equal(st.PodIPs, []corev1.PodIP{{IP: "10.99.0.5"}}) || st.HostIP != "10.99.0.5" || !slices.Equal(st.Conditions, conditions) {
		t.Fatalf("the Running status must carry the Initialized, Ready and PodScheduled conditions and the owner_addr host as Pod IP: podIP=%q podIPs=%v hostIP=%q conditions=%v",
			st.PodIP, st.PodIPs, st.HostIP, st.Conditions)
	}
	if len(st.ContainerStatuses) != 1 || st.ContainerStatuses[0].Name != "agent" || !st.ContainerStatuses[0].Ready || st.ContainerStatuses[0].State.Running == nil ||
		st.ContainerStatuses[0].ImageID != "sandboxd://"+c.ID {
		t.Fatalf("want one ready, running container backed by %s: %+v", c.ID, st.ContainerStatuses)
	}
}

func TestAdoptionDoesNotResurrectARowVerificationRemoved(t *testing.T) {
	sd := &fakeSandboxd{}
	p, err := New(t.Context(), Config{Client: sd, Logger: logr.Discard()})
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

func TestTheTTLAnnotationSelectsTheLease(t *testing.T) {
	for _, tc := range []struct {
		ttl  string
		want int
	}{
		{"", defaultClaimTTLSeconds},
		{"0", 0},
	} {
		sd := &fakeSandboxd{}
		p, err := New(t.Context(), Config{Client: sd, Logger: logr.Discard()})
		if err != nil {
			t.Fatal(err)
		}
		pod := sandboxPod("ns", "p", "u", "", "")
		if tc.ttl != "" {
			pod.Annotations[AnnTTLSeconds] = tc.ttl
		}
		if err := p.CreatePod(t.Context(), pod); err != nil {
			t.Fatalf("ttl %q: %v", tc.ttl, err)
		}
		if sd.lastSpec.TTLSeconds != tc.want {
			t.Errorf("ttl %q: TTLSeconds = %d, want %d", tc.ttl, sd.lastSpec.TTLSeconds, tc.want)
		}
	}
}

func TestClaimRecordsTheLeaseDeadline(t *testing.T) {
	want := time.Now().Add(24 * time.Hour).UTC().Truncate(time.Second)
	sd := &fakeSandboxd{deadline: want}
	p, err := New(t.Context(), Config{Client: sd, Logger: logr.Discard()})
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
	p, err := New(t.Context(), Config{Logger: logr.Discard()})
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
	p, err := New(t.Context(), Config{Logger: logr.Discard()})
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
	if st.Phase != corev1.PodRunning || st.PodIP != "" || st.PodIPs != nil {
		t.Errorf("phase = %v podIP = %q podIPs = %v, want Running and no Pod IP for a claim without owner_addr", st.Phase, st.PodIP, st.PodIPs)
	}
}

func TestLeaseWatchPublishesFailedForAReapedSandbox(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p, err := New(t.Context(), Config{Logger: logr.Discard()})
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
			Deadline:  metav1.NewTime(time.Now().Add(1500 * time.Millisecond)),
		}
		p.mu.Unlock()

		go p.RunLeaseWatch(t.Context(), time.Second)
		time.Sleep(2 * time.Second)
		synctest.Wait()
		select {
		case pod := <-got:
			if pod.Status.Phase != corev1.PodFailed || pod.Status.Reason != ReasonLeaseExpired {
				t.Fatalf("published %v/%v, want Failed/%s", pod.Status.Phase, pod.Status.Reason, ReasonLeaseExpired)
			}
		default:
			t.Fatal("lease expiry was never published")
		}
	})
}

func TestAnExpiredClaimIsReplacedNotAdopted(t *testing.T) {
	for _, listed := range []bool{false, true} {
		sd := &fakeSandboxd{}
		cfg := Config{Client: sd, Logger: logr.Discard()}
		if listed {
			cfg.Lister = sd
		}
		p, err := New(t.Context(), cfg)
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
			t.Fatalf("listed=%v: CreatePod: %v", listed, err)
		}
		if sd.claims != 1 {
			t.Fatalf("listed=%v: claims = %d, want a fresh claim", listed, sd.claims)
		}
		c, ok := p.claimFor("ns/p")
		if !ok || c.ID == "sb_reaped" {
			t.Fatalf("listed=%v: claim = %+v ok=%v, want the reaped row replaced", listed, c, ok)
		}
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
	p, err := New(t.Context(), Config{Client: sd, Lister: sd, StatePath: path, Logger: logr.Discard()})
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
	p, err := New(t.Context(), Config{Client: sd, Lister: sd, Logger: logr.Discard()})
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
	p, err := New(t.Context(), Config{Client: sd, Lister: sd, Logger: logr.Discard()})
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
	p, err := New(t.Context(), Config{Client: sd, Lister: sd, Logger: logr.Discard()})
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

func TestAnExpiredClaimIsNeitherAdoptedNorReplacedWhileTheNodeCannotBeListed(t *testing.T) {
	sd := &fakeSandboxd{listErr: errTestReleaseFailed}
	p, err := New(t.Context(), Config{Client: sd, Lister: sd, Logger: logr.Discard()})
	if err != nil {
		t.Fatal(err)
	}
	var pushed bool
	p.NotifyPods(t.Context(), func(*corev1.Pod) { pushed = true })
	p.mu.Lock()
	p.claims["ns/p"] = Claim{ID: "sb_expired", Token: "t", ClaimedAt: metav1.Now(), Deadline: metav1.NewTime(time.Now().Add(-time.Hour))}
	p.mu.Unlock()

	if err := p.CreatePod(t.Context(), sandboxPod("ns", "p", "u", "", "")); err == nil {
		t.Fatal("a past-deadline claim was settled without the node's listing")
	}
	c, ok := p.heldClaimFor("ns/p")
	if pushed || sd.claimCount() != 0 || !ok || c.ID != "sb_expired" {
		t.Fatalf("an unlistable node must defer: pushed=%v claims=%d row=%+v ok=%v", pushed, sd.claimCount(), c, ok)
	}
}

func TestTheWatchPublishesNothingForAPodThatChangedDuringTheListing(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*Provider)
	}{
		{"replaced", func(p *Provider) {
			p.pods["ns/p"] = sandboxPod("ns", "p", "u-new", "", "")
			p.claims["ns/p"] = Claim{ID: "sb_new", Token: "t2", Deadline: metav1.NewTime(time.Now().Add(time.Hour))}
		}},
		{"deleted", func(p *Provider) { delete(p.pods, "ns/p") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, err := New(t.Context(), Config{Logger: logr.Discard()})
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
				tc.change(p)
				p.mu.Unlock()
			}

			p.publishExpiredLeases(t.Context())
			select {
			case pod := <-notified:
				t.Fatalf("a Pod that changed behind the listing was published %v with the predecessor's expiry", pod.Status.Phase)
			default:
			}
		})
	}
}

func TestAPreservedClaimIsReleasedOnceItsOwnerIsGone(t *testing.T) {
	ctx := t.Context()
	sd := &fakeSandboxd{}
	dyn := dynWith(t, ownerSandbox("ns1", "sb-owner", "owner-uid", false))
	p := newTestProvider(t, sd, dyn, "")
	pod := sandboxPod("ns1", "sb-pod", "uid-1", "sb-owner", "owner-uid")
	if err := p.CreatePod(ctx, pod); err != nil {
		t.Fatalf("CreatePod: %v", err)
	}
	if err := p.DeletePod(ctx, pod); err != nil {
		t.Fatalf("DeletePod: %v", err)
	}
	c, ok := p.claimFor("ns1/sb-pod")
	if !ok || c.Owner == nil || c.Owner.Name != "sb-owner" || c.Owner.UID != "owner-uid" {
		t.Fatalf("a preserved claim must record its owner: %+v ok=%v", c, ok)
	}

	now := time.Now()
	p.recheckOwners(ctx, now, time.Second)
	if got := sd.releaseCount(); got != 0 {
		t.Fatalf("the re-check released while the owner was alive; releases=%d", got)
	}
	if err := dyn.Tracker().Delete(sandboxGVR, "ns1", "sb-owner"); err != nil {
		t.Fatalf("delete owner: %v", err)
	}
	p.recheckOwners(ctx, now, time.Second)
	if got := sd.releaseCount(); got != 0 {
		t.Fatalf("the re-check ran again inside its backoff; releases=%d", got)
	}
	p.recheckOwners(ctx, now.Add(time.Second), time.Second)
	if got := sd.releaseCount(); got != 1 {
		t.Fatalf("an owner confirmed gone must release; releases=%d", got)
	}
	if _, ok := p.heldClaimFor("ns1/sb-pod"); ok {
		t.Fatal("the released claim must be dropped")
	}
	if len(p.ownerRechecks) != 0 || len(p.releasing) != 0 {
		t.Fatalf("a released claim lingers: backoff=%v queue=%v", p.ownerRechecks, p.releasing)
	}
}

func TestTheOwnerRecheckBacksOffWhileTheOwnerLives(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := t.Context()
		sd := &fakeSandboxd{}
		dyn := dynWith(t, ownerSandbox("ns1", "sb-owner", "owner-uid", false))
		p := newTestProvider(t, sd, dyn, "")
		pod := sandboxPod("ns1", "sb-pod", "uid-1", "sb-owner", "owner-uid")
		if err := p.CreatePod(ctx, pod); err != nil {
			t.Fatalf("CreatePod: %v", err)
		}
		if err := p.DeletePod(ctx, pod); err != nil {
			t.Fatalf("DeletePod: %v", err)
		}
		dyn.ClearActions()

		go p.RunOwnerRecheck(ctx, time.Second)
		time.Sleep(8 * time.Second)
		synctest.Wait()
		if got := len(dyn.Actions()); got != 4 {
			t.Fatalf("owner reads over eight 1s ticks = %d, want 4 (1s, 2s, 4s, 8s)", got)
		}
		if got := sd.releaseCount(); got != 0 {
			t.Fatalf("a living owner must keep the claim; releases=%d", got)
		}
		if c, ok := p.claimFor("ns1/sb-pod"); !ok || c.Owner == nil {
			t.Fatalf("the preserved claim lost its owner: %+v ok=%v", c, ok)
		}
	})
}

func TestTheOwnerRecheckKeepsPreservingWhileTheOwnerIsUnverifiable(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"404 without Details.Name", k8serrors.NewNotFound(sandboxGVR.GroupResource(), "")},
		{"forbidden read of the owner", k8serrors.NewForbidden(sandboxGVR.GroupResource(), "sb-owner", errors.New("rbac"))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			sd := &fakeSandboxd{}
			dyn := dynWith(t, ownerSandbox("ns1", "sb-owner", "owner-uid", false))
			p := newTestProvider(t, sd, dyn, "")
			pod := sandboxPod("ns1", "sb-pod", "uid-1", "sb-owner", "owner-uid")
			if err := p.CreatePod(ctx, pod); err != nil {
				t.Fatalf("CreatePod: %v", err)
			}
			if err := p.DeletePod(ctx, pod); err != nil {
				t.Fatalf("DeletePod: %v", err)
			}
			dyn.PrependReactor("get", "sandboxes", func(k8stesting.Action) (bool, runtime.Object, error) {
				return true, nil, tc.err
			})

			p.recheckOwners(ctx, time.Now(), time.Second)
			if got := sd.releaseCount(); got != 0 {
				t.Fatalf("an owner read that proves nothing must not release; releases=%d", got)
			}
			if c, ok := p.heldClaimFor("ns1/sb-pod"); !ok || c.Owner == nil {
				t.Fatalf("the claim must stay preserved with its owner: %+v ok=%v", c, ok)
			}
		})
	}
}

func TestAdoptionCancelsTheOwnerRecheck(t *testing.T) {
	ctx := t.Context()
	sd := &fakeSandboxd{}
	dyn := dynWith(t, ownerSandbox("ns1", "sb-owner", "owner-uid", false))
	p := newTestProvider(t, sd, dyn, "")
	if err := p.CreatePod(ctx, sandboxPod("ns1", "sb-pod", "uid-1", "sb-owner", "owner-uid")); err != nil {
		t.Fatalf("CreatePod: %v", err)
	}
	if err := p.DeletePod(ctx, sandboxPod("ns1", "sb-pod", "uid-1", "sb-owner", "owner-uid")); err != nil {
		t.Fatalf("DeletePod: %v", err)
	}
	if err := p.CreatePod(ctx, sandboxPod("ns1", "sb-pod", "uid-2", "sb-owner", "owner-uid")); err != nil {
		t.Fatalf("CreatePod replacement: %v", err)
	}
	if c, ok := p.claimFor("ns1/sb-pod"); !ok || c.Owner != nil {
		t.Fatalf("adoption must clear the recorded owner: %+v ok=%v", c, ok)
	}

	if err := dyn.Tracker().Delete(sandboxGVR, "ns1", "sb-owner"); err != nil {
		t.Fatalf("delete owner: %v", err)
	}
	p.recheckOwners(ctx, time.Now(), time.Second)
	if got := sd.releaseCount(); got != 0 {
		t.Fatalf("a claim held by a Pod must not be re-checked; releases=%d", got)
	}
}

func TestAPodOfANewOwnerGenerationClaimsFresh(t *testing.T) {
	ctx := t.Context()
	sd := &fakeSandboxd{}
	dyn := dynWith(t, ownerSandbox("ns1", "sb-owner", "owner-uid", false))
	p := newTestProvider(t, sd, dyn, "")
	pod := sandboxPod("ns1", "sb-pod", "uid-1", "sb-owner", "owner-uid")
	if err := p.CreatePod(ctx, pod); err != nil {
		t.Fatalf("CreatePod: %v", err)
	}
	if err := p.DeletePod(ctx, pod); err != nil {
		t.Fatalf("DeletePod: %v", err)
	}
	old, _ := p.claimFor("ns1/sb-pod")
	p.recheckOwners(ctx, time.Now(), time.Minute)

	if err := dyn.Tracker().Delete(sandboxGVR, "ns1", "sb-owner"); err != nil {
		t.Fatalf("delete owner: %v", err)
	}
	if err := dyn.Tracker().Create(sandboxGVR, ownerSandbox("ns1", "sb-owner", "owner-uid-2", false), "ns1"); err != nil {
		t.Fatalf("recreate owner: %v", err)
	}
	if err := p.CreatePod(ctx, sandboxPod("ns1", "sb-pod", "uid-2", "sb-owner", "owner-uid-2")); err != nil {
		t.Fatalf("CreatePod for the recreated owner: %v", err)
	}
	c, ok := p.claimFor("ns1/sb-pod")
	if !ok || c.ID == old.ID || sd.claimCount() != 2 {
		t.Fatalf("the recreated owner's Pod adopted its predecessor's sandbox: %+v ok=%v claims=%d", c, ok, sd.claimCount())
	}
	p.recheckOwners(ctx, time.Now(), time.Minute)
	if sd.releaseCount() != 1 || sd.releases[0] != old.ID {
		t.Fatalf("the predecessor's sandbox was not released: %v", sd.releases)
	}
}

func TestABarePodDoesNotAdoptAPreservedClaim(t *testing.T) {
	ctx := t.Context()
	sd := &fakeSandboxd{}
	p := newTestProvider(t, sd, dynWith(t, ownerSandbox("ns1", "sb-owner", "owner-uid", false)), "")
	pod := sandboxPod("ns1", "sb-pod", "uid-1", "sb-owner", "owner-uid")
	if err := p.CreatePod(ctx, pod); err != nil {
		t.Fatalf("CreatePod: %v", err)
	}
	if err := p.DeletePod(ctx, pod); err != nil {
		t.Fatalf("DeletePod: %v", err)
	}
	old, _ := p.claimFor("ns1/sb-pod")

	if err := p.CreatePod(ctx, sandboxPod("ns1", "sb-pod", "uid-2", "", "")); err != nil {
		t.Fatalf("CreatePod for a bare Pod: %v", err)
	}
	c, ok := p.claimFor("ns1/sb-pod")
	if !ok || c.ID == old.ID || sd.claimCount() != 2 {
		t.Fatalf("a bare Pod adopted a sandbox preserved for a Sandbox owner: %+v ok=%v claims=%d", c, ok, sd.claimCount())
	}
	p.recheckOwners(ctx, time.Now(), time.Minute)
	if sd.releaseCount() != 1 || sd.releases[0] != old.ID {
		t.Fatalf("the preserved sandbox was not released: %v", sd.releases)
	}
}

func TestARestartDoesNotHandAPodOfAnotherOwnerItsPredecessorsSandbox(t *testing.T) {
	for _, tc := range []struct {
		name       string
		prev, next *corev1.Pod
	}{
		{"new owner generation", sandboxPod("ns1", "sb-pod", "uid-1", "sb-owner", "owner-uid"), sandboxPod("ns1", "sb-pod", "uid-2", "sb-owner", "owner-uid-2")},
		{"new bare pod", sandboxPod("ns1", "sb-pod", "uid-1", "", ""), sandboxPod("ns1", "sb-pod", "uid-2", "", "")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			path := t.TempDir() + "/claims.json"
			sd := &fakeSandboxd{}
			dyn := dynWith(t)
			p1 := newTestProvider(t, sd, dyn, path)
			if err := p1.CreatePod(ctx, tc.prev); err != nil {
				t.Fatalf("CreatePod: %v", err)
			}
			old, _ := p1.claimFor("ns1/sb-pod")

			p2 := newTestProvider(t, sd, dyn, path)
			if err := p2.CreatePod(ctx, tc.next); err != nil {
				t.Fatalf("CreatePod after the restart: %v", err)
			}
			c, ok := p2.claimFor("ns1/sb-pod")
			if !ok || c.ID == old.ID || sd.claimCount() != 2 {
				t.Fatalf("the successor adopted its predecessor's sandbox across a restart: %+v ok=%v claims=%d", c, ok, sd.claimCount())
			}
			p2.recheckOwners(ctx, time.Now(), time.Minute)
			if sd.releaseCount() != 1 || sd.releases[0] != old.ID {
				t.Fatalf("the predecessor's sandbox was not released: %v", sd.releases)
			}
		})
	}
}

func TestARestartStillHandsASameOwnerReplacementItsSandbox(t *testing.T) {
	ctx := t.Context()
	path := t.TempDir() + "/claims.json"
	sd := &fakeSandboxd{}
	dyn := dynWith(t)
	p1 := newTestProvider(t, sd, dyn, path)
	if err := p1.CreatePod(ctx, sandboxPod("ns1", "sb-pod", "uid-1", "sb-owner", "owner-uid")); err != nil {
		t.Fatalf("CreatePod: %v", err)
	}
	old, _ := p1.claimFor("ns1/sb-pod")

	p2 := newTestProvider(t, sd, dyn, path)
	if err := p2.CreatePod(ctx, sandboxPod("ns1", "sb-pod", "uid-2", "sb-owner", "owner-uid")); err != nil {
		t.Fatalf("CreatePod after the restart: %v", err)
	}
	c, ok := p2.claimFor("ns1/sb-pod")
	if !ok || c.ID != old.ID || sd.claimCount() != 1 || sd.releaseCount() != 0 {
		t.Fatalf("a same-owner replacement lost its sandbox across a restart: %+v ok=%v claims=%d releases=%v", c, ok, sd.claimCount(), sd.releases)
	}
}

func TestARestartKeepsTheSandboxOfAPodWhoseOwnerChanged(t *testing.T) {
	ctx := t.Context()
	path := t.TempDir() + "/claims.json"
	sd := &fakeSandboxd{}
	dyn := dynWith(t)
	p1 := newTestProvider(t, sd, dyn, path)
	if err := p1.CreatePod(ctx, sandboxPod("ns1", "sb-pod", "uid-1", "", "")); err != nil {
		t.Fatalf("CreatePod: %v", err)
	}
	old, _ := p1.claimFor("ns1/sb-pod")

	p2 := newTestProvider(t, sd, dyn, path)
	if err := p2.CreatePod(ctx, sandboxPod("ns1", "sb-pod", "uid-1", "sb-pod", "owner-uid")); err != nil {
		t.Fatalf("CreatePod after the restart: %v", err)
	}
	c, ok := p2.claimFor("ns1/sb-pod")
	if !ok || c.ID != old.ID || c.Authority == nil || *c.Authority != "owner-uid" || sd.claimCount() != 1 || sd.releaseCount() != 0 {
		t.Fatalf("the same Pod lost its sandbox after a Sandbox adopted it: %+v ok=%v claims=%d releases=%v", c, ok, sd.claimCount(), sd.releases)
	}
}

func TestARestartReleasesAnUnadoptedClaimOnlyWhenItsPodIsDeleted(t *testing.T) {
	ctx := t.Context()
	path := t.TempDir() + "/claims.json"
	sd := &fakeSandboxd{}
	dyn := dynWith(t)
	pod := sandboxPod("ns1", "sb-pod", "uid-1", "", "")
	if err := newTestProvider(t, sd, dyn, path).CreatePod(ctx, pod); err != nil {
		t.Fatalf("CreatePod: %v", err)
	}

	p := newTestProvider(t, sd, dyn, path)
	p.recheckOwners(ctx, time.Now(), time.Second)
	if got := sd.releaseCount(); got != 0 {
		t.Fatalf("the owner re-check released a claim whose Pod had not come back after the restart; releases=%d", got)
	}
	if err := p.DeletePod(ctx, pod); err != nil {
		t.Fatalf("DeletePod: %v", err)
	}
	if got := sd.releaseCount(); got != 1 {
		t.Fatalf("deleting a Pod the restarted provider had not seen again must release its sandbox; releases=%d", got)
	}
}

func TestAPreservedClaimAnswersToTheOwnerThatPreservedIt(t *testing.T) {
	ctx := t.Context()
	sd := &fakeSandboxd{}
	p := newTestProvider(t, sd, dynWith(t, ownerSandbox("ns1", "sb-pod", "owner-uid", false)), "")
	if err := p.CreatePod(ctx, sandboxPod("ns1", "sb-pod", "uid-1", "", "")); err != nil {
		t.Fatalf("CreatePod: %v", err)
	}
	old, _ := p.claimFor("ns1/sb-pod")
	adopted := sandboxPod("ns1", "sb-pod", "uid-1", "sb-pod", "owner-uid")
	if err := p.UpdatePod(ctx, adopted); err != nil {
		t.Fatalf("UpdatePod: %v", err)
	}
	if err := p.DeletePod(ctx, adopted); err != nil {
		t.Fatalf("DeletePod: %v", err)
	}

	if err := p.CreatePod(ctx, sandboxPod("ns1", "sb-pod", "uid-2", "sb-pod", "owner-uid")); err != nil {
		t.Fatalf("CreatePod replacement: %v", err)
	}
	c, ok := p.claimFor("ns1/sb-pod")
	if !ok || c.ID != old.ID || sd.claimCount() != 1 || sd.releaseCount() != 0 {
		t.Fatalf("the Sandbox that adopted a bare Pod lost its sandbox when that Pod was replaced: %+v ok=%v claims=%d releases=%v", c, ok, sd.claimCount(), sd.releases)
	}
}

func TestALegacyClaimWithoutAnAuthorityIsStillAdopted(t *testing.T) {
	path := t.TempDir() + "/claims.json"
	legacy := `{"claims":{"ns/p":{"id":"sb_prev","token":"t","podUID":"uid-1","claimedAt":"2026-01-01T00:00:00Z"}}}`
	if err := os.WriteFile(path, []byte(legacy), 0o600); err != nil {
		t.Fatal(err)
	}
	sd := &fakeSandboxd{live: []sandboxd.SandboxSummary{{ID: "sb_prev"}}}
	p, err := New(t.Context(), Config{Client: sd, Lister: sd, StatePath: path, Logger: logr.Discard()})
	if err != nil {
		t.Fatal(err)
	}

	if err := p.CreatePod(t.Context(), sandboxPod("ns", "p", "uid-2", "sb-owner", "owner-uid")); err != nil {
		t.Fatalf("CreatePod: %v", err)
	}
	c, ok := p.claimFor("ns/p")
	if !ok || c.ID != "sb_prev" || c.Authority == nil || *c.Authority != "owner-uid" || sd.claimCount() != 0 || sd.releaseCount() != 0 {
		t.Fatalf("a claim from an older table was not adopted as before: %+v ok=%v claims=%d releases=%v", c, ok, sd.claimCount(), sd.releases)
	}
}

func TestALegacyPreservedClaimIsNotAdoptedByAnotherOwner(t *testing.T) {
	path := t.TempDir() + "/claims.json"
	legacy := `{"claims":{"ns/p":{"id":"sb_prev","token":"t","podUID":"uid-1","claimedAt":"2026-01-01T00:00:00Z",` +
		`"owner":{"apiVersion":"agents.x-k8s.io/v1beta1","kind":"Sandbox","name":"sb-owner","uid":"owner-uid","controller":true}}}}`
	if err := os.WriteFile(path, []byte(legacy), 0o600); err != nil {
		t.Fatal(err)
	}
	sd := &fakeSandboxd{live: []sandboxd.SandboxSummary{{ID: "sb_prev"}}}
	p, err := New(t.Context(), Config{Client: sd, Lister: sd, StatePath: path, Logger: logr.Discard()})
	if err != nil {
		t.Fatal(err)
	}

	if err := p.CreatePod(t.Context(), sandboxPod("ns", "p", "uid-2", "sb-owner", "owner-uid-2")); err != nil {
		t.Fatalf("CreatePod: %v", err)
	}
	c, ok := p.claimFor("ns/p")
	if !ok || c.ID == "sb_prev" || sd.claimCount() != 1 {
		t.Fatalf("a recreated owner adopted a claim preserved by an older build: %+v ok=%v claims=%d", c, ok, sd.claimCount())
	}
	p.recheckOwners(t.Context(), time.Now(), time.Minute)
	if sd.releaseCount() != 1 || sd.releases[0] != "sb_prev" {
		t.Fatalf("the preserved sandbox was not released: %v", sd.releases)
	}
}

func TestARestartKeepsThePendingOwnerRecheck(t *testing.T) {
	ctx := t.Context()
	path := t.TempDir() + "/claims.json"
	sd := &fakeSandboxd{}
	dyn := dynWith(t, ownerSandbox("ns1", "sb-owner", "owner-uid", false))
	p1 := newTestProvider(t, sd, dyn, path)
	pod := sandboxPod("ns1", "sb-pod", "uid-1", "sb-owner", "owner-uid")
	if err := p1.CreatePod(ctx, pod); err != nil {
		t.Fatalf("CreatePod: %v", err)
	}
	if err := p1.DeletePod(ctx, pod); err != nil {
		t.Fatalf("DeletePod: %v", err)
	}

	p2 := newTestProvider(t, sd, dyn, path)
	c, ok := p2.claimFor("ns1/sb-pod")
	if !ok || c.Owner == nil || c.Owner.Name != "sb-owner" {
		t.Fatalf("the recorded owner did not survive the restart: %+v ok=%v", c, ok)
	}
	if err := dyn.Tracker().Delete(sandboxGVR, "ns1", "sb-owner"); err != nil {
		t.Fatalf("delete owner: %v", err)
	}
	p2.recheckOwners(ctx, time.Now(), time.Second)
	if got := sd.releaseCount(); got != 1 {
		t.Fatalf("the restarted provider must finish the re-check; releases=%d", got)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), "sb-pod") {
		t.Fatalf("the released claim is still on disk: %s", b)
	}
}

func TestAPodArrivingDuringTheReleaseClaimsFresh(t *testing.T) {
	ctx := t.Context()
	sd := &fakeSandboxd{}
	dyn := dynWith(t, ownerSandbox("ns1", "sb-owner", "owner-uid", false))
	p := newTestProvider(t, sd, dyn, "")
	pod := sandboxPod("ns1", "sb-pod", "uid-1", "sb-owner", "owner-uid")
	if err := p.CreatePod(ctx, pod); err != nil {
		t.Fatalf("CreatePod: %v", err)
	}
	if err := p.DeletePod(ctx, pod); err != nil {
		t.Fatalf("DeletePod: %v", err)
	}
	old, _ := p.claimFor("ns1/sb-pod")
	if err := dyn.Tracker().Delete(sandboxGVR, "ns1", "sb-owner"); err != nil {
		t.Fatalf("delete owner: %v", err)
	}
	sd.onRelease = func() {
		sd.onRelease = nil
		if err := p.CreatePod(ctx, sandboxPod("ns1", "sb-pod", "uid-2", "sb-owner", "owner-uid-2")); err != nil {
			t.Errorf("CreatePod during the release: %v", err)
		}
	}

	p.recheckOwners(ctx, time.Now(), time.Second)
	c, ok := p.claimFor("ns1/sb-pod")
	if !ok || c.ID == old.ID {
		t.Fatalf("the replacement Pod adopted a sandbox that was being released: %+v ok=%v", c, ok)
	}
	if sd.claimCount() != 2 || sd.releaseCount() != 1 || sd.releases[0] != old.ID {
		t.Fatalf("want a fresh claim and exactly the old release: claims=%d releases=%v", sd.claimCount(), sd.releases)
	}
	if cached, err := p.GetPod(ctx, "ns1", "sb-pod"); err != nil || cached == nil || cached.UID != "uid-2" {
		t.Fatalf("the replacement Pod must survive the release: %v err=%v", cached, err)
	}
}

func TestAFailedReleaseKeepsTheCredentialQueued(t *testing.T) {
	ctx := t.Context()
	path := t.TempDir() + "/claims.json"
	sd := &fakeSandboxd{}
	dyn := dynWith(t, ownerSandbox("ns1", "sb-owner", "owner-uid", false))
	p := newTestProvider(t, sd, dyn, path)
	pod := sandboxPod("ns1", "sb-pod", "uid-1", "sb-owner", "owner-uid")
	if err := p.CreatePod(ctx, pod); err != nil {
		t.Fatalf("CreatePod: %v", err)
	}
	if err := p.DeletePod(ctx, pod); err != nil {
		t.Fatalf("DeletePod: %v", err)
	}
	old, _ := p.claimFor("ns1/sb-pod")
	if err := dyn.Tracker().Delete(sandboxGVR, "ns1", "sb-owner"); err != nil {
		t.Fatalf("delete owner: %v", err)
	}
	sd.releaseErr = errTestReleaseFailed

	p.recheckOwners(ctx, time.Now(), time.Second)
	if sd.releaseCount() != 0 {
		t.Fatal("a failing release was counted")
	}
	if _, ok := p.heldClaimFor("ns1/sb-pod"); ok {
		t.Fatal("a claim on its way out must leave its key")
	}
	if err := p.CreatePod(ctx, sandboxPod("ns1", "sb-pod", "uid-2", "sb-owner", "owner-uid-2")); err != nil {
		t.Fatalf("CreatePod replacement: %v", err)
	}
	fresh, ok := p.claimFor("ns1/sb-pod")
	if !ok || fresh.ID == old.ID {
		t.Fatalf("the replacement must claim fresh: %+v ok=%v", fresh, ok)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var st stateFile
	if err := json.Unmarshal(b, &st); err != nil {
		t.Fatal(err)
	}
	if len(st.Releasing) != 1 || st.Releasing[0].ID != old.ID || st.Releasing[0].Token != old.Token {
		t.Fatalf("the unreleased credential is not on disk: %+v", st.Releasing)
	}

	sd.releaseErr = nil
	p.recheckOwners(ctx, time.Now(), time.Second)
	if sd.releaseCount() != 1 || sd.releases[0] != old.ID {
		t.Fatalf("the queued release was not retried: %v", sd.releases)
	}
	if still, ok := p.claimFor("ns1/sb-pod"); !ok || still.ID != fresh.ID {
		t.Fatalf("the retry disturbed the replacement's claim: %+v ok=%v", still, ok)
	}
	if len(p.releasing) != 0 {
		t.Fatalf("queue not drained: %+v", p.releasing)
	}
}

func TestADrainKeepsExactlyTheReleasesThatFailed(t *testing.T) {
	sd := &fakeSandboxd{}
	p := newTestProvider(t, sd, dynWith(t), "")
	p.mu.Lock()
	p.releasing = []Claim{{ID: "sb_a", Token: "t"}, {ID: "sb_b", Token: "t"}}
	p.mu.Unlock()
	sd.onRelease = func() { sd.onRelease = func() { sd.releaseErr = errTestReleaseFailed } }

	if !p.drainReleases(t.Context()) {
		t.Fatal("setup: the first release should succeed")
	}
	if len(p.releasing) != 1 || p.releasing[0].ID != "sb_b" || sd.releaseCount() != 1 {
		t.Fatalf("a drain must keep exactly the release that failed: queue=%v released=%v", p.releasing, sd.releases)
	}
}

func TestARestartRetriesAQueuedRelease(t *testing.T) {
	ctx := t.Context()
	path := t.TempDir() + "/claims.json"
	sd := &fakeSandboxd{}
	dyn := dynWith(t, ownerSandbox("ns1", "sb-owner", "owner-uid", false))
	p1 := newTestProvider(t, sd, dyn, path)
	pod := sandboxPod("ns1", "sb-pod", "uid-1", "sb-owner", "owner-uid")
	if err := p1.CreatePod(ctx, pod); err != nil {
		t.Fatalf("CreatePod: %v", err)
	}
	if err := p1.DeletePod(ctx, pod); err != nil {
		t.Fatalf("DeletePod: %v", err)
	}
	old, _ := p1.claimFor("ns1/sb-pod")
	if err := dyn.Tracker().Delete(sandboxGVR, "ns1", "sb-owner"); err != nil {
		t.Fatalf("delete owner: %v", err)
	}
	sd.releaseErr = errTestReleaseFailed
	p1.recheckOwners(ctx, time.Now(), time.Second)

	sd.releaseErr = nil
	p2 := newTestProvider(t, sd, dyn, path)
	p2.recheckOwners(ctx, time.Now(), time.Second)
	if sd.releaseCount() != 1 || sd.releases[0] != old.ID {
		t.Fatalf("the restarted provider did not finish the queued release: %v", sd.releases)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), old.ID) {
		t.Fatalf("the released credential is still on disk: %s", b)
	}
}

type listAfterHook struct {
	onList func()
}

func (l *listAfterHook) Sandboxes(_ context.Context) ([]sandboxd.SandboxSummary, error) {
	if l.onList != nil {
		l.onList()
	}
	return nil, nil
}

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
	onRelease  func()
	issued     map[string]string
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
	if f.issued == nil {
		f.issued = map[string]string{}
	}
	f.issued[id] = "tok-" + id
	f.live = append(f.live, sandboxd.SandboxSummary{ID: id})
	return sandboxd.ClaimResult{ID: id, Token: f.issued[id], OwnerAddr: "10.99.0.5:7777", Deadline: f.deadline}, nil
}

func (f *fakeSandboxd) Release(_ context.Context, id, token string) error {
	if f.onRelease != nil {
		f.onRelease()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.releaseErr != nil {
		return f.releaseErr
	}
	if want, ok := f.issued[id]; ok && token != want {
		return errTestWrongToken
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
	return slices.Clone(f.live), nil
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

func ownerSandbox(ns, name string, uid types.UID, deleting bool) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetAPIVersion("agents.x-k8s.io/v1beta1")
	u.SetKind("Sandbox")
	u.SetNamespace(ns)
	u.SetName(name)
	u.SetUID(uid)
	if deleting {
		u.SetDeletionTimestamp(new(metav1.Now()))
		u.SetFinalizers([]string{"keep"})
	}
	return u
}

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

func blockStateWrites(t *testing.T, dir string) {
	t.Helper()
	if err := os.Symlink(t.TempDir(), dir+"/claims.json.tmp"); err != nil {
		t.Fatal(err)
	}
}

func newTestProvider(t *testing.T, sd *fakeSandboxd, dyn *dynamicfake.FakeDynamicClient, statePath string) *Provider {
	t.Helper()
	cfg := Config{Client: sd, Lister: sd, StatePath: statePath, Logger: logr.Discard()}
	if dyn != nil {
		cfg.Dynamic = dyn
	}
	p, err := New(t.Context(), cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return p
}
