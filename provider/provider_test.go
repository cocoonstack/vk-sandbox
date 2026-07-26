package provider

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

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

// fakeSandboxd implements SandboxdClient + Lister with call accounting.
type fakeSandboxd struct {
	mu       sync.Mutex
	claims   int
	releases []string
	listErr  error
	live     []ListedSandbox
	claimErr error
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
