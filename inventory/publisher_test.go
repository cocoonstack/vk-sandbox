package inventory

import (
	"context"
	"errors"
	"slices"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"

	extv1beta1 "github.com/cocoonstack/sandbox-operator/api/v1beta1"
	"github.com/cocoonstack/sandbox-operator/pkg/sandboxd"
	"github.com/cocoonstack/sandbox-operator/pkg/scale"
	"github.com/cocoonstack/vk-sandbox/provider"
)

func TestLiveSandboxes(t *testing.T) {
	claims := staticClaims{
		"ns1/pod-a": {ID: "sb_a", Address: "10.0.0.5:7777"},
		"ns1/pod-b": {ID: "sb_b"},
	}
	deadline := time.Date(2026, 8, 17, 12, 0, 0, 0, time.UTC)
	claimed := time.Date(2026, 8, 17, 11, 0, 0, 0, time.UTC)
	lister := staticLister{
		{ID: "sb_a", ClaimRef: "ns1/pod-a", Deadline: deadline, ClaimedAt: claimed, Key: sandboxd.PoolKey{Template: "rt:24.04"}},
		{ID: "sb_b", ClaimRef: "ns1/pod-b", Hibernated: true},
		{ID: "sb_direct", ClaimRef: "ns2/direct-c"},
		{ID: "sb_noref"},
	}
	src := NewLiveSource(claims, lister)
	got, err := src.LiveSandboxes(t.Context())
	if err != nil {
		t.Fatalf("LiveSandboxes: %v", err)
	}
	if len(got) != 4 {
		t.Fatalf("want 4 entries (every listed sandbox published), got %d: %+v", len(got), got)
	}
	byName := map[string]scale.InventoryEntry{}
	for _, e := range got {
		byName[e.Name] = e
		if e.ClaimRef != e.Name {
			t.Errorf("entry %s: claimRef %q != name", e.Name, e.ClaimRef)
		}
	}

	if e := byName["ns1/pod-a"]; e.Phase != "Running" || e.Address != "10.0.0.5:7777" || e.ID != "sb_a" || e.Deadline == nil || !e.Deadline.Time.Equal(deadline) {
		t.Errorf("ns1/pod-a: got phase=%q addr=%q id=%q deadline=%v, want Running / 10.0.0.5:7777 / sb_a / %v", e.Phase, e.Address, e.ID, e.Deadline, deadline)
	}
	if e := byName["ns1/pod-a"]; e.Template != "rt:24.04" {
		t.Errorf("ns1/pod-a: template = %q, want rt:24.04", e.Template)
	}
	if e := byName["ns1/pod-a"]; e.ClaimedAt == nil || !e.ClaimedAt.Time.Equal(claimed) {
		t.Errorf("ns1/pod-a: claimedAt = %v, want %v", e.ClaimedAt, claimed)
	}
	if e := byName["ns1/pod-b"]; e.ClaimedAt != nil {
		t.Errorf("ns1/pod-b: claimedAt = %v, want none when the node publishes no claimed_at", e.ClaimedAt)
	}
	if e := byName["ns1/pod-b"]; e.Phase != "Hibernated" || e.Address != "" || e.ID != "sb_b" || e.Deadline != nil {
		t.Errorf("ns1/pod-b: got phase=%q addr=%q id=%q deadline=%v, want Hibernated / no address / sb_b / none", e.Phase, e.Address, e.ID, e.Deadline)
	}

	if e, ok := byName["ns2/direct-c"]; !ok || e.Phase != "Running" || e.Address != "" || e.ID != "sb_direct" {
		t.Errorf("apiserver-direct claim ns2/direct-c must be published with its id: %+v (ok=%v)", e, ok)
	}

	if e, ok := byName["sb_noref"]; !ok || e.Phase != "Running" || e.ID != "sb_noref" {
		t.Errorf("ref-less claim must fall back to the id: %+v (ok=%v)", e, ok)
	}
}

func TestPublisherStampsNodeInfo(t *testing.T) {
	live := staticLive{{Name: "ns1/pod-a", Phase: "Running", ClaimRef: "ns1/pod-a", Address: "10.0.0.5:7777"}}
	info := staticInfo{info: NodeInfo{
		Address: "172.16.26.2:7777",
		Pools: []extv1beta1.PoolCapacity{
			{Template: "base:24.04", Net: "none", Size: "small", Warm: 4, Target: 4},
		},
	}}
	applier := &captureApplier{}
	pub := NewPublisher("vk-sandboxd-26", live, info, registered("vk-sandboxd-26", "uid-26"), applier, logr.Discard())

	n, err := pub.Publish(t.Context())
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if n != 1 {
		t.Fatalf("want 1 summarized entry, got %d", n)
	}
	got := applier.got
	if got == nil {
		t.Fatal("applier received no inventory")
	}
	if got.Kind != "NodeInventory" || got.APIVersion == "" {
		t.Fatalf("GVK not stamped: %q %q", got.APIVersion, got.Kind)
	}
	if got.Node != "vk-sandboxd-26" || got.Name != "vk-sandboxd-26" {
		t.Fatalf("node/name wrong: node=%q name=%q", got.Node, got.Name)
	}
	if got.Address != "172.16.26.2:7777" {
		t.Fatalf("advertise address not published: %q", got.Address)
	}
	if len(got.Pools) != 1 || got.Pools[0].Template != "base:24.04" || got.Pools[0].Warm != 4 || got.Pools[0].Target != 4 {
		t.Fatalf("pool capacity not published: %+v", got.Pools)
	}
	if len(got.Entries) != 1 || got.Entries[0].Name != "ns1/pod-a" {
		t.Fatalf("entries wrong: %+v", got.Entries)
	}
}

func TestPublisherWithoutInfo(t *testing.T) {
	applier := &captureApplier{}
	pub := NewPublisher("n1", staticLive{}, nil, registered("n1", "uid-1"), applier, logr.Discard())
	if _, err := pub.Publish(t.Context()); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if applier.got.Address != "" || applier.got.Pools != nil {
		t.Fatalf("nil info must leave address/pools empty: %+v", applier.got)
	}
}

func TestPublisherHandsTheInventoryToItsNode(t *testing.T) {
	applier := &captureApplier{}
	pub := NewPublisher("n1", staticLive{}, nil, registered("n1", "uid-1"), applier, logr.Discard())
	if _, err := pub.Publish(t.Context()); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	want := []metav1.OwnerReference{{APIVersion: "v1", Kind: "Node", Name: "n1", UID: "uid-1"}}
	if !slices.Equal(applier.got.OwnerReferences, want) {
		t.Fatalf("ownerReferences = %+v, want %+v so deleting the Node collects its inventory", applier.got.OwnerReferences, want)
	}
}

func TestPublisherPublishesBeforeTheNodeRegisters(t *testing.T) {
	applier := &captureApplier{}
	pub := NewPublisher("n1", staticLive{}, nil, registered("other", "uid-2"), applier, logr.Discard())
	if _, err := pub.Publish(t.Context()); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if applier.got == nil || len(applier.got.OwnerReferences) != 0 {
		t.Fatalf("a node not yet registered must still publish, unowned: %+v", applier.got)
	}
}

func TestPublisherStopsOnceItsNodeIsDeleted(t *testing.T) {
	cs := fake.NewClientset(&corev1.Node{Name: "n1", UID: "uid-1"})
	applier := &captureApplier{}
	pub := NewPublisher("n1", staticLive{}, nil, cs.CoreV1().Nodes(), applier, logr.Discard())
	if _, err := pub.Publish(t.Context()); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if err := cs.CoreV1().Nodes().Delete(t.Context(), "n1", metav1.DeleteOptions{}); err != nil {
		t.Fatalf("delete node: %v", err)
	}
	applier.got = nil
	if _, err := pub.Publish(t.Context()); err == nil || applier.got != nil {
		t.Fatalf("a deleted Node must stop the publish, or the collected inventory comes back unowned: err=%v applied=%+v", err, applier.got)
	}
}

func TestPublisherHoldsTheInventoryWhenTheNodeIsUnreadable(t *testing.T) {
	applier := &captureApplier{}
	pub := NewPublisher("n1", staticLive{}, nil, unreadableNodes{}, applier, logr.Discard())
	if _, err := pub.Publish(t.Context()); err == nil || applier.got != nil {
		t.Fatalf("an unreadable Node must fail the publish before the apply: err=%v applied=%+v", err, applier.got)
	}
}

func TestPublisherHoldsTheInventoryWhenNodeInfoFails(t *testing.T) {
	applier := &captureApplier{}
	info := staticInfo{err: errors.New("sandboxd unreachable")}
	pub := NewPublisher("n1", staticLive{}, info, registered("n1", "uid-1"), applier, logr.Discard())
	if _, err := pub.Publish(t.Context()); err == nil || applier.got != nil {
		t.Fatalf("a failed node-info read must skip the apply, not publish the node without its address and pools: err=%v applied=%+v", err, applier.got)
	}
}

func TestPublisherHoldsTheInventoryWhenSandboxdCannotBeListed(t *testing.T) {
	applier := &captureApplier{}
	live := NewLiveSource(staticClaims{}, unlistableSandboxd{})
	pub := NewPublisher("n1", live, nil, registered("n1", "uid-1"), applier, logr.Discard())
	if _, err := pub.Publish(t.Context()); err == nil || applier.got != nil {
		t.Fatalf("a failed sandboxd listing must skip the apply, not publish the node with no sandboxes: err=%v applied=%+v", err, applier.got)
	}
}

func TestPublisherPublishesAtStartAndOnEveryTick(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		applier := &captureApplier{}
		pub := NewPublisher("n1", staticLive{}, nil, registered("n1", "uid-1"), applier, logr.Discard())
		go pub.PublishPeriodically(t.Context(), time.Second)
		time.Sleep(2 * time.Second)
		synctest.Wait()
		if got := applier.applies.Load(); got != 3 {
			t.Fatalf("applies after two 1s ticks = %d, want 3 (start, 1s, 2s)", got)
		}
	})
}

type staticClaims map[string]provider.Claim

func (s staticClaims) ClaimAddresses() map[string]string {
	out := map[string]string{}
	for _, c := range s {
		if c.Address != "" {
			out[c.ID] = c.Address
		}
	}
	return out
}

type staticLister []sandboxd.SandboxSummary

func (s staticLister) Sandboxes(context.Context) ([]sandboxd.SandboxSummary, error) {
	return s, nil
}

type staticLive []scale.InventoryEntry

func (s staticLive) LiveSandboxes(context.Context) ([]scale.InventoryEntry, error) { return s, nil }

type staticInfo struct {
	info NodeInfo
	err  error
}

func (s staticInfo) NodeInfo(context.Context) (NodeInfo, error) { return s.info, s.err }

type captureApplier struct {
	got     *scale.NodeInventory
	applies atomic.Int32
}

func (c *captureApplier) Apply(_ context.Context, inv *scale.NodeInventory) error {
	c.got = inv
	c.applies.Add(1)
	return nil
}

type unreadableNodes struct{}

func (unreadableNodes) Get(context.Context, string, metav1.GetOptions) (*corev1.Node, error) {
	return nil, errors.New("apiserver unavailable")
}

type unlistableSandboxd struct{}

func (unlistableSandboxd) Sandboxes(context.Context) ([]sandboxd.SandboxSummary, error) {
	return nil, errors.New("sandboxd unreachable")
}

func registered(name, uid string) NodeGetter {
	return fake.NewClientset(&corev1.Node{Name: name, UID: types.UID(uid)}).CoreV1().Nodes()
}
