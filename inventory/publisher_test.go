package inventory

import (
	"context"
	"testing"

	"github.com/go-logr/logr"

	extv1beta1 "github.com/cocoonstack/sandbox-operator/extensions/api/v1beta1"
	"github.com/cocoonstack/sandbox-operator/pkg/scale"

	"github.com/cocoonstack/vk-sandbox/provider"
)

// TestLiveSandboxes publishes the node's sandboxd operator index as inventory
// entries: each listed sandbox is named by its claim ref (falling back to the
// sandboxd id when it has none), carries the sandboxd "sb_..." id (the release
// handle), maps phase from the hibernated bit, and is stamped with the address
// from this provider's own claim when it tracks the sandbox. Apiserver-direct
// claims — listed by sandboxd but absent from this provider's claims table —
// must still be published, named by their claim ref and carrying their id.
func TestLiveSandboxes(t *testing.T) {
	// This provider tracks pod-a (with its address) and pod-b; the apiserver-
	// direct claim and the ref-less claim are absent from its claims table.
	claims := staticClaims{
		"ns1/pod-a": {ID: "sb_a", Address: "10.0.0.5:7777"},
		"ns1/pod-b": {ID: "sb_b"},
	}
	// The node's sandboxd index — the authoritative set of claims on this node.
	lister := staticLister{
		{ID: "sb_a", ClaimRef: "ns1/pod-a"},
		{ID: "sb_b", ClaimRef: "ns1/pod-b", Hibernated: true},
		{ID: "sb_direct", ClaimRef: "ns2/direct-c"}, // apiserver-direct: not in claims
		{ID: "sb_noref"}, // no claim ref: falls back to the id
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
	// Named by claim ref, id carried, phase mapped, address stamped from the provider's claim.
	if e := byName["ns1/pod-a"]; e.Phase != "Running" || e.Address != "10.0.0.5:7777" || e.ID != "sb_a" {
		t.Errorf("ns1/pod-a: got phase=%q addr=%q id=%q, want Running / 10.0.0.5:7777 / sb_a", e.Phase, e.Address, e.ID)
	}
	if e := byName["ns1/pod-b"]; e.Phase != "Hibernated" || e.Address != "" || e.ID != "sb_b" {
		t.Errorf("ns1/pod-b: got phase=%q addr=%q id=%q, want Hibernated / no address / sb_b", e.Phase, e.Address, e.ID)
	}
	// Apiserver-direct claim: published under its claim ref though this provider
	// never claimed it (so no address is available here), still carrying its id.
	if e, ok := byName["ns2/direct-c"]; !ok || e.Phase != "Running" || e.Address != "" || e.ID != "sb_direct" {
		t.Errorf("apiserver-direct claim ns2/direct-c must be published with its id: %+v (ok=%v)", e, ok)
	}
	// A sandbox sandboxd listed with no claim ref falls back to the sandboxd id
	// for its name, and carries that id as the release handle too.
	if e, ok := byName["sb_noref"]; !ok || e.Phase != "Running" || e.ID != "sb_noref" {
		t.Errorf("ref-less claim must fall back to the id: %+v (ok=%v)", e, ok)
	}
}

// TestPublisherStampsNodeInfo verifies the node's published NodeInventory carries
// its live entries plus the sandboxd advertise address and per-pool warm capacity
// — the fields the aggregated apiserver reads to pick a warm node and route a
// claim.
func TestPublisherStampsNodeInfo(t *testing.T) {
	live := staticLive{{Name: "ns1/pod-a", Phase: "Running", ClaimRef: "ns1/pod-a", Address: "10.0.0.5:7777"}}
	info := staticInfo{info: NodeInfo{
		Address: "172.16.26.2:7777",
		Pools: []extv1beta1.PoolCapacity{
			{Template: "base:24.04", Net: "none", Size: "small", Warm: 4, Target: 4},
		},
	}}
	applier := &captureApplier{}
	pub := NewPublisher("vk-sandboxd-26", live, info, applier, logr.Discard())

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

// TestPublisherWithoutInfo confirms a nil NodeInfoSource yields an entries-only
// inventory (no address/pools), keeping the source optional.
func TestPublisherWithoutInfo(t *testing.T) {
	applier := &captureApplier{}
	pub := NewPublisher("n1", staticLive{}, nil, applier, logr.Discard())
	if _, err := pub.Publish(t.Context()); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if applier.got.Address != "" || applier.got.Pools != nil {
		t.Fatalf("nil info must leave address/pools empty: %+v", applier.got)
	}
}

type staticClaims map[string]provider.Claim

func (s staticClaims) SnapshotClaims() map[string]provider.Claim { return s }

type staticLister []provider.ListedSandbox

func (s staticLister) ListSandboxes(context.Context) ([]provider.ListedSandbox, error) {
	return s, nil
}

type staticLive []scale.InventoryEntry

func (s staticLive) LiveSandboxes(context.Context) ([]scale.InventoryEntry, error) { return s, nil }

type staticInfo struct {
	info NodeInfo
	err  error
}

func (s staticInfo) NodeInfo(context.Context) (NodeInfo, error) { return s.info, s.err }

type captureApplier struct{ got *scale.NodeInventory }

func (c *captureApplier) Apply(_ context.Context, inv *scale.NodeInventory) error {
	c.got = inv
	return nil
}
