// Copyright 2026 The CocoonStack Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package inventory

import (
	"context"
	"testing"

	"github.com/go-logr/logr"

	extv1beta1 "github.com/cocoonstack/cocoon-sandbox-operator/extensions/api/v1beta1"
	"github.com/cocoonstack/cocoon-sandbox-operator/pkg/scale"

	"github.com/cocoonstack/vk-cocoon-sandbox/provider"
)

type staticClaims map[string]provider.Claim

func (s staticClaims) SnapshotClaims() map[string]provider.Claim { return s }

type staticLister []provider.ListedSandbox

func (s staticLister) ListSandboxes(context.Context) ([]provider.ListedSandbox, error) {
	return s, nil
}

// TestLiveSandboxes maps pod-bound claims into inventory entries with the
// node-observed phase, and drops claims whose VM is gone from the sandboxd index
// (computed phase "Gone") so only live sandboxes are published. Unbound
// (pool-warm) sandboxes are omitted.
func TestLiveSandboxes(t *testing.T) {
	claims := staticClaims{
		"ns1/pod-a": {ID: "sb_a", Address: "10.0.0.5:7777"},
		"ns1/pod-b": {ID: "sb_b"},
		"ns1/pod-c": {ID: "sb_gone"}, // claim whose VM left the index → Gone
	}
	lister := staticLister{
		{ID: "sb_a"},
		{ID: "sb_b", Hibernated: true},
		{ID: "sb_warm_unbound"},
	}
	src := NewLiveSource(claims, lister)
	got, err := src.LiveSandboxes(context.Background())
	if err != nil {
		t.Fatalf("LiveSandboxes: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("want 2 live entries (Gone filtered, unbound omitted), got %d: %+v", len(got), got)
	}
	byName := map[string]string{}
	for _, e := range got {
		byName[e.Name] = e.Phase
		if e.ClaimRef != e.Name {
			t.Errorf("entry %s: claimRef %q != name", e.Name, e.ClaimRef)
		}
	}
	if byName["ns1/pod-a"] != "Running" || byName["ns1/pod-b"] != "Hibernated" {
		t.Fatalf("phase mapping wrong: %v", byName)
	}
	if _, ok := byName["ns1/pod-c"]; ok {
		t.Fatalf("Gone claim ns1/pod-c must not be published: %v", byName)
	}
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

	n, err := pub.Publish(context.Background())
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
	if _, err := pub.Publish(context.Background()); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if applier.got.Address != "" || applier.got.Pools != nil {
		t.Fatalf("nil info must leave address/pools empty: %+v", applier.got)
	}
}
