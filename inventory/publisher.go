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

// Package inventory bridges this node's live sandboxes into the operator's L3
// aggregation layer: it implements scale.NodeLiveSource over the provider's
// claims table + the sandboxd operator index, and publishes the one O(nodes)
// NodeInventory object this node contributes — its live entries plus the node's
// sandboxd advertise address and per-pool warm capacity. The per-sandbox truth
// stays on the node; etcd stores only the summary — the metrics.k8s.io pattern
// from the operator's scaling design.
package inventory

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/go-logr/logr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/cocoonstack/cocoon-sandbox-operator/pkg/scale"

	"github.com/cocoonstack/vk-cocoon-sandbox/provider"
)

// ClaimSnapshotter exposes the provider's pod-key → claim view.
type ClaimSnapshotter interface {
	SnapshotClaims() map[string]provider.Claim
}

// LiveSource implements scale.NodeLiveSource: the node's own live sandbox
// state, never a cluster-wide LIST.
type LiveSource struct {
	claims ClaimSnapshotter
	lister provider.Lister
}

// NewLiveSource builds a LiveSource over the provider's claims and the
// sandboxd index.
func NewLiveSource(claims ClaimSnapshotter, lister provider.Lister) *LiveSource {
	return &LiveSource{claims: claims, lister: lister}
}

var _ scale.NodeLiveSource = (*LiveSource)(nil)

// LiveSandboxes summarizes the node's live sandboxes as inventory entries. A
// sandbox bound to a pod carries that pod as its name and claim reference; an
// unbound (pool-warm or orphan-candidate) sandbox is deliberately omitted —
// inventory serves the aggregated sandboxes view, which is pod-scoped.
//
// A claim whose VM is no longer in the node's sandboxd index (computed phase
// "Gone") is a stale record; it is skipped so the aggregated view publishes only
// live (Running/Hibernated) sandboxes and never surfaces dead entries.
func (s *LiveSource) LiveSandboxes(ctx context.Context) ([]scale.InventoryEntry, error) {
	listed, err := s.lister.ListSandboxes(ctx)
	if err != nil {
		return nil, err
	}
	live := make(map[string]provider.ListedSandbox, len(listed))
	for _, row := range listed {
		live[row.ID] = row
	}

	claims := s.claims.SnapshotClaims()
	keys := make([]string, 0, len(claims))
	for k := range claims {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	out := make([]scale.InventoryEntry, 0, len(keys))
	for _, key := range keys {
		c := claims[key]
		row, alive := live[c.ID]
		if !alive {
			// The claim's VM is gone from the node's sandboxd index: a stale
			// (Gone) claim. Publish only live sandboxes so the aggregated
			// apiserver never shows dead entries.
			continue
		}
		phase := "Running"
		if row.Hibernated {
			phase = "Hibernated"
		}
		out = append(out, scale.InventoryEntry{
			Name:     key, // "<namespace>/<name>", the store's expected shape
			Phase:    phase,
			ClaimRef: key,
			Address:  c.Address,
		})
	}
	return out, nil
}

// Publisher server-side-applies this node's single NodeInventory object on a slow
// cadence: its live entries (from a NodeLiveSource) plus the node's sandboxd
// advertise address and per-pool warm capacity (from a NodeInfoSource). This is
// the entire L3 write path for this node — one O(nodes) apply, no per-sandbox
// etcd object. It composes the operator's stable scale.InventoryApplier so the
// SSA/RESTMapper details live in exactly one place.
type Publisher struct {
	node    string
	live    scale.NodeLiveSource
	info    NodeInfoSource
	applier scale.InventoryApplier
	log     logr.Logger
}

// NewPublisher builds a Publisher for node. info may be nil, in which case the
// applied NodeInventory carries entries only (no address/pools).
func NewPublisher(node string, live scale.NodeLiveSource, info NodeInfoSource, applier scale.InventoryApplier, log logr.Logger) *Publisher {
	return &Publisher{node: node, live: live, info: info, applier: applier, log: log}
}

// Publish reads the node's live sandboxes and node info and server-side-applies a
// single NodeInventory object for the node, returning the number of summarized
// entries.
func (p *Publisher) Publish(ctx context.Context) (int, error) {
	entries, err := p.live.LiveSandboxes(ctx)
	if err != nil {
		return 0, fmt.Errorf("inventory: read node %q live sandboxes: %w", p.node, err)
	}
	inv := &scale.NodeInventory{
		TypeMeta: metav1.TypeMeta{
			Kind:       scale.NodeInventoryGVK.Kind,
			APIVersion: scale.NodeInventoryGVK.GroupVersion().String(),
		},
		ObjectMeta: metav1.ObjectMeta{Name: p.node},
		Node:       p.node,
		Entries:    entries,
	}
	if p.info != nil {
		ni, infoErr := p.info.NodeInfo(ctx)
		if infoErr != nil {
			return 0, fmt.Errorf("inventory: read node %q info: %w", p.node, infoErr)
		}
		inv.Address = ni.Address
		inv.Pools = ni.Pools
	}
	if err := p.applier.Apply(ctx, inv); err != nil {
		return 0, fmt.Errorf("inventory: apply node %q inventory: %w", p.node, err)
	}
	return len(entries), nil
}

// PublishPeriodically runs Publish on interval until ctx is cancelled. Publish
// failures are logged, not fatal — the next tick rebuilds from live state.
func (p *Publisher) PublishPeriodically(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if n, err := p.Publish(ctx); err != nil {
			p.log.Error(err, "node inventory publish failed; will retry on next tick", "node", p.node)
		} else {
			p.log.V(1).Info("published node inventory", "node", p.node, "entries", n)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
