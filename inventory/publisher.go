// Package inventory bridges this node's live sandboxes into the operator's L3
// aggregation layer: it implements scale.NodeLiveSource over the provider's
// claims table + the sandboxd operator index, and publishes the one O(nodes)
// NodeInventory object this node contributes — its live entries plus the node's
// sandboxd advertise address and per-pool warm capacity. The per-sandbox truth
// stays on the node; etcd stores only the summary — the metrics.k8s.io pattern
// from the operator's scaling design.
package inventory

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/go-logr/logr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/cocoonstack/sandbox-operator/pkg/scale"
	"github.com/cocoonstack/vk-sandbox/provider"
)

// phaseRunning is the sandbox phase a live claim reports in the inventory.
const phaseRunning = "Running"

// ClaimAddresses exposes the provider's sandboxd id → address view.
type ClaimAddresses interface {
	ClaimAddresses() map[string]string
}

var _ scale.NodeLiveSource = (*LiveSource)(nil)

// LiveSource implements scale.NodeLiveSource: the node's own live sandbox
// state, never a cluster-wide LIST.
type LiveSource struct {
	claims ClaimAddresses
	lister provider.Lister
}

// NewLiveSource builds a LiveSource over the provider's claim addresses and the sandboxd index.
func NewLiveSource(claims ClaimAddresses, lister provider.Lister) *LiveSource {
	return &LiveSource{claims: claims, lister: lister}
}

// the sandboxd index is authoritative: it also holds claims the aggregated apiserver made directly against sandboxd
func (s *LiveSource) LiveSandboxes(ctx context.Context) ([]scale.InventoryEntry, error) {
	listed, err := s.lister.Sandboxes(ctx)
	if err != nil {
		return nil, err
	}
	addrByID := s.claims.ClaimAddresses()

	out := make([]scale.InventoryEntry, 0, len(listed))
	for _, row := range listed {
		name := row.ClaimRef
		if name == "" {
			name = row.ID
		}
		phase := phaseRunning
		if row.Hibernated {
			phase = "Hibernated"
		}
		e := scale.InventoryEntry{
			Name:     name,
			ID:       row.ID,
			Phase:    phase,
			ClaimRef: name,
			Address:  addrByID[row.ID],
		}
		if !row.Deadline.IsZero() {
			d := metav1.NewTime(row.Deadline)
			e.Deadline = &d
		}
		out = append(out, e)
	}
	slices.SortFunc(out, func(a, b scale.InventoryEntry) int { return cmp.Compare(a.Name, b.Name) })
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
		Kind:       scale.NodeInventoryGVK.Kind,
		APIVersion: scale.NodeInventoryGVK.GroupVersion().String(),
		Name:       p.node,
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

// PublishPeriodically runs Publish on interval until ctx is canceled. Publish
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
