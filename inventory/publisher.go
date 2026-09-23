// Package inventory publishes this node's one O(nodes) NodeInventory for the
// operator's aggregation layer: live entries from sandboxd's index plus the
// node's advertise address and warm capacity. Per-sandbox truth stays on the node.
package inventory

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/cocoonstack/sandbox-operator/pkg/scale"
	"github.com/cocoonstack/vk-sandbox/provider"
)

// ClaimAddresses exposes the provider's sandboxd id → address view.
type ClaimAddresses interface {
	ClaimAddresses() map[string]string
}

// NodeGetter reads the Node that owns this node's NodeInventory.
type NodeGetter interface {
	Get(ctx context.Context, name string, opts metav1.GetOptions) (*corev1.Node, error)
}

var _ scale.NodeLiveSource = (*LiveSource)(nil)

// LiveSource implements scale.NodeLiveSource from the node's own state, never a cluster-wide LIST.
type LiveSource struct {
	claims ClaimAddresses
	lister provider.Lister
}

// NewLiveSource builds a LiveSource over the provider's claim addresses and the sandboxd index.
func NewLiveSource(claims ClaimAddresses, lister provider.Lister) *LiveSource {
	return &LiveSource{claims: claims, lister: lister}
}

// LiveSandboxes reads the sandboxd index, which is authoritative: it also holds claims the aggregated apiserver made directly against sandboxd.
func (s *LiveSource) LiveSandboxes(ctx context.Context) ([]scale.InventoryEntry, error) {
	listed, err := s.lister.Sandboxes(ctx)
	if err != nil {
		return nil, err
	}
	addrByID := s.claims.ClaimAddresses()

	out := make([]scale.InventoryEntry, 0, len(listed))
	for _, row := range listed {
		e := scale.EntryFromSummary(row)
		e.Address = addrByID[row.ID]
		out = append(out, e)
	}
	slices.SortFunc(out, func(a, b scale.InventoryEntry) int { return cmp.Compare(a.Name, b.Name) })
	return out, nil
}

// Publisher server-side-applies this node's single NodeInventory: live entries plus address and warm capacity.
type Publisher struct {
	node    string
	live    scale.NodeLiveSource
	info    NodeInfoSource
	nodes   NodeGetter
	applier scale.InventoryApplier
	log     logr.Logger

	nodeSeen bool
}

// NewPublisher builds a Publisher for node; a nil info publishes entries only.
func NewPublisher(node string, live scale.NodeLiveSource, info NodeInfoSource, nodes NodeGetter, applier scale.InventoryApplier, log logr.Logger) *Publisher {
	return &Publisher{node: node, live: live, info: info, nodes: nodes, applier: applier, log: log}
}

// Publish server-side-applies this node's live sandboxes as a NodeInventory object, returning the summarized entry count.
func (p *Publisher) Publish(ctx context.Context) (int, error) {
	owners, err := p.owners(ctx)
	if err != nil {
		return 0, err
	}
	entries, err := p.live.LiveSandboxes(ctx)
	if err != nil {
		return 0, fmt.Errorf("inventory: read node %q live sandboxes: %w", p.node, err)
	}
	inv := &scale.NodeInventory{
		Kind:            scale.NodeInventoryGVK.Kind,
		APIVersion:      scale.NodeInventoryGVK.GroupVersion().String(),
		Name:            p.node,
		Node:            p.node,
		Entries:         entries,
		OwnerReferences: owners,
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

// PublishPeriodically runs Publish on interval until ctx is canceled; failures are logged and retried next tick.
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

// owners names this node's Node as the owner: none before the Node registers, an error once it is deleted.
func (p *Publisher) owners(ctx context.Context) ([]metav1.OwnerReference, error) {
	node, err := p.nodes.Get(ctx, p.node, metav1.GetOptions{ResourceVersion: "0"})
	switch {
	case err == nil:
		p.nodeSeen = true
		return []metav1.OwnerReference{{APIVersion: "v1", Kind: "Node", Name: node.Name, UID: node.UID}}, nil
	case apierrors.IsNotFound(err) && !p.nodeSeen:
		return nil, nil
	case apierrors.IsNotFound(err):
		return nil, fmt.Errorf("inventory: node %q was deleted; its inventory is not republished", p.node)
	default:
		return nil, fmt.Errorf("inventory: read node %q: %w", p.node, err)
	}
}
