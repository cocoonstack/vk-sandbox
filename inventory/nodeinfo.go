package inventory

import (
	"context"

	extv1beta1 "github.com/cocoonstack/sandbox-operator/api/v1beta1"
	"github.com/cocoonstack/sandbox-operator/pkg/sandboxd"
	"github.com/cocoonstack/sandbox-operator/pkg/scale"
)

// InfoClient reads this node's warm-pool state from sandboxd; *sandboxd.Client satisfies it.
type InfoClient interface {
	Info(ctx context.Context) (*sandboxd.NodeInfo, error)
}

// NodeInfo is the node summary stamped onto NodeInventory: the sandboxd advertise address, per-pool warm capacity and promoted templates.
type NodeInfo struct {
	Address   string
	Pools     []extv1beta1.PoolCapacity
	Templates []extv1beta1.PromotedTemplate
}

// NodeInfoSource yields this node's NodeInfo.
type NodeInfoSource interface {
	NodeInfo(ctx context.Context) (NodeInfo, error)
}

type sandboxdInfoSource struct {
	address string
	info    InfoClient
}

// NewNodeInfoSource pairs the sandboxd advertise address with live warm-pool capacity from info.
func NewNodeInfoSource(address string, info InfoClient) NodeInfoSource {
	return &sandboxdInfoSource{address: address, info: info}
}

func (s *sandboxdInfoSource) NodeInfo(ctx context.Context) (NodeInfo, error) {
	info, err := s.info.Info(ctx)
	if err != nil {
		return NodeInfo{}, err
	}
	return NodeInfo{Address: s.address, Pools: scale.PoolCapacityFromInfo(info), Templates: scale.TemplatesFromInfo(info)}, nil
}
