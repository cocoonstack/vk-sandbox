package inventory

import (
	"context"

	extv1beta1 "github.com/cocoonstack/sandbox-operator/extensions/api/v1beta1"
)

// InfoClient reads this node's warm-pool capacity from sandboxd GET /v1/info.
// *sandboxdx.ListClient satisfies it.
type InfoClient interface {
	Info(ctx context.Context) ([]extv1beta1.PoolCapacity, error)
}

// NodeInfo is the node-level summary the publisher stamps onto NodeInventory
// alongside its live entries: the sandboxd advertise address (claim routing) and
// per-pool warm capacity (node-picking).
type NodeInfo struct {
	Address string
	Pools   []extv1beta1.PoolCapacity
}

// NodeInfoSource yields this node's NodeInfo. The address is a static
// configuration value (the node's sandboxd advertise address); the pools are read
// live from sandboxd.
type NodeInfoSource interface {
	NodeInfo(ctx context.Context) (NodeInfo, error)
}

type sandboxdInfoSource struct {
	address string
	info    InfoClient
}

// NewNodeInfoSource builds a NodeInfoSource that pairs the node's sandboxd
// advertise address (host:port, the claim-routing target the aggregated apiserver
// dials) with live warm-pool capacity read from info.
func NewNodeInfoSource(address string, info InfoClient) NodeInfoSource {
	return &sandboxdInfoSource{address: address, info: info}
}

func (s *sandboxdInfoSource) NodeInfo(ctx context.Context) (NodeInfo, error) {
	pools, err := s.info.Info(ctx)
	if err != nil {
		return NodeInfo{}, err
	}
	return NodeInfo{Address: s.address, Pools: pools}, nil
}
