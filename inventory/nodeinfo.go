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

	extv1beta1 "github.com/cocoonstack/cocoon-sandbox-operator/extensions/api/v1beta1"
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

// sandboxdInfoSource implements NodeInfoSource: a fixed advertise address paired
// with live warm-pool capacity from sandboxd.
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

// NodeInfo reads the node's warm-pool capacity and returns it with the node's
// advertise address.
func (s *sandboxdInfoSource) NodeInfo(ctx context.Context) (NodeInfo, error) {
	pools, err := s.info.Info(ctx)
	if err != nil {
		return NodeInfo{}, err
	}
	return NodeInfo{Address: s.address, Pools: pools}, nil
}
