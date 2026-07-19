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
// claims table + the sandboxd operator index, so the operator's
// NodeInventoryPublisher can server-side-apply the one O(nodes) NodeInventory
// object this node contributes. The per-sandbox truth stays on the node; etcd
// stores only the summary — the metrics.k8s.io pattern from the operator's
// scaling design.
package inventory

import (
	"context"
	"sort"

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

// LiveSandboxes summarizes the node's sandboxes as inventory entries. A
// sandbox bound to a pod carries that pod as its name and claim reference; an
// unbound (pool-warm or orphan-candidate) sandbox is deliberately omitted —
// inventory serves the aggregated sandboxes view, which is pod-scoped.
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
		phase := "Running"
		switch {
		case !alive:
			phase = "Gone"
		case row.Hibernated:
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
