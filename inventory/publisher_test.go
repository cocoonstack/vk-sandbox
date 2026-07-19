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

	"github.com/cocoonstack/vk-cocoon-sandbox/provider"
)

type staticClaims map[string]provider.Claim

func (s staticClaims) SnapshotClaims() map[string]provider.Claim { return s }

type staticLister []provider.ListedSandbox

func (s staticLister) ListSandboxes(context.Context) ([]provider.ListedSandbox, error) {
	return s, nil
}

// TestLiveSandboxes maps pod-bound claims into inventory entries with the
// node-observed phase; unbound (pool-warm) sandboxes are omitted.
func TestLiveSandboxes(t *testing.T) {
	claims := staticClaims{
		"ns1/pod-a": {ID: "sb_a", Address: "10.0.0.5:7777"},
		"ns1/pod-b": {ID: "sb_b"},
		"ns1/pod-c": {ID: "sb_gone"},
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
	if len(got) != 3 {
		t.Fatalf("want 3 entries (bound only), got %d: %+v", len(got), got)
	}
	byName := map[string]string{}
	for _, e := range got {
		byName[e.Name] = e.Phase
		if e.ClaimRef != e.Name {
			t.Errorf("entry %s: claimRef %q != name", e.Name, e.ClaimRef)
		}
	}
	if byName["ns1/pod-a"] != "Running" || byName["ns1/pod-b"] != "Hibernated" || byName["ns1/pod-c"] != "Gone" {
		t.Fatalf("phase mapping wrong: %v", byName)
	}
}
