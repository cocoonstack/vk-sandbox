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
	"errors"
	"testing"

	extv1beta1 "github.com/cocoonstack/cocoon-sandbox-operator/extensions/api/v1beta1"
)

type stubInfoClient struct {
	pools []extv1beta1.PoolCapacity
	err   error
}

func (s stubInfoClient) Info(context.Context) ([]extv1beta1.PoolCapacity, error) {
	return s.pools, s.err
}

// TestNodeInfoSource confirms the source pairs the configured advertise address
// with the live warm-pool capacity read from sandboxd.
func TestNodeInfoSource(t *testing.T) {
	pools := []extv1beta1.PoolCapacity{{Template: "base:24.04", Net: "none", Size: "small", Warm: 2, Target: 4}}
	src := NewNodeInfoSource("172.16.26.2:7777", stubInfoClient{pools: pools})

	ni, err := src.NodeInfo(context.Background())
	if err != nil {
		t.Fatalf("NodeInfo: %v", err)
	}
	if ni.Address != "172.16.26.2:7777" {
		t.Fatalf("address wrong: %q", ni.Address)
	}
	if len(ni.Pools) != 1 || ni.Pools[0].Warm != 2 || ni.Pools[0].Target != 4 {
		t.Fatalf("pools wrong: %+v", ni.Pools)
	}
}

// TestNodeInfoSourceError propagates a sandboxd read failure rather than
// publishing a half-empty inventory.
func TestNodeInfoSourceError(t *testing.T) {
	src := NewNodeInfoSource("172.16.26.2:7777", stubInfoClient{err: errors.New("boom")})
	if _, err := src.NodeInfo(context.Background()); err == nil {
		t.Fatal("expected error to propagate")
	}
}
