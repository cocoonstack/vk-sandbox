package inventory

import (
	"context"
	"errors"
	"testing"

	extv1beta1 "github.com/cocoonstack/sandbox-operator/extensions/api/v1beta1"
	"github.com/cocoonstack/sandbox-operator/pkg/scale/sandboxd"
)

// TestNodeInfoSource confirms the source pairs the configured advertise address
// with the live warm-pool capacity read from sandboxd, flattening the nested pool
// key and keeping only the warm/target counts inventory needs.
func TestNodeInfoSource(t *testing.T) {
	info := &sandboxd.NodeInfo{Pools: []sandboxd.NodePool{
		{Key: sandboxd.PoolKey{Template: "base:24.04", Net: "none", Size: "small"}, Warm: 4, Target: 4, Golden: true},
		{Key: sandboxd.PoolKey{Template: "rt:24.04", Net: "egress", Size: "medium"}, Warm: 1, Refilling: 1, Target: 2},
	}, Claimed: 2}
	src := NewNodeInfoSource("172.16.26.2:7777", stubInfoClient{info: info})

	ni, err := src.NodeInfo(t.Context())
	if err != nil {
		t.Fatalf("NodeInfo: %v", err)
	}
	if ni.Address != "172.16.26.2:7777" {
		t.Fatalf("address wrong: %q", ni.Address)
	}
	want := []extv1beta1.PoolCapacity{
		{Template: "base:24.04", Net: "none", Size: "small", Warm: 4, Target: 4},
		{Template: "rt:24.04", Net: "egress", Size: "medium", Warm: 1, Target: 2},
	}
	if len(ni.Pools) != 2 || ni.Pools[0] != want[0] || ni.Pools[1] != want[1] {
		t.Fatalf("pools wrong: %+v, want %+v", ni.Pools, want)
	}
}

// TestNodeInfoSourceError propagates a sandboxd read failure rather than
// publishing a half-empty inventory.
func TestNodeInfoSourceError(t *testing.T) {
	src := NewNodeInfoSource("172.16.26.2:7777", stubInfoClient{err: errors.New("boom")})
	if _, err := src.NodeInfo(t.Context()); err == nil {
		t.Fatal("expected error to propagate")
	}
}

type stubInfoClient struct {
	info *sandboxd.NodeInfo
	err  error
}

func (s stubInfoClient) Info(context.Context) (*sandboxd.NodeInfo, error) {
	return s.info, s.err
}
