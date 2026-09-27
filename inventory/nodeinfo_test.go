package inventory

import (
	"context"
	"errors"
	"slices"
	"testing"

	extv1beta1 "github.com/cocoonstack/sandbox-operator/api/v1beta1"
	"github.com/cocoonstack/sandbox-operator/pkg/sandboxd"
)

func TestNodeInfoSource(t *testing.T) {
	info := &sandboxd.NodeInfo{Pools: []sandboxd.NodePool{
		{Key: sandboxd.PoolKey{Template: "base:24.04", Net: "none", Size: "small"}, Warm: 4, Target: 4, Golden: true},
		{Key: sandboxd.PoolKey{Template: "rt:24.04", Net: "egress", Size: "medium"}, Warm: 1, Refilling: 1, Target: 2},
	}, Templates: []sandboxd.NodeTemplate{
		{Key: sandboxd.PoolKey{Template: "tpl:a", Net: "none", Size: "medium"}, ContentDigest: "sha256:aa", Tenant: "acme", CPUCount: 2, MemTotalBytes: 1 << 30},
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
	if !slices.Equal(ni.Pools, want) {
		t.Fatalf("pools wrong: %+v, want %+v", ni.Pools, want)
	}
	if len(ni.Templates) != 1 || ni.Templates[0].Template != "tpl:a" || ni.Templates[0].ContentDigest != "sha256:aa" ||
		ni.Templates[0].Tenant != "acme" || ni.Templates[0].CPUCount != 2 || ni.Templates[0].MemoryBytes != 1<<30 {
		t.Fatalf("templates wrong: %+v", ni.Templates)
	}
}

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
