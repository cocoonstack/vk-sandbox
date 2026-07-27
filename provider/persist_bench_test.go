package provider

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/go-logr/logr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// BenchmarkCommitClaim measures making one claim durable with a full
// 2000-claim table — the advertised node pod capacity. Every commit
// serializes the whole table under saveMu (#2), so ns/op is the per-create
// persistence floor and its inverse the node's claim-commit throughput ceiling.
func BenchmarkCommitClaim(b *testing.B) {
	p, err := New(b.Context(), Config{
		NodeName:  "bench",
		StatePath: filepath.Join(b.TempDir(), "claims.json"),
		Logger:    logr.Discard(),
	})
	if err != nil {
		b.Fatalf("new provider: %v", err)
	}
	for i := range 2000 {
		p.claims[fmt.Sprintf("ns/pod-%d", i)] = Claim{
			ID:        fmt.Sprintf("sb_%032d", i),
			Token:     "0123456789abcdef0123456789abcdef",
			Address:   "10.0.0.1:7777",
			PodUID:    "b2f0c5c4-9d1e-4a67-9f4e-000000000000",
			ClaimedAt: metav1.Now(),
			Deadline:  metav1.Now(),
		}
	}
	const key = "ns/pod-0"
	b.ReportAllocs()
	for b.Loop() {
		p.mu.Lock()
		p.tentative[key] = struct{}{}
		p.mu.Unlock()
		if err := p.commitClaim(key); err != nil {
			b.Fatalf("commit: %v", err)
		}
	}
}
