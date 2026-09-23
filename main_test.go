package main

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/virtual-kubelet/virtual-kubelet/node"
	"github.com/virtual-kubelet/virtual-kubelet/node/nodeutil"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/util/workqueue"
)

func TestPodQueuesFollowTheClientBudget(t *testing.T) {
	o := &options{nodeName: "vk-test", listenAddr: "127.0.0.1:10260", kubeQPS: 200, kubeBurst: 400}
	opts, err := o.nodeOptions(fake.NewSimpleClientset())
	if err != nil {
		t.Fatalf("nodeOptions: %v", err)
	}
	var cfg nodeutil.NodeConfig
	for _, opt := range opts {
		if err := opt(&cfg); err != nil {
			t.Fatalf("node option: %v", err)
		}
	}
	var c node.PodControllerConfig
	for _, override := range cfg.PodControllerConfigOpts {
		if err := override(&c); err != nil {
			t.Fatalf("pod controller override: %v", err)
		}
	}
	for name, l := range map[string]workqueue.TypedRateLimiter[any]{
		"sync":   c.SyncPodsFromKubernetesRateLimiter,
		"delete": c.DeletePodsFromKubernetesRateLimiter,
		"status": c.SyncPodStatusFromProviderRateLimiter,
	} {
		if l == nil {
			t.Fatalf("%s queue keeps virtual-kubelet's default limiter", name)
		}
		for i := range 300 {
			if d := l.When(i); d > podRetryBaseDelay {
				t.Fatalf("%s queue delayed pod %d by %v inside the client's burst", name, i, d)
			}
		}
	}
}

func BenchmarkClientThrottle(b *testing.B) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"kind":"Pod","apiVersion":"v1","metadata":{"name":"p","namespace":"d"}}`))
	}))
	defer srv.Close()

	arms := []struct {
		name  string
		qps   float32
		burst int
	}{
		{"default-qps5", 0, 0},
		{"tuned-qps200", 200, 400},
	}
	for _, arm := range arms {
		b.Run(arm.name, func(b *testing.B) {
			cs, err := kubernetes.NewForConfig(&restclient.Config{Host: srv.URL, QPS: arm.qps, Burst: arm.burst})
			if err != nil {
				b.Fatalf("clientset: %v", err)
			}
			ctx := b.Context()
			for range 12 {
				if _, err := cs.CoreV1().Pods("d").Get(ctx, "p", metav1.GetOptions{}); err != nil {
					b.Fatalf("drain get: %v", err)
				}
			}
			for b.Loop() {
				if _, err := cs.CoreV1().Pods("d").Get(ctx, "p", metav1.GetOptions{}); err != nil {
					b.Fatalf("get: %v", err)
				}
			}
		})
	}
}
