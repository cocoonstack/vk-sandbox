package main

import (
	"net/http"
	"net/http/httptest"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
)

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
