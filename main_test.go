package main

import (
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"
	"testing/synctest"
	"time"

	"github.com/virtual-kubelet/virtual-kubelet/node"
	"github.com/virtual-kubelet/virtual-kubelet/node/nodeutil"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
)

func TestPodQueuesFollowTheClientBudget(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		for _, tc := range []struct {
			name      string
			qps       float64
			burst     int
			undelayed int
			pastBurst time.Duration
		}{
			{"configured", 100, 400, 400, 10 * time.Millisecond},
			{"client-go defaults", 0, 0, restclient.DefaultBurst, time.Second / time.Duration(restclient.DefaultQPS)},
			{"unlimited", -1, 0, 1000, podRetryBaseDelay},
		} {
			for queue, l := range podQueues(t, tc.qps, tc.burst) {
				for i := range tc.undelayed {
					if d := l.When(i); d > podRetryBaseDelay {
						t.Fatalf("%s: %s queue delayed pod %d by %v inside the client's burst", tc.name, queue, i, d)
					}
				}
				if d := l.When(tc.undelayed); d != tc.pastBurst {
					t.Fatalf("%s: %s queue delayed pod %d past the client's burst by %v, want %v", tc.name, queue, tc.undelayed, d, tc.pastBurst)
				}
			}
		}
	})
}

func TestPodQueuesBackOffAFailingPod(t *testing.T) {
	for _, qps := range []float64{100, -1} {
		for queue, l := range podQueues(t, qps, 400) {
			l.When("pod")
			if d := l.When("pod"); d != 2*podRetryBaseDelay {
				t.Fatalf("qps %v: %s queue retried a failing pod after %v, want %v", qps, queue, d, 2*podRetryBaseDelay)
			}
		}
	}
}

func TestWarningEventsDropNormalEvents(t *testing.T) {
	sink := record.NewFakeRecorder(8)
	w := warningEvents{sink}
	pod := &corev1.Pod{Name: "p"}
	for _, eventtype := range []string{corev1.EventTypeNormal, corev1.EventTypeWarning} {
		w.Event(pod, eventtype, "R", "m")
		w.Eventf(pod, eventtype, "R", "m %d", 1)
		w.AnnotatedEventf(pod, nil, eventtype, "R", "m %d", 2)
	}
	close(sink.Events)
	var got []string
	for e := range sink.Events {
		got = append(got, e)
	}
	if want := []string{"Warning R m", "Warning R m 1", "Warning R m 2"}; !slices.Equal(got, want) {
		t.Fatalf("recorded %q, want only the warnings %q", got, want)
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

func podQueues(t *testing.T, qps float64, burst int) map[string]workqueue.TypedRateLimiter[any] {
	t.Helper()
	o := &options{nodeName: "vk-test", listenAddr: "127.0.0.1:10260", kubeQPS: qps, kubeBurst: burst}
	opts, err := o.nodeOptions(t.Context(), fake.NewClientset())
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
	queues := map[string]workqueue.TypedRateLimiter[any]{
		"sync":   c.SyncPodsFromKubernetesRateLimiter,
		"delete": c.DeletePodsFromKubernetesRateLimiter,
		"status": c.SyncPodStatusFromProviderRateLimiter,
	}
	for name, l := range queues {
		if l == nil {
			t.Fatalf("%s queue keeps virtual-kubelet's default limiter", name)
		}
	}
	return queues
}
