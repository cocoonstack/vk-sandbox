// vk-sandbox is a virtual-kubelet that serves Kubernetes agent-sandbox
// semantics (agents.x-k8s.io, driven by sandbox-operator) from sandboxd,
// the node-local hot-sandbox daemon of github.com/cocoonstack/sandbox. One
// virtual node fronts one sandboxd: a sandbox Pod scheduled here becomes a
// sub-millisecond warm claim, pod deletion never destroys a VM without owner
// authorization, and the node publishes its O(nodes) inventory summary for the
// operator's aggregated apiserver.
package main

import (
	"cmp"
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"maps"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/go-logr/logr"
	"github.com/virtual-kubelet/virtual-kubelet/node"
	"github.com/virtual-kubelet/virtual-kubelet/node/nodeutil"
	"golang.org/x/time/rate"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/workqueue"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	ctrlzap "sigs.k8s.io/controller-runtime/pkg/log/zap"

	"github.com/cocoonstack/sandbox-operator/pkg/sandboxd"
	"github.com/cocoonstack/sandbox-operator/pkg/scale"
	"github.com/cocoonstack/vk-sandbox/inventory"
	"github.com/cocoonstack/vk-sandbox/provider"
	"github.com/cocoonstack/vk-sandbox/version"
)

const (
	// Sized to the per-node pod-create fan-out: every claim targets the one
	// node-local sandboxd, so idle connections are pooled against a single host.
	sandboxdRequestTimeout  = 10 * time.Second
	sandboxdMaxIdleConns    = 64
	sandboxdIdleConnTimeout = 90 * time.Second

	// claimVerifyInterval stops once everything is vouched for.
	claimVerifyInterval = 15 * time.Second

	// leaseWatchInterval bounds how stale a reaped sandbox's Running status can
	// stay; leases run for hours, so half a minute of slop is immaterial.
	leaseWatchInterval = 30 * time.Second

	// Per-pod retry backoff of the pod queues, workqueue's own defaults.
	podRetryBaseDelay = 5 * time.Millisecond
	podRetryMaxDelay  = 1000 * time.Second

	// TaintKey marks the virtual node; the operator's runtime mutator adds the
	// matching toleration to sandbox pods it routes here.
	TaintKey = "virtual-kubelet.io/provider"
)

func main() {
	var o options
	flag.StringVar(&o.nodeName, "node-name", envOr("VK_NODE_NAME", "vk-sandboxd"), "virtual node name (must differ from the physical node and any co-located vk-cocoon node)")
	flag.StringVar(&o.nodeIP, "node-ip", envOr("VK_NODE_IP", ""), "node InternalIP advertised to the apiserver")
	flag.StringVar(&o.listenAddr, "listen-addr", envOr("VK_LISTEN_ADDR", ":10260"), "kubelet API listen address (must differ from a co-located vk-cocoon, which uses :10250)")
	flag.StringVar(&o.tlsCert, "tls-cert", os.Getenv("VK_TLS_CERT"), "kubelet API TLS certificate (optional; a self-signed in-memory cert is used when unset)")
	flag.StringVar(&o.tlsKey, "tls-key", os.Getenv("VK_TLS_KEY"), "kubelet API TLS key")
	flag.StringVar(&o.nodeCPU, "node-cpu", envOr("VK_NODE_CPU", "4000"), "advertised node CPU capacity (a scheduling budget; the real resource is sandboxd's)")
	flag.StringVar(&o.nodeMem, "node-memory", envOr("VK_NODE_MEMORY", "8Ti"), "advertised node memory capacity")
	flag.StringVar(&o.nodePods, "node-pods", envOr("VK_NODE_PODS", "2000"), "advertised node max pods")
	flag.Float64Var(&o.kubeQPS, "kube-api-qps", 200, "client-go QPS for the kubernetes clients (status pushes, delete authorization, inventory publish), and the rate of the pod create, delete and status queues")
	flag.IntVar(&o.kubeBurst, "kube-api-burst", 400, "client-go burst on top of --kube-api-qps, and the burst of the pod queues")
	flag.StringVar(&o.sandboxdURL, "sandboxd-url", envOr("SANDBOXD_URL", "http://127.0.0.1:7777"), "sandboxd base URL")
	flag.StringVar(&o.sandboxdAddr, "sandboxd-advertise-addr", envOr("SANDBOXD_ADVERTISE_ADDR", ""), "sandboxd advertise address (host:port) published in NodeInventory for claim routing; defaults to the host:port of --sandboxd-url")
	flag.StringVar(&o.tokenFile, "sandboxd-token-file", os.Getenv("SANDBOXD_TOKEN_FILE"), "file holding the sandboxd node api token")
	flag.StringVar(&o.statePath, "state-path", envOr("VK_STATE_PATH", "/var/lib/vk-sandbox/claims.json"), "claims table persistence path")
	flag.DurationVar(&o.orphanInterval, "orphan-scan-interval", 60*time.Second, "audit-only orphan scan cadence (0 disables)")
	flag.BoolVar(&o.publishInventory, "publish-inventory", false, "server-side-apply this node's NodeInventory for the L3 aggregation layer")
	flag.DurationVar(&o.publishInterval, "publish-interval", 30*time.Second, "NodeInventory publish cadence")
	flag.StringVar(&o.podLabels, "node-labels", "sandbox.cocoonstack.io/runtime=sandboxd", "comma-separated extra node labels key=value")
	showVersion := flag.Bool("version", false, "print build version and exit")
	flag.Parse()

	if *showVersion {
		fmt.Printf("vk-sandbox %s (rev=%s built=%s)\n", version.VERSION, version.REVISION, version.BUILTAT)
		return
	}

	o.log = ctrlzap.New(ctrlzap.UseDevMode(false)).WithName("vk-sandbox")
	if err := o.run(); err != nil {
		o.log.Error(err, "vk-sandbox exited")
		os.Exit(1)
	}
	o.log.Info("vk-sandbox exiting")
}

type options struct {
	nodeName     string
	nodeIP       string
	listenAddr   string
	tlsCert      string
	tlsKey       string
	nodeCPU      string
	nodeMem      string
	nodePods     string
	sandboxdURL  string
	sandboxdAddr string
	tokenFile    string
	statePath    string
	podLabels    string

	orphanInterval   time.Duration
	publishInterval  time.Duration
	publishInventory bool
	kubeQPS          float64
	kubeBurst        int

	log logr.Logger
}

func (o *options) run() error {
	if o.statePath == "" {
		return fmt.Errorf("--state-path is required: without it no release credential survives a restart")
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	cfg, err := kubeConfig()
	if err != nil {
		return fmt.Errorf("kubernetes client config: %w", err)
	}
	// client-go defaults to QPS=5/Burst=10, which queues ~400s of client-side
	// throttling when the advertised 2000 pods push status at once (#1).
	cfg.QPS = float32(o.kubeQPS)
	cfg.Burst = o.kubeBurst
	clientset, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return fmt.Errorf("kubernetes clientset: %w", err)
	}
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return fmt.Errorf("dynamic client: %w", err)
	}

	token, err := o.sandboxdToken()
	if err != nil {
		return err
	}
	// One pooled client for the claim path. A bare &http.Client{} falls back to
	// http.DefaultTransport, whose MaxIdleConnsPerHost of 2 forces a fresh
	// handshake on every concurrent pod create past the second, and has no
	// timeout, so a wedged sandboxd would hold the create goroutine forever.
	hc := &http.Client{
		Timeout: sandboxdRequestTimeout,
		Transport: &http.Transport{
			MaxIdleConns:        sandboxdMaxIdleConns,
			MaxIdleConnsPerHost: sandboxdMaxIdleConns,
			IdleConnTimeout:     sandboxdIdleConnTimeout,
		},
	}
	sdClient := sandboxd.New(o.sandboxdURL, token, sandboxd.WithHTTPClient(hc))

	p, err := provider.New(ctx, provider.Config{
		Client:    sdClient,
		Lister:    sdClient,
		Dynamic:   dyn,
		StatePath: o.statePath,
		Logger:    o.log.WithName("provider"),
	})
	if err != nil {
		return fmt.Errorf("build provider: %w", err)
	}

	opts, err := o.nodeOptions(clientset)
	if err != nil {
		return err
	}
	n, err := nodeutil.NewNode(o.nodeName, o.providerFactory(p), opts...)
	if err != nil {
		return fmt.Errorf("create virtual-kubelet node: %w", err)
	}

	if o.orphanInterval > 0 {
		go p.RunOrphanScan(ctx, o.orphanInterval)
	}
	// Independent of the audit scan: a startup that could not reach sandboxd
	// leaves claims unusable until a listing vouches for them.
	go p.RunClaimVerification(ctx, claimVerifyInterval)
	// virtual-kubelet never polls an asynchronous provider, so lease expiry
	// must be pushed or a reaped sandbox stays Running forever.
	go p.RunLeaseWatch(ctx, leaseWatchInterval)
	if o.publishInventory {
		if err := o.startInventoryPublisher(ctx, cfg, p, sdClient); err != nil {
			return err
		}
	}

	o.log.Info("starting virtual node", "node", o.nodeName, "sandboxd", o.sandboxdURL)
	if err := n.Run(ctx); err != nil && ctx.Err() == nil {
		return fmt.Errorf("virtual-kubelet node exited: %w", err)
	}
	return nil
}

func (o *options) sandboxdToken() (string, error) {
	if o.tokenFile == "" {
		return "", nil
	}
	b, err := os.ReadFile(o.tokenFile) //nolint:gosec // operator-supplied path
	if err != nil {
		return "", fmt.Errorf("read sandboxd token file: %w", err)
	}
	return strings.TrimSpace(string(b)), nil
}

func (o *options) providerFactory(p *provider.Provider) nodeutil.NewProviderFunc {
	// Advertised capacity is a scheduling budget only — the pod is a placeholder
	// and sandboxd holds the real microVM. Without it the scheduler sees 0
	// allocatable and rejects every sandbox pod.
	capacity := corev1.ResourceList{
		corev1.ResourceCPU:    resource.MustParse(o.nodeCPU),
		corev1.ResourceMemory: resource.MustParse(o.nodeMem),
		corev1.ResourcePods:   resource.MustParse(o.nodePods),
	}
	kubeletPort, _ := listenPort(o.listenAddr)
	return func(cfg nodeutil.ProviderConfig) (nodeutil.Provider, node.NodeProvider, error) {
		if cfg.Node.Labels == nil {
			cfg.Node.Labels = map[string]string{}
		}
		cfg.Node.Labels["type"] = "virtual-kubelet"
		maps.Copy(cfg.Node.Labels, parseLabels(o.podLabels))
		cfg.Node.Spec.Taints = append(cfg.Node.Spec.Taints, corev1.Taint{
			Key:    TaintKey,
			Value:  provider.RuntimeSandboxd,
			Effect: corev1.TaintEffectNoSchedule,
		})
		addrs := []corev1.NodeAddress{{Type: corev1.NodeHostName, Address: o.nodeName}}
		if o.nodeIP != "" {
			addrs = append([]corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: o.nodeIP}}, addrs...)
		}
		cfg.Node.Status.Addresses = addrs
		cfg.Node.Status.DaemonEndpoints.KubeletEndpoint.Port = kubeletPort
		cfg.Node.Status.Capacity = capacity
		cfg.Node.Status.Allocatable = capacity
		return p, nil, nil
	}
}

func (o *options) nodeOptions(clientset kubernetes.Interface) ([]nodeutil.NodeOpt, error) {
	if _, err := listenPort(o.listenAddr); err != nil {
		return nil, fmt.Errorf("parse --listen-addr: %w", err)
	}
	kubeletMux := http.NewServeMux()
	opts := []nodeutil.NodeOpt{
		nodeutil.WithClient(clientset),
		nodeutil.AttachProviderRoutes(kubeletMux),
		nodeutil.WithPodControllerConfigOverrides(o.podQueueLimits),
		func(c *nodeutil.NodeConfig) error {
			c.HTTPListenAddr = o.listenAddr
			c.Handler = kubeletMux
			return nil
		},
	}

	// virtual-kubelet only serves the kubelet API over TLS. Reuse the node's
	// kubelet cert when present, else self-sign one so every node's API surface
	// is uniform regardless of what the co-located vk-cocoon carries.
	var cert tls.Certificate
	var err error
	if o.tlsCert != "" && o.tlsKey != "" && fileReadable(o.tlsCert) && fileReadable(o.tlsKey) {
		if cert, err = tls.LoadX509KeyPair(o.tlsCert, o.tlsKey); err != nil {
			return nil, fmt.Errorf("load kubelet TLS cert: %w", err)
		}
	} else {
		o.log.Info("kubelet cert absent; self-signing", "node", o.nodeName)
		if cert, err = selfSignedCert(o.nodeName, o.nodeIP, "127.0.0.1"); err != nil {
			return nil, fmt.Errorf("self-sign kubelet cert: %w", err)
		}
	}
	return append(opts, func(c *nodeutil.NodeConfig) error {
		c.TLSConfig = &tls.Config{Certificates: []tls.Certificate{cert}, ClientAuth: tls.NoClientCert, MinVersion: tls.VersionTLS12}
		return nil
	}), nil
}

// podQueueLimits holds the pod queues to the kube client's own budget; the
// library default caps a node at 10 pod creates or status pushes a second.
func (o *options) podQueueLimits(c *node.PodControllerConfig) error {
	limiter := func() workqueue.TypedRateLimiter[any] {
		return workqueue.NewTypedMaxOfRateLimiter(
			workqueue.NewTypedItemExponentialFailureRateLimiter[any](podRetryBaseDelay, podRetryMaxDelay),
			&workqueue.TypedBucketRateLimiter[any]{Limiter: rate.NewLimiter(rate.Limit(o.kubeQPS), o.kubeBurst)},
		)
	}
	c.SyncPodsFromKubernetesRateLimiter = limiter()
	c.DeletePodsFromKubernetesRateLimiter = limiter()
	c.SyncPodStatusFromProviderRateLimiter = limiter()
	return nil
}

func (o *options) startInventoryPublisher(ctx context.Context, cfg *rest.Config, p *provider.Provider, sd *sandboxd.Client) error {
	cclient, err := ctrlclient.New(cfg, ctrlclient.Options{})
	if err != nil {
		return fmt.Errorf("controller-runtime client for inventory publish: %w", err)
	}
	advertiseAddr := o.sandboxdAddr
	if advertiseAddr == "" {
		advertiseAddr = hostPort(o.sandboxdURL)
	}
	pub := inventory.NewPublisher(o.nodeName,
		inventory.NewLiveSource(p, sd),
		inventory.NewNodeInfoSource(advertiseAddr, sd),
		scale.NewSSAInventoryApplier(cclient, "vk-sandbox"),
		o.log.WithName("inventory"))
	go pub.PublishPeriodically(ctx, o.publishInterval)
	return nil
}

func envOr(key, def string) string { return cmp.Or(os.Getenv(key), def) }

func parseLabels(s string) map[string]string {
	out := map[string]string{}
	for kv := range strings.SplitSeq(s, ",") {
		if k, v, ok := strings.Cut(strings.TrimSpace(kv), "="); ok && k != "" {
			out[k] = v
		}
	}
	return out
}

func kubeConfig() (*rest.Config, error) {
	if cfg, err := rest.InClusterConfig(); err == nil {
		return cfg, nil
	}
	return clientcmd.BuildConfigFromFlags("", os.Getenv("KUBECONFIG"))
}

func fileReadable(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.Mode().IsRegular()
}

func hostPort(raw string) string {
	if u, err := url.Parse(raw); err == nil && u.Host != "" {
		return u.Host
	}
	return strings.TrimRight(raw, "/")
}

func listenPort(addr string) (int32, error) {
	_, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return 0, err
	}
	port, err := strconv.ParseInt(portStr, 10, 32)
	if err != nil {
		return 0, err
	}
	if port < 1 || port > 65535 {
		return 0, fmt.Errorf("port %d out of range", port)
	}
	return int32(port), nil
}
