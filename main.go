// vk-sandbox is a virtual-kubelet that serves Kubernetes agent-sandbox
// semantics (agents.x-k8s.io, driven by sandbox-operator) from sandboxd,
// the node-local hot-sandbox daemon of github.com/cocoonstack/sandbox. One
// virtual node fronts one sandboxd: a sandbox Pod scheduled here becomes a
// sub-millisecond warm claim, pod deletion never destroys a VM without owner
// authorization, and the node publishes its O(nodes) inventory summary for the
// operator's aggregated apiserver.
package main

import (
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

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	ctrlzap "sigs.k8s.io/controller-runtime/pkg/log/zap"

	"github.com/virtual-kubelet/virtual-kubelet/node"
	"github.com/virtual-kubelet/virtual-kubelet/node/nodeutil"

	"github.com/cocoonstack/sandbox-operator/pkg/scale"
	"github.com/cocoonstack/sandbox-operator/pkg/scale/sandboxd"

	"github.com/cocoonstack/vk-sandbox/inventory"
	"github.com/cocoonstack/vk-sandbox/provider"
	"github.com/cocoonstack/vk-sandbox/sandboxdx"
	"github.com/cocoonstack/vk-sandbox/version"
)

const (
	// Sized to the per-node pod-create fan-out: every claim targets the one
	// node-local sandboxd, so idle connections are pooled against a single host.
	sandboxdRequestTimeout  = 10 * time.Second
	sandboxdMaxIdleConns    = 64
	sandboxdIdleConnTimeout = 90 * time.Second

	// TaintKey marks the virtual node; the operator's runtime mutator adds the
	// matching toleration to sandbox pods it routes here.
	TaintKey = "virtual-kubelet.io/provider"
)

func main() {
	var (
		nodeName         = flag.String("node-name", envOr("VK_NODE_NAME", "vk-sandboxd"), "virtual node name (must be distinct from a co-located vk-cocoon node)")
		nodeIP           = flag.String("node-ip", envOr("VK_NODE_IP", ""), "node InternalIP advertised to the apiserver")
		listenAddr       = flag.String("listen-addr", envOr("VK_LISTEN_ADDR", ":10260"), "kubelet API listen address (must differ from a co-located vk-cocoon, which uses :10250)")
		tlsCert          = flag.String("tls-cert", os.Getenv("VK_TLS_CERT"), "kubelet API TLS certificate (optional; plain HTTP if unset)")
		tlsKey           = flag.String("tls-key", os.Getenv("VK_TLS_KEY"), "kubelet API TLS key")
		nodeCPU          = flag.String("node-cpu", envOr("VK_NODE_CPU", "4000"), "advertised node CPU capacity (a scheduling budget; the real resource is sandboxd's)")
		nodeMem          = flag.String("node-memory", envOr("VK_NODE_MEMORY", "8Ti"), "advertised node memory capacity")
		nodePods         = flag.String("node-pods", envOr("VK_NODE_PODS", "2000"), "advertised node max pods")
		sandboxdURL      = flag.String("sandboxd-url", envOr("SANDBOXD_URL", "http://127.0.0.1:7777"), "sandboxd base URL")
		sandboxdAddr     = flag.String("sandboxd-advertise-addr", envOr("SANDBOXD_ADVERTISE_ADDR", ""), "sandboxd advertise address (host:port) published in NodeInventory for claim routing; defaults to the host:port of --sandboxd-url")
		tokenFile        = flag.String("sandboxd-token-file", os.Getenv("SANDBOXD_TOKEN_FILE"), "file holding the sandboxd node api token")
		statePath        = flag.String("state-path", envOr("VK_STATE_PATH", "/var/lib/vk-sandbox/claims.json"), "claims table persistence path")
		orphanInterval   = flag.Duration("orphan-scan-interval", 60*time.Second, "audit-only orphan scan cadence (0 disables)")
		publishInventory = flag.Bool("publish-inventory", false, "server-side-apply this node's NodeInventory for the L3 aggregation layer")
		publishInterval  = flag.Duration("publish-interval", 30*time.Second, "NodeInventory publish cadence")
		podLabels        = flag.String("node-labels", "cocoon-sandbox.io/runtime=sandboxd", "comma-separated extra node labels key=value")
		showVersion      = flag.Bool("version", false, "print build version and exit")
	)
	flag.Parse()

	if *showVersion {
		fmt.Printf("vk-sandbox %s (rev=%s built=%s)\n", version.VERSION, version.REVISION, version.BUILTAT)
		return
	}

	logger := ctrlzap.New(ctrlzap.UseDevMode(false)).WithName("vk-sandbox")
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	cfg, err := kubeConfig()
	if err != nil {
		logger.Error(err, "kubernetes client config")
		os.Exit(1) //nolint:gocritic // startup failure: the process dies before ctx matters
	}
	clientset, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		logger.Error(err, "kubernetes clientset")
		os.Exit(1)
	}
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		logger.Error(err, "dynamic client")
		os.Exit(1)
	}

	token := ""
	if *tokenFile != "" {
		b, readErr := os.ReadFile(*tokenFile) //nolint:gosec // operator-supplied path
		if readErr != nil {
			logger.Error(readErr, "read sandboxd token file")
			os.Exit(1)
		}
		token = strings.TrimSpace(string(b))
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
	sdClient := sandboxd.New(*sandboxdURL, token, sandboxd.WithHTTPClient(hc))
	lister := sandboxdx.NewListClient(*sandboxdURL, token, sandboxdRequestTimeout)

	p, err := provider.New(provider.Config{
		NodeName:  *nodeName,
		Client:    sdClient,
		Lister:    lister,
		Dynamic:   dyn,
		StatePath: *statePath,
		Logger:    logger.WithName("provider"),
	})
	if err != nil {
		logger.Error(err, "build provider")
		os.Exit(1)
	}

	kubeletPort, err := listenPort(*listenAddr)
	if err != nil {
		logger.Error(err, "parse --listen-addr")
		os.Exit(1)
	}

	// Advertised capacity is a scheduling budget only — the pod is a placeholder
	// and sandboxd holds the real microVM. Without it the scheduler sees 0
	// allocatable and rejects every sandbox pod.
	capacity := corev1.ResourceList{
		corev1.ResourceCPU:    resource.MustParse(*nodeCPU),
		corev1.ResourceMemory: resource.MustParse(*nodeMem),
		corev1.ResourcePods:   resource.MustParse(*nodePods),
	}

	newProvider := func(cfg nodeutil.ProviderConfig) (nodeutil.Provider, node.NodeProvider, error) {
		if cfg.Node != nil {
			if cfg.Node.Labels == nil {
				cfg.Node.Labels = map[string]string{}
			}
			cfg.Node.Labels["type"] = "virtual-kubelet"
			maps.Copy(cfg.Node.Labels, parseLabels(*podLabels))
			cfg.Node.Spec.Taints = append(cfg.Node.Spec.Taints, corev1.Taint{
				Key:    TaintKey,
				Value:  provider.RuntimeSandboxd,
				Effect: corev1.TaintEffectNoSchedule,
			})
			addrs := []corev1.NodeAddress{{Type: corev1.NodeHostName, Address: *nodeName}}
			if *nodeIP != "" {
				addrs = append([]corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: *nodeIP}}, addrs...)
			}
			cfg.Node.Status.Addresses = addrs
			cfg.Node.Status.DaemonEndpoints.KubeletEndpoint.Port = kubeletPort
			cfg.Node.Status.Capacity = capacity
			cfg.Node.Status.Allocatable = capacity
		}
		return p, nil, nil
	}

	kubeletMux := http.NewServeMux()
	opts := []nodeutil.NodeOpt{
		nodeutil.WithClient(clientset),
		nodeutil.AttachProviderRoutes(kubeletMux),
		func(c *nodeutil.NodeConfig) error {
			c.HTTPListenAddr = *listenAddr
			c.Handler = kubeletMux
			return nil
		},
	}
	// virtual-kubelet only serves the kubelet API over TLS. Reuse the node's
	// kubelet cert when present, else self-sign one so every node's API surface is
	// uniform regardless of what the co-located vk-cocoon carries.
	var cert tls.Certificate
	if *tlsCert != "" && *tlsKey != "" && fileReadable(*tlsCert) && fileReadable(*tlsKey) {
		cert, err = tls.LoadX509KeyPair(*tlsCert, *tlsKey)
		if err != nil {
			logger.Error(err, "load kubelet TLS cert")
			os.Exit(1)
		}
	} else {
		logger.Info("kubelet cert absent; self-signing", "node", *nodeName)
		cert, err = selfSignedCert(*nodeName, *nodeIP, "127.0.0.1")
		if err != nil {
			logger.Error(err, "self-sign kubelet cert")
			os.Exit(1)
		}
	}
	opts = append(opts, func(c *nodeutil.NodeConfig) error {
		c.TLSConfig = &tls.Config{Certificates: []tls.Certificate{cert}, ClientAuth: tls.NoClientCert, MinVersion: tls.VersionTLS12}
		return nil
	})

	n, err := nodeutil.NewNode(*nodeName, newProvider, opts...)
	if err != nil {
		logger.Error(err, "create virtual-kubelet node")
		os.Exit(1)
	}

	if *orphanInterval > 0 {
		go p.RunOrphanScan(ctx, *orphanInterval)
	}

	if *publishInventory {
		cclient, cerr := ctrlclient.New(cfg, ctrlclient.Options{})
		if cerr != nil {
			logger.Error(cerr, "controller-runtime client for inventory publish")
			os.Exit(1)
		}
		advertiseAddr := *sandboxdAddr
		if advertiseAddr == "" {
			advertiseAddr = hostPort(*sandboxdURL)
		}
		src := inventory.NewLiveSource(p, lister)
		infoSrc := inventory.NewNodeInfoSource(advertiseAddr, lister)
		pub := inventory.NewPublisher(*nodeName, src, infoSrc,
			scale.NewSSAInventoryApplier(cclient, "vk-sandbox"), logger.WithName("inventory"))
		go pub.PublishPeriodically(ctx, *publishInterval)
	}

	logger.Info("starting virtual node", "node", *nodeName, "sandboxd", *sandboxdURL)
	if err := n.Run(ctx); err != nil && ctx.Err() == nil {
		logger.Error(err, "virtual-kubelet node exited")
		os.Exit(1)
	}
	logger.Info("vk-sandbox exiting")
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func parseLabels(s string) map[string]string {
	out := map[string]string{}
	for kv := range strings.SplitSeq(s, ",") {
		if k, v, ok := strings.Cut(strings.TrimSpace(kv), "="); ok && k != "" {
			out[k] = v
		}
	}
	return out
}

// kubeConfig prefers in-cluster, falling back to KUBECONFIG.
func kubeConfig() (*rest.Config, error) {
	if cfg, err := rest.InClusterConfig(); err == nil {
		return cfg, nil
	}
	return clientcmd.BuildConfigFromFlags("", os.Getenv("KUBECONFIG"))
}

// fileReadable reports whether path exists and is a regular readable file.
func fileReadable(path string) bool {
	info, err := os.Stat(path) //nolint:gosec // operator-supplied path
	return err == nil && info.Mode().IsRegular()
}

// hostPort returns the "host:port" of a sandboxd base URL with the scheme
// stripped, the form NodeInventory publishes as the node's claim-routing advertise
// address. A bare "host:port" (no scheme) is returned unchanged.
func hostPort(raw string) string {
	if u, err := url.Parse(raw); err == nil && u.Host != "" {
		return u.Host
	}
	return strings.TrimRight(raw, "/")
}

// listenPort extracts the numeric port from a listen address (":10260").
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
