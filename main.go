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

// vk-cocoon-sandbox is a virtual-kubelet that serves Kubernetes agent-sandbox
// semantics (agents.x-k8s.io, driven by cocoon-sandbox-operator) from sandboxd,
// the node-local hot-sandbox daemon of github.com/cocoonstack/sandbox. One
// virtual node fronts one sandboxd: a sandbox Pod scheduled here becomes a
// sub-millisecond warm claim, pod deletion never destroys a VM without owner
// authorization, and the node publishes its O(nodes) inventory summary for the
// operator's aggregated apiserver.
package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	ctrlzap "sigs.k8s.io/controller-runtime/pkg/log/zap"

	"github.com/virtual-kubelet/virtual-kubelet/node"
	"github.com/virtual-kubelet/virtual-kubelet/node/nodeutil"

	"github.com/cocoonstack/cocoon-sandbox-operator/pkg/scale"
	"github.com/cocoonstack/cocoon-sandbox-operator/pkg/scale/sandboxd"

	"github.com/cocoonstack/vk-cocoon-sandbox/inventory"
	"github.com/cocoonstack/vk-cocoon-sandbox/provider"
	"github.com/cocoonstack/vk-cocoon-sandbox/sandboxdx"
)

// TaintKey marks the virtual node; the operator's runtime mutator adds the
// matching toleration to sandbox pods it routes here.
const TaintKey = "virtual-kubelet.io/provider"

func main() {
	var (
		nodeName         = flag.String("node-name", envOr("VK_NODE_NAME", "vk-sandboxd"), "virtual node name")
		sandboxdURL      = flag.String("sandboxd-url", envOr("SANDBOXD_URL", "http://127.0.0.1:7777"), "sandboxd base URL")
		tokenFile        = flag.String("sandboxd-token-file", os.Getenv("SANDBOXD_TOKEN_FILE"), "file holding the sandboxd node api token")
		statePath        = flag.String("state-path", envOr("VK_STATE_PATH", "/var/lib/vk-cocoon-sandbox/claims.json"), "claims table persistence path")
		orphanInterval   = flag.Duration("orphan-scan-interval", 60*time.Second, "audit-only orphan scan cadence (0 disables)")
		publishInventory = flag.Bool("publish-inventory", false, "server-side-apply this node's NodeInventory for the L3 aggregation layer")
		publishInterval  = flag.Duration("publish-interval", 30*time.Second, "NodeInventory publish cadence")
		podLabels        = flag.String("node-labels", "cocoon-sandbox.io/runtime=sandboxd", "comma-separated extra node labels key=value")
	)
	flag.Parse()

	logger := ctrlzap.New(ctrlzap.UseDevMode(false)).WithName("vk-cocoon-sandbox")
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	cfg, err := kubeConfig()
	if err != nil {
		logger.Error(err, "kubernetes client config")
		os.Exit(1)
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
		b, readErr := os.ReadFile(*tokenFile)
		if readErr != nil {
			logger.Error(readErr, "read sandboxd token file")
			os.Exit(1)
		}
		token = strings.TrimSpace(string(b))
	}

	sdClient := sandboxd.New(*sandboxdURL, token)
	lister := sandboxdx.NewListClient(*sandboxdURL, token, 0)

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

	newProvider := func(cfg nodeutil.ProviderConfig) (nodeutil.Provider, node.NodeProvider, error) {
		if cfg.Node != nil {
			if cfg.Node.Labels == nil {
				cfg.Node.Labels = map[string]string{}
			}
			cfg.Node.Labels["type"] = "virtual-kubelet"
			for k, v := range parseLabels(*podLabels) {
				cfg.Node.Labels[k] = v
			}
			cfg.Node.Spec.Taints = append(cfg.Node.Spec.Taints, corev1.Taint{
				Key:    TaintKey,
				Value:  provider.RuntimeSandboxd,
				Effect: corev1.TaintEffectNoSchedule,
			})
		}
		return p, nil, nil
	}

	n, err := nodeutil.NewNode(*nodeName, newProvider, nodeutil.WithClient(clientset))
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
		src := inventory.NewLiveSource(p, lister)
		pub := scale.NewNodeInventoryPublisher(*nodeName, src,
			scale.NewSSAInventoryApplier(cclient, "vk-cocoon-sandbox"), logger.WithName("inventory"))
		go pub.PublishPeriodically(ctx, *publishInterval)
	}

	logger.Info("starting virtual node", "node", *nodeName, "sandboxd", *sandboxdURL)
	if err := n.Run(ctx); err != nil && ctx.Err() == nil {
		logger.Error(err, "virtual-kubelet node exited")
		os.Exit(1)
	}
	logger.Info("vk-cocoon-sandbox exiting")
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func parseLabels(s string) map[string]string {
	out := map[string]string{}
	for _, kv := range strings.Split(s, ",") {
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
