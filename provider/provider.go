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

// Package provider implements the virtual-kubelet provider that bridges
// Kubernetes agent-sandbox semantics (agents.x-k8s.io, driven by
// cocoon-sandbox-operator) onto sandboxd — the node-local hot-sandbox daemon
// from github.com/cocoonstack/sandbox that hands over an already-running
// microVM in 0.2–0.7 ms.
//
// The division of labor in the million-scale design (the operator README's
// "Scaling design" chapter): the operator owns the record plane (CRDs, warm
// pools, claims, admission), this provider owns the node transaction plane —
// a sandbox Pod scheduled to the virtual node becomes one sandboxd claim, and
// the delete-authorization contract guarantees a Pod deletion alone never
// destroys the backing VM.
package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/dynamic"

	"github.com/cocoonstack/cocoon-sandbox-operator/pkg/scale/sandboxd"
)

// SandboxdClient is the subset of the sandboxd API this provider drives.
// *sandboxd.Client (from cocoon-sandbox-operator) satisfies Claim/Release;
// Lister adds the operator-index read used by status and the audit-only
// orphan scan.
type SandboxdClient interface {
	Claim(ctx context.Context, spec sandboxd.ClaimSpec) (sandboxd.ClaimResult, error)
	Release(ctx context.Context, id, token string) error
}

// Lister enumerates the node's live sandboxes (sandboxd GET /v1/sandboxes,
// root token). Separate from SandboxdClient so tests can fail listing
// independently of claiming.
type Lister interface {
	ListSandboxes(ctx context.Context) ([]ListedSandbox, error)
}

// ListedSandbox is one row of the sandboxd operator index.
type ListedSandbox struct {
	ID         string `json:"id"`
	Deadline   string `json:"deadline,omitempty"`
	Hibernated bool   `json:"hibernated,omitempty"`
}

// Claim is the durable record binding one pod key to one sandboxd claim. The
// token is the release credential: holding it is what makes this provider —
// never background reconciliation — the only party able to destroy the VM.
type Claim struct {
	ID      string `json:"id"`
	Token   string `json:"token"`
	Address string `json:"address,omitempty"`
	PodUID  string `json:"podUID"`
}

// Config assembles a Provider.
type Config struct {
	NodeName string
	Client   SandboxdClient
	Lister   Lister
	// Dynamic reads owner CRs for the destroy-authorization quorum. nil means
	// owner state is unverifiable and every guarded delete preserves.
	Dynamic dynamic.Interface
	// StatePath persists the claims table (0600) so a provider restart keeps
	// the release credentials. Empty disables persistence (tests).
	StatePath string
	Logger    logr.Logger
}

// Provider implements the virtual-kubelet PodLifecycleHandler over sandboxd.
type Provider struct {
	nodeName  string
	client    SandboxdClient
	lister    Lister
	dyn       dynamic.Interface
	statePath string
	log       logr.Logger

	mu       sync.RWMutex
	pods     map[string]*corev1.Pod // key -> last accepted pod object
	claims   map[string]Claim       // key -> sandboxd claim
	notifier func(*corev1.Pod)
}

// New builds a Provider and loads any persisted claims table.
func New(cfg Config) (*Provider, error) {
	p := &Provider{
		nodeName:  cfg.NodeName,
		client:    cfg.Client,
		lister:    cfg.Lister,
		dyn:       cfg.Dynamic,
		statePath: cfg.StatePath,
		log:       cfg.Logger,
		pods:      map[string]*corev1.Pod{},
		claims:    map[string]Claim{},
	}
	if err := p.loadState(); err != nil {
		return nil, err
	}
	return p, nil
}

func podKey(namespace, name string) string { return namespace + "/" + name }

// NotifyPods stores the kubelet's pod-status callback.
func (p *Provider) NotifyPods(_ context.Context, notifier func(*corev1.Pod)) {
	p.mu.Lock()
	p.notifier = notifier
	p.mu.Unlock()
}

func (p *Provider) notify(pod *corev1.Pod) {
	p.mu.RLock()
	n := p.notifier
	p.mu.RUnlock()
	if n != nil && pod != nil {
		n(pod)
	}
}

// claimFor returns the claim bound to key.
func (p *Provider) claimFor(key string) (Claim, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	c, ok := p.claims[key]
	return c, ok
}

// SnapshotClaims returns a copy of the pod-key → claim table. Inventory
// publishing reads it so the O(nodes) NodeInventory summary always reflects
// the node's own live bindings.
func (p *Provider) SnapshotClaims() map[string]Claim {
	p.mu.RLock()
	defer p.mu.RUnlock()
	out := make(map[string]Claim, len(p.claims))
	for k, v := range p.claims {
		out[k] = v
	}
	return out
}

// podUIDIsCurrent guards lifecycle actions against stale objects: a same-name
// pod with a different UID in the table means the request refers to a previous
// generation.
func (p *Provider) podUIDIsCurrent(key string, pod *corev1.Pod) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	cur, ok := p.pods[key]
	if !ok {
		return true // nothing tracked: not stale, just unknown
	}
	return cur.UID == pod.UID
}

// --- state persistence -------------------------------------------------------

type stateFile struct {
	Claims map[string]Claim `json:"claims"`
}

// loadState restores the claims table after a provider restart so the release
// credentials survive (mirrors the vk-cocoon fallback-identity contract: a
// restart must not orphan authority over live VMs).
func (p *Provider) loadState() error {
	if p.statePath == "" {
		return nil
	}
	b, err := os.ReadFile(p.statePath)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read state %s: %w", p.statePath, err)
	}
	var st stateFile
	if err := json.Unmarshal(b, &st); err != nil {
		return fmt.Errorf("decode state %s: %w", p.statePath, err)
	}
	if st.Claims != nil {
		p.claims = st.Claims
	}
	return nil
}

// saveState atomically persists the claims table. Callers hold no lock.
func (p *Provider) saveState() {
	if p.statePath == "" {
		return
	}
	p.mu.RLock()
	st := stateFile{Claims: make(map[string]Claim, len(p.claims))}
	for k, v := range p.claims {
		st.Claims[k] = v
	}
	p.mu.RUnlock()
	b, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		p.log.Error(err, "encode claims state")
		return
	}
	tmp := p.statePath + ".tmp"
	if err := os.MkdirAll(filepath.Dir(p.statePath), 0o700); err != nil {
		p.log.Error(err, "mkdir state dir")
		return
	}
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		p.log.Error(err, "write claims state")
		return
	}
	if err := os.Rename(tmp, p.statePath); err != nil {
		p.log.Error(err, "rename claims state")
	}
}
