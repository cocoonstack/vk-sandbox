// Package provider implements the virtual-kubelet provider that bridges
// Kubernetes agent-sandbox semantics (agents.x-k8s.io, driven by
// sandbox-operator) onto sandboxd — the node-local hot-sandbox daemon
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
	"maps"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/dynamic"

	"github.com/cocoonstack/sandbox-operator/pkg/scale/sandboxd"
)

// undoReleaseTimeout bounds the compensating release when a fresh claim cannot
// be persisted; the caller's context may already be canceled by then.
const undoReleaseTimeout = 10 * time.Second

// SandboxdClient is the subset of the sandboxd API this provider drives.
// *sandboxd.Client (from sandbox-operator) satisfies Claim/Release;
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
	// ClaimRef is the k8s "<namespace>/<name>" the sandbox was claimed under
	// (echoed by sandboxd from the claim). Empty for claims made without one.
	ClaimRef string `json:"claim_ref,omitempty"`
}

// Claim is the durable record binding one pod key to one sandboxd claim. The
// token is the release credential: holding it is what makes this provider —
// never background reconciliation — the only party able to destroy the VM.
type Claim struct {
	ID      string `json:"id"`
	Token   string `json:"token"`
	Address string `json:"address,omitempty"`
	// PodUID is written for operator forensics — which Pod generation last held
	// this sandbox. The stale-request guard reads the in-memory pod table, not
	// this field, so a restart does not depend on it.
	PodUID string `json:"podUID"`
	// ClaimedAt is when this claim was taken. Status reads report it as the Pod
	// start time, which must not move between reads; a table written by an older
	// build has none, so the first read after upgrade settles it.
	ClaimedAt metav1.Time `json:"claimedAt,omitzero"`
	// Deadline is the lease end sandboxd returned for this claim. There is no
	// renewal anywhere in the stack — the e2b keepalive is record-keeping only —
	// so past it the reaper destroys the VM and status must stop saying Running.
	// Zero means unknown (an older table), which reads as no known expiry.
	Deadline metav1.Time `json:"deadline,omitzero"`
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

type stateFile struct {
	Claims map[string]Claim `json:"claims"`
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

	// tentative holds pod keys whose claim has not been durably written yet. Such
	// a claim is invisible to persist — otherwise a concurrent create's snapshot
	// would make it durable, and a later rollback could not take it back — and it
	// never reports Running, because its release credential may not survive.
	tentative map[string]struct{}

	// quarantined holds keys loaded from disk that no sandboxd listing has
	// confirmed yet. A row left behind by a release whose save failed looks
	// identical to a live one, so it may not be adopted or reported Running
	// until the node vouches for it.
	quarantined map[string]struct{}

	// orphanVerdicts is the previous scan's verdict per sandbox id, used to log
	// each verdict transition once. Touched only by the scan goroutine.
	orphanVerdicts map[string]string

	// saveMu orders snapshot-to-rename as one step. Without it concurrent pod
	// creates can rename an older snapshot last, dropping a release credential
	// and leaking its microVM until sandboxd's TTL reaps it.
	saveMu sync.Mutex
}

// New builds a Provider and loads any persisted claims table.
func New(ctx context.Context, cfg Config) (*Provider, error) {
	p := &Provider{
		nodeName:    cfg.NodeName,
		client:      cfg.Client,
		lister:      cfg.Lister,
		dyn:         cfg.Dynamic,
		statePath:   cfg.StatePath,
		log:         cfg.Logger,
		pods:        map[string]*corev1.Pod{},
		claims:      map[string]Claim{},
		tentative:   map[string]struct{}{},
		quarantined: map[string]struct{}{},
	}
	if err := p.loadState(); err != nil {
		return nil, err
	}
	// Nothing has vouched for a table read off disk, so it starts quarantined and
	// a listing settles it. Without a lister nothing ever could, and quarantining
	// would hide it forever; that shape is tests and the no-inventory path.
	if p.lister != nil {
		p.quarantineLoadedClaims()
		p.VerifyClaimsAgainstNode(ctx)
	}
	// Prove the claims table is writable before accepting any Pod. Running with an
	// unwritable state path would persist no release credential, so every claim
	// this process made would leak its microVM on restart. Failing here instead
	// keeps the virtual node out of scheduling until the path is fixed.
	if err := p.persist(); err != nil {
		return nil, fmt.Errorf("claims state is not writable: %w", err)
	}
	return p, nil
}

func (p *Provider) NotifyPods(_ context.Context, notifier func(*corev1.Pod)) {
	p.mu.Lock()
	p.notifier = notifier
	p.mu.Unlock()
}

// SnapshotClaims returns a copy of the pod-key → claim table. Inventory
// publishing reads it so the O(nodes) NodeInventory summary always reflects
// the node's own live bindings.
func (p *Provider) SnapshotClaims() map[string]Claim {
	p.mu.RLock()
	defer p.mu.RUnlock()
	out := make(map[string]Claim, len(p.claims))
	maps.Copy(out, p.claims)
	return out
}

// VerifyClaimsAgainstNode settles the rows no listing has vouched for yet:
// those the node still holds leave quarantine, those it does not are dropped.
// It deliberately judges nothing else — a row this process created is already
// known good, and judging it against a listing taken moments earlier is how a
// live sandbox loses its record. A release whose save failed leaves such a row
// behind, and reloading it would let a same-key Pod adopt a destroyed sandbox
// and report it Running.
//
// A failed listing is NOT an empty list — the 2026-05-15 rule — so nothing is
// dropped when sandboxd cannot be read. Unverified rows stay quarantined
// instead: still releasable, but invisible to adoption and to Running until a
// listing confirms them. Reports whether the listing succeeded.
func (p *Provider) VerifyClaimsAgainstNode(ctx context.Context) bool {
	if p.lister == nil {
		return false
	}
	// Snapshot first: a claim made while the listing is in flight is not in it,
	// and judging that claim by this list would drop a row for a live sandbox.
	p.mu.RLock()
	before := make(map[string]string, len(p.quarantined))
	for k := range p.quarantined {
		if c, ok := p.claims[k]; ok {
			before[k] = c.ID
		}
	}
	p.mu.RUnlock()
	if len(before) == 0 {
		return true // nothing unverified: the listing has nothing to judge
	}

	listed, err := p.lister.ListSandboxes(ctx)
	if err != nil {
		p.log.Info("sandboxd list failed; claims stay unverified", "err", err.Error())
		return false
	}
	live := make(map[string]string, len(listed))
	for _, s := range listed {
		live[s.ID] = s.Deadline
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	for key, id := range before {
		c, still := p.claims[key]
		if !still || c.ID != id {
			continue // replaced since the snapshot; this listing cannot judge it
		}
		if rowDeadline, ok := live[id]; ok {
			// A row from a build that predates Deadline would otherwise read as
			// never-expiring; the node's listing carries the lease end.
			if c.Deadline.IsZero() {
				if d := parseDeadline(rowDeadline); !d.IsZero() {
					c.Deadline = d
					p.claims[key] = c
				}
			}
			delete(p.quarantined, key)
			continue
		}
		if _, pending := p.tentative[key]; pending {
			continue // mid-create, not yet reported by sandboxd
		}
		delete(p.claims, key)
		delete(p.quarantined, key)
		p.log.Info("dropping a claim whose sandbox the node no longer holds", "pod", key, "claim", id)
	}
	return true
}

func (p *Provider) notify(pod *corev1.Pod) {
	p.mu.RLock()
	n := p.notifier
	p.mu.RUnlock()
	if n != nil && pod != nil {
		n(pod)
	}
}

// settled reports whether key's claim is durable and vouched for — neither
// tentative nor quarantined. Callers hold mu.
func (p *Provider) settled(key string) bool {
	_, pending := p.tentative[key]
	_, unverified := p.quarantined[key]
	return !pending && !unverified
}

// claimFor returns the durable claim bound to key. A claim whose credential is
// not on disk yet is withheld: it can still be rolled back, so nothing may
// report it Running or adopt it.
func (p *Provider) claimFor(key string) (Claim, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if !p.settled(key) {
		return Claim{}, false
	}
	c, ok := p.claims[key]
	return c, ok
}

// heldClaimFor returns the claim bound to key whether or not it is durable yet.
// Release paths use this: a tentative claim still holds a live microVM, and
// withholding it from delete would strand that VM until sandboxd's TTL.
func (p *Provider) heldClaimFor(key string) (Claim, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	c, ok := p.claims[key]
	return c, ok
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
	if st.Claims == nil {
		return nil
	}
	// A table written before ClaimedAt existed would otherwise report a Pod start
	// time that moves on every status read. New persists right after this, which
	// is what settles it for good.
	now := metav1.Now()
	for k, c := range st.Claims {
		if c.ClaimedAt.IsZero() {
			c.ClaimedAt = now
			st.Claims[k] = c
		}
	}
	p.claims = st.Claims
	return nil
}

// dropClaim removes a row the node confirmed gone. ID-guarded like refresh.
func (p *Provider) dropClaim(key, id string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if c, ok := p.claims[key]; ok && c.ID == id {
		delete(p.claims, key)
	}
}

// refreshDeadline adopts the node's view of a claim's lease. ID-guarded: the
// row may have been replaced since the caller looked.
func (p *Provider) refreshDeadline(key, id string, deadline metav1.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if c, ok := p.claims[key]; ok && c.ID == id {
		c.Deadline = deadline
		p.claims[key] = c
	}
}

func (p *Provider) hasQuarantined() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.quarantined) > 0
}

// quarantineLoadedClaims marks every loaded row unverified, for the case where
// startup could not reach sandboxd. The next successful listing clears them.
func (p *Provider) quarantineLoadedClaims() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for key := range p.claims {
		p.quarantined[key] = struct{}{}
	}
}

// saveState persists the claims table, logging rather than returning a failure.
// Its callers are the paths that have already changed what the node holds —
// adoption of a preserved claim, and release — where there is nothing left to
// undo. A fresh claim uses persist directly, because that one is still undoable.
func (p *Provider) saveState() {
	if err := p.persist(); err != nil {
		p.log.Error(err, "persist claims state")
	}
}

// commitClaim makes a tentative claim durable. The marker is cleared only after
// the rename lands, so the claim never looks durable during the write itself.
func (p *Provider) commitClaim(key string) error {
	p.saveMu.Lock()
	defer p.saveMu.Unlock()

	if err := p.write(key); err != nil {
		return err
	}
	p.mu.Lock()
	delete(p.tentative, key)
	p.mu.Unlock()
	return nil
}

// persist atomically writes the claims table. Callers hold no lock.
func (p *Provider) persist() error {
	p.saveMu.Lock()
	defer p.saveMu.Unlock()
	return p.write("")
}

// write serializes the durable claims — plus committing, the key being made
// durable by this call — and replaces the state file. Callers hold saveMu.
func (p *Provider) write(committing string) error {
	if p.statePath == "" {
		return nil
	}
	p.mu.RLock()
	st := stateFile{Claims: make(map[string]Claim, len(p.claims))}
	for k, c := range p.claims {
		if _, pending := p.tentative[k]; !pending || k == committing {
			st.Claims[k] = c
		}
	}
	p.mu.RUnlock()
	b, err := json.Marshal(st)
	if err != nil {
		return fmt.Errorf("encode claims state: %w", err)
	}
	// Cheap next to the write (2us of a 300us save) and it keeps a deleted
	// state directory from turning every later save into a permanent failure.
	if err := os.MkdirAll(filepath.Dir(p.statePath), 0o700); err != nil {
		return fmt.Errorf("mkdir state dir: %w", err)
	}
	tmp := p.statePath + ".tmp"
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		return fmt.Errorf("write claims state: %w", err)
	}
	if err := os.Rename(tmp, p.statePath); err != nil {
		return fmt.Errorf("rename claims state: %w", err)
	}
	return nil
}

func podKey(namespace, name string) string { return namespace + "/" + name }
