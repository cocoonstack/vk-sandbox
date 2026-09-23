// Package provider implements the virtual-kubelet PodLifecycleHandler over
// sandboxd: a Pod scheduled to the virtual node becomes one warm claim, and a
// Pod deletion alone never destroys the backing VM.
package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/dynamic"

	"github.com/cocoonstack/sandbox-operator/pkg/sandboxd"
)

// undoReleaseTimeout bounds a compensating release whose caller context may already be canceled.
const undoReleaseTimeout = 10 * time.Second

// SandboxdClient is the sandboxd surface the claim path drives; *sandboxd.Client satisfies it and Lister.
type SandboxdClient interface {
	Claim(ctx context.Context, spec sandboxd.ClaimSpec) (sandboxd.ClaimResult, error)
	Release(ctx context.Context, id, token string) error
}

// Lister enumerates the node's live sandboxes, apart from SandboxdClient so tests can fail either alone.
type Lister interface {
	Sandboxes(ctx context.Context) ([]sandboxd.SandboxSummary, error)
}

// Claim binds one pod key to one sandboxd claim; Token is the release credential and never leaves the node.
type Claim struct {
	ID      string `json:"id"`
	Token   string `json:"token"`
	Address string `json:"address,omitempty"`
	PodUID  string `json:"podUID"` // forensics only; the stale-UID guard reads the pod table
	// ClaimedAt is reported as the Pod start time, so it must not move between reads.
	ClaimedAt metav1.Time `json:"claimedAt,omitzero"`
	// Deadline is the lease end sandboxd returned; zero means none known (an older table, or a keep-forever archive).
	Deadline metav1.Time `json:"deadline,omitzero"`
	// Owner is recorded when a delete preserves the claim; the owner re-check releases once it is gone.
	Owner *metav1.OwnerReference `json:"owner,omitempty"`
}

func (c Claim) expired(now time.Time) bool {
	return !c.Deadline.IsZero() && !now.Before(c.Deadline.Time)
}

func (c Claim) preservedForAnotherOwner(pod *corev1.Pod) bool {
	ref := metav1.GetControllerOf(pod)
	return c.Owner != nil && (ref == nil || ref.UID != c.Owner.UID)
}

// Config assembles a Provider.
type Config struct {
	Client SandboxdClient
	Lister Lister
	// Dynamic reads owner CRs for delete authorization; nil makes every guarded delete preserve.
	Dynamic dynamic.Interface
	// StatePath persists the claims table at 0600; empty disables persistence (tests).
	StatePath string
	Logger    logr.Logger
}

type stateFile struct {
	Claims    map[string]Claim `json:"claims"`
	Releasing []Claim          `json:"releasing,omitempty"`
}

type podNotifier func(*corev1.Pod)

// Provider implements the virtual-kubelet PodLifecycleHandler over sandboxd.
type Provider struct {
	client    SandboxdClient
	lister    Lister
	dyn       dynamic.Interface
	statePath string
	log       logr.Logger

	mu       sync.RWMutex
	pods     map[string]*corev1.Pod // key -> last accepted pod object
	claims   map[string]Claim
	notifier podNotifier

	// tentative holds keys whose claim is not on disk yet: invisible to persist, never Running.
	tentative map[string]struct{}

	// quarantined holds loaded keys no listing has vouched for: releasable, not adoptable or Running.
	quarantined map[string]struct{}

	// releasing holds claims withdrawn from their key whose release has not succeeded yet.
	releasing []Claim

	// orphanVerdicts is the previous scan's verdict per sandbox id; the scan goroutine owns it.
	orphanVerdicts map[string]string

	// ownerRechecks is the backoff per preserved key; the re-check goroutine owns it.
	ownerRechecks map[string]ownerRecheck

	// saveMu orders snapshot-to-rename, or a concurrent create renames an older snapshot last.
	saveMu sync.Mutex
}

// New builds a Provider and loads any persisted claims table.
func New(ctx context.Context, cfg Config) (*Provider, error) {
	p := &Provider{
		client:      cfg.Client,
		lister:      cfg.Lister,
		dyn:         cfg.Dynamic,
		statePath:   cfg.StatePath,
		log:         cfg.Logger,
		pods:        map[string]*corev1.Pod{},
		claims:      map[string]Claim{},
		tentative:   map[string]struct{}{},
		quarantined: map[string]struct{}{},

		ownerRechecks: map[string]ownerRecheck{},
	}
	if err := p.loadState(); err != nil {
		return nil, err
	}
	// A loaded table starts quarantined; the state-file tests build a provider with no lister.
	if p.lister != nil {
		p.quarantineLoadedClaims()
		p.VerifyClaimsAgainstNode(ctx)
	}
	// An unwritable state path would leak every microVM this process claims on restart.
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

func (p *Provider) ClaimAddresses() map[string]string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	out := make(map[string]string, len(p.claims))
	for _, c := range p.claims {
		if c.Address != "" {
			out[c.ID] = c.Address
		}
	}
	return out
}

// VerifyClaimsAgainstNode settles the quarantined rows against a listing: rows
// the node holds leave quarantine, rows it does not are dropped. A failed
// listing is not an empty list, so it drops nothing and reports false.
func (p *Provider) VerifyClaimsAgainstNode(ctx context.Context) bool {
	if p.lister == nil {
		return false
	}
	// A claim made while the listing is in flight is not in it, so only the snapshot is judged.
	p.mu.RLock()
	before := make(map[string]string, len(p.quarantined))
	for k := range p.quarantined {
		if c, ok := p.claims[k]; ok {
			before[k] = c.ID
		}
	}
	p.mu.RUnlock()
	if len(before) == 0 {
		return true
	}

	listed, err := p.lister.Sandboxes(ctx)
	if err != nil {
		p.log.Info("sandboxd list failed; claims stay unverified", "err", err.Error())
		return false
	}
	live := liveDeadlines(listed)

	p.mu.Lock()
	defer p.mu.Unlock()
	for key, id := range before {
		c, still := p.claims[key]
		if !still || c.ID != id {
			continue // replaced since the snapshot; this listing cannot judge it
		}
		if rowDeadline, ok := live[id]; ok {
			// An older table has no Deadline; the listing carries the lease end.
			if c.Deadline.IsZero() && !rowDeadline.IsZero() {
				c.Deadline = metav1.NewTime(rowDeadline)
				p.claims[key] = c
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
	if n != nil {
		n(pod)
	}
}

// settled means neither tentative nor quarantined; callers hold mu.
func (p *Provider) settled(key string) bool {
	_, pending := p.tentative[key]
	_, unverified := p.quarantined[key]
	return !pending && !unverified
}

// claimFor returns the settled claim for key; a tentative or quarantined one is withheld.
func (p *Provider) claimFor(key string) (Claim, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if !p.settled(key) {
		return Claim{}, false
	}
	c, ok := p.claims[key]
	return c, ok
}

// heldClaimFor returns the claim for key even before it is durable, for release paths.
func (p *Provider) heldClaimFor(key string) (Claim, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	c, ok := p.claims[key]
	return c, ok
}

// podUIDIsCurrent rejects a request carrying a previous pod generation's UID.
func (p *Provider) podUIDIsCurrent(key string, pod *corev1.Pod) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	cur, ok := p.pods[key]
	if !ok {
		return true // nothing tracked: not stale, just unknown
	}
	return cur.UID == pod.UID
}

// loadState restores the claims table so the release credentials survive a restart.
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
	p.releasing = st.Releasing
	if st.Claims == nil {
		return nil
	}
	// A table from before ClaimedAt would report a moving Pod start time; New persists the backfill.
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

// refreshDeadline adopts the node's lease for the row unless it was replaced since.
func (p *Provider) refreshDeadline(key, id string, deadline time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if c, ok := p.claims[key]; ok && c.ID == id {
		c.Deadline = metav1.NewTime(deadline)
		p.claims[key] = c
	}
}

func (p *Provider) hasQuarantined() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.quarantined) > 0
}

func (p *Provider) quarantineLoadedClaims() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for key := range p.claims {
		p.quarantined[key] = struct{}{}
	}
}

// saveState logs a failed persist: its callers have already changed what the node holds.
func (p *Provider) saveState() {
	if err := p.persist(); err != nil {
		p.log.Error(err, "persist claims state")
	}
}

// commitClaim makes a tentative claim durable; the marker clears only after the rename lands.
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

// write replaces the state file with the durable claims plus committing. Callers hold saveMu.
func (p *Provider) write(committing string) error {
	if p.statePath == "" {
		return nil
	}
	p.mu.RLock()
	st := stateFile{Claims: make(map[string]Claim, len(p.claims)), Releasing: slices.Clone(p.releasing)}
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
	// A deleted state directory must not turn every later save into a permanent failure.
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

func liveDeadlines(listed []sandboxd.SandboxSummary) map[string]time.Time {
	live := make(map[string]time.Time, len(listed))
	for _, s := range listed {
		live[s.ID] = s.Deadline
	}
	return live
}

func podKey(namespace, name string) string { return namespace + "/" + name }
