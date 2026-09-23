package provider

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/cocoonstack/sandbox-operator/pkg/sandboxd"
	"github.com/cocoonstack/sandbox-operator/pkg/scale"
)

// Annotation keys; template, net and size are the operator's selector keys verbatim.
const (
	// AnnRuntime names the runtime the pod template asks for; an absent value means sandboxd.
	AnnRuntime = "sandbox.cocoonstack.io/runtime"
	// RuntimeSandboxd is the AnnRuntime value this provider serves.
	RuntimeSandboxd = "sandboxd"

	// AnnTemplate/AnnNet/AnnSize select the sandboxd claim axes.
	AnnTemplate = scale.SelectorTemplateKey
	AnnNet      = scale.SelectorNetKey
	AnnSize     = scale.SelectorSizeKey
	// AnnTTLSeconds bounds the claim lease (0 = sandboxd default).
	AnnTTLSeconds = "sandbox.cocoonstack.io/ttl-seconds"

	// AnnClaimID is written back with the sandboxd claim id; the release token never leaves the node.
	AnnClaimID = "sandbox.cocoonstack.io/claim-id"

	// defaultClaimTTLSeconds is sandboxd's maxTTL; 0 would select the five-minute SDK default.
	defaultClaimTTLSeconds = 24 * 60 * 60
)

func (p *Provider) CreatePod(ctx context.Context, pod *corev1.Pod) error {
	key := podKey(pod.Namespace, pod.Name)

	if rt := ann(pod, AnnRuntime, RuntimeSandboxd); rt != RuntimeSandboxd {
		return fmt.Errorf("pod %s requests runtime %q; this node serves %q", key, rt, RuntimeSandboxd)
	}

	// An unverified row may be a live sandbox whose only credential is that row.
	if err := p.resolveUnverifiedClaim(ctx, key); err != nil {
		return err
	}
	if err := p.settleExpiredClaim(ctx, key); err != nil {
		return err
	}

	// The adopted credential is already durable, so this save is log-only.
	if c, ok := p.adoptExistingClaim(key, pod); ok {
		p.saveState()
		p.log.Info("adopted preserved sandbox for replacement pod", "pod", key, "claim", c.ID)
		p.pushRunning(pod, c)
		return nil
	}

	// A stranded claim holds a live microVM whose credential exists only here.
	if err := p.clearStrandedClaim(ctx, key); err != nil {
		return err
	}

	ttl := defaultClaimTTLSeconds
	if s := ann(pod, AnnTTLSeconds, ""); s != "" {
		v, err := strconv.Atoi(s)
		if err != nil || v < 0 {
			return fmt.Errorf("pod %s: invalid %s=%q", key, AnnTTLSeconds, s)
		}
		ttl = v
	}
	spec := sandboxd.ClaimSpec{
		Template:   ann(pod, AnnTemplate, ""),
		Net:        ann(pod, AnnNet, ""),
		Size:       ann(pod, AnnSize, ""),
		TTLSeconds: ttl,
		// Echoed in the node's index, which traces a claim whose response was lost to its Pod.
		ClaimRef: key,
	}
	if spec.Template == "" {
		return fmt.Errorf("pod %s: missing %s annotation", key, AnnTemplate)
	}

	res, err := p.client.Claim(ctx, spec)
	if err != nil {
		return fmt.Errorf("claim sandbox for %s (template %s): %w", key, spec.Template, err)
	}

	c := Claim{
		ID: res.ID, Token: res.Token, Address: res.OwnerAddr,
		PodUID: string(pod.UID), Authority: new(authorityOf(pod)), ClaimedAt: metav1.Now(),
		Deadline: metav1.NewTime(res.Deadline),
	}
	p.mu.Lock()
	p.claims[key] = c
	p.pods[key] = podWithClaim(pod, c.ID)
	// Invisible to a concurrent create's snapshot until its own write lands.
	p.tentative[key] = struct{}{}
	p.mu.Unlock()

	// Kubernetes has not been told Running, so an unpersisted claim is still undoable.
	if err := p.commitClaim(key); err != nil {
		return p.undoUnpersistedClaim(ctx, key, c, err)
	}

	p.log.Info("claimed hot sandbox", "pod", key, "claim", c.ID, "addr", c.Address)
	p.pushRunning(pod, c)
	return nil
}

func (p *Provider) UpdatePod(ctx context.Context, pod *corev1.Pod) error {
	key := podKey(pod.Namespace, pod.Name)
	if !p.podUIDIsCurrent(key, pod) {
		p.log.Info("ignoring stale UpdatePod for previous pod generation", "pod", key, "uid", pod.UID)
		return nil
	}
	p.mu.Lock()
	if _, pending := p.tentative[key]; pending {
		p.mu.Unlock()
		return p.CreatePod(ctx, pod)
	}
	p.pods[key] = pod.DeepCopy()
	p.mu.Unlock()
	return nil
}

// adoptExistingClaim rebinds pod to its key's claim in one locked step, so a row verification dropped is not written back.
func (p *Provider) adoptExistingClaim(key string, pod *corev1.Pod) (Claim, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	c, ok := p.claims[key]
	if !ok || !p.settled(key) {
		return Claim{}, false
	}
	if !c.adoptableBy(pod) {
		delete(p.claims, key)
		p.releasing = append(p.releasing, c)
		return Claim{}, false
	}
	c.PodUID = string(pod.UID)
	c.Authority = new(authorityOf(pod))
	c.Owner = nil
	if c.ClaimedAt.IsZero() {
		c.ClaimedAt = metav1.Now()
	}
	p.claims[key] = c
	p.pods[key] = pod.DeepCopy()
	return c, true
}

// settleExpiredClaim refreshes a past-deadline row the node still lists, drops one
// it no longer holds, and fails the create when the node cannot be listed.
func (p *Provider) settleExpiredClaim(ctx context.Context, key string) error {
	c, held := p.claimFor(key)
	if !held || !c.expired(time.Now()) {
		return nil
	}
	if p.lister == nil {
		p.dropClaim(key, c.ID)
		return nil
	}
	listed, err := p.lister.Sandboxes(ctx)
	if err != nil {
		return fmt.Errorf("pod %s: claim %s is past its cached deadline and sandboxd cannot be listed; refusing to replace it: %w", key, c.ID, err)
	}
	for _, row := range listed {
		if row.ID == c.ID {
			p.refreshDeadline(key, c.ID, row.Deadline)
			return nil
		}
	}
	p.dropClaim(key, c.ID)
	return nil
}

// resolveUnverifiedClaim fails the create rather than claim over a quarantined row it cannot settle.
func (p *Provider) resolveUnverifiedClaim(ctx context.Context, key string) error {
	p.mu.RLock()
	_, unverified := p.quarantined[key]
	p.mu.RUnlock()
	if !unverified {
		return nil
	}
	if p.VerifyClaimsAgainstNode(ctx) {
		return nil
	}
	return fmt.Errorf("pod %s: a claim from a previous run is unverified and sandboxd cannot be listed; refusing to claim over a possibly live sandbox", key)
}

// clearStrandedClaim returns a sandbox whose claim never reached disk and whose undo release failed.
func (p *Provider) clearStrandedClaim(ctx context.Context, key string) error {
	p.mu.RLock()
	c, held := p.claims[key]
	_, pending := p.tentative[key]
	p.mu.RUnlock()
	if !held || !pending {
		return nil
	}

	if err := p.releaseDetached(ctx, c); err != nil {
		return fmt.Errorf("pod %s: a previous sandbox %s could not be returned and its credential is only in memory: %w", key, c.ID, err)
	}
	p.withdrawClaim(key, c.ID)
	p.log.Info("returned a stranded sandbox before reusing its pod key", "pod", key, "claim", c.ID)
	return nil
}

// undoUnpersistedClaim returns a just-claimed sandbox; if that fails too, the credential stays in memory.
func (p *Provider) undoUnpersistedClaim(ctx context.Context, key string, c Claim, persistErr error) error {
	if err := p.releaseDetached(ctx, c); err != nil {
		p.log.Error(err, "could not return a sandbox whose claim failed to persist; keeping the credential in memory",
			"pod", key, "claim", c.ID)
		return errors.Join(
			fmt.Errorf("persist claim for %s: %w", key, persistErr),
			fmt.Errorf("release sandbox %s: %w", c.ID, err),
		)
	}
	p.withdrawClaim(key, c.ID)
	p.log.Info("returned sandbox after its claim could not be persisted", "pod", key, "claim", c.ID)
	return fmt.Errorf("persist claim for %s: %w", key, persistErr)
}

func (p *Provider) pushRunning(pod *corev1.Pod, c Claim) {
	out := podWithClaim(pod, c.ID)
	out.Status = runningStatus(out, c)
	p.notify(out)
}

// releaseDetached runs on its own deadline; the caller's context may already be canceled.
func (p *Provider) releaseDetached(ctx context.Context, c Claim) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), undoReleaseTimeout)
	defer cancel()
	return p.client.Release(ctx, c.ID, c.Token)
}

func (p *Provider) withdrawClaim(key, id string) {
	p.mu.Lock()
	if cur, ok := p.claims[key]; ok && cur.ID == id {
		delete(p.claims, key)
		delete(p.pods, key)
	}
	delete(p.tentative, key)
	delete(p.quarantined, key)
	p.mu.Unlock()
}

func podWithClaim(pod *corev1.Pod, id string) *corev1.Pod {
	out := pod.DeepCopy()
	if out.Annotations == nil {
		out.Annotations = map[string]string{}
	}
	out.Annotations[AnnClaimID] = id
	return out
}

func ann(pod *corev1.Pod, key, def string) string { return cmp.Or(pod.Annotations[key], def) }

// claimIP extracts the host of a sandboxd owner_addr ("10.0.0.5:7777").
func claimIP(addr string) string {
	if host, _, err := net.SplitHostPort(addr); err == nil && host != "" {
		return host
	}
	return addr
}
