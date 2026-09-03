package provider

import (
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

// Pod-contract annotation keys. Template/net/size reuse the operator's
// pkg/scale selector keys verbatim so one contract spans the L2 gateway and
// this provider; the rest are provider-local.
const (
	// AnnRuntime routes a pod to this provider. sandbox-operator's
	// runtime mutator sets it on sandbox pods destined for sandboxd nodes.
	AnnRuntime = "sandbox.cocoonstack.io/runtime"
	// RuntimeSandboxd is the AnnRuntime value this provider serves.
	RuntimeSandboxd = "sandboxd"

	// AnnTemplate/AnnNet/AnnSize select the sandboxd claim axes.
	AnnTemplate = scale.SelectorTemplateKey
	AnnNet      = scale.SelectorNetKey
	AnnSize     = scale.SelectorSizeKey
	// AnnTTLSeconds bounds the claim lease (0 = sandboxd default).
	AnnTTLSeconds = "sandbox.cocoonstack.io/ttl-seconds"

	// AnnClaimID is written back by the provider: the sandboxd claim id backing
	// this pod. Identity only — the release token never leaves the node.
	AnnClaimID = "sandbox.cocoonstack.io/claim-id"

	// defaultClaimTTLSeconds is requested when a pod carries no TTL annotation:
	// sandboxd's maxTTL. A pod's sandbox lives until the pod is deleted, and
	// sending 0 would mean sandboxd's own default — five minutes, sized for
	// ephemeral SDK claims, after which the reaper destroys the VM under a pod
	// still reporting Running.
	defaultClaimTTLSeconds = 24 * 60 * 60
)

func (p *Provider) CreatePod(ctx context.Context, pod *corev1.Pod) error {
	key := podKey(pod.Namespace, pod.Name)

	if rt := ann(pod, AnnRuntime, RuntimeSandboxd); rt != RuntimeSandboxd {
		return fmt.Errorf("pod %s requests runtime %q; this node serves %q", key, rt, RuntimeSandboxd)
	}

	// An unverified row from a previous process may still be a live sandbox whose
	// only credential is that row. Settle it before deciding anything, so a
	// vouched-for sandbox is adopted on this pass rather than the next retry.
	if err := p.resolveUnverifiedClaim(ctx, key); err != nil {
		return err
	}
	if err := p.settleExpiredClaim(ctx, key); err != nil {
		return err
	}

	// Adopt-in-place: an existing claim for this key survives pod churn. Its
	// release credential is already durable — it was persisted when the sandbox
	// was first claimed — so this save only refreshes which Pod holds it, and a
	// failure costs nothing recoverable. That is why it stays log-only.
	if c, ok := p.adoptExistingClaim(key, pod); ok {
		p.saveState()
		p.log.Info("adopted preserved sandbox for replacement pod", "pod", key, "claim", c.ID)
		p.pushRunning(pod, c)
		return nil
	}

	// A stranded claim from an earlier failed create still holds a live microVM
	// and its credential exists nowhere else. Claiming over it would destroy the
	// only handle to that sandbox, so it is returned first.
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
		// The node echoes this back in its operator index, which is what lets a
		// claim whose response never arrived be traced to the Pod it was for.
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
		PodUID: string(pod.UID), ClaimedAt: metav1.Now(),
		Deadline: metav1.NewTime(res.Deadline),
	}
	p.mu.Lock()
	p.claims[key] = c
	p.pods[key] = pod.DeepCopy()
	// Tentative until its own write lands: a concurrent create's snapshot must
	// not make this claim durable, or the rollback below could not take it back.
	p.tentative[key] = struct{}{}
	p.mu.Unlock()

	// Nothing has told Kubernetes this Pod is running yet, so a claim whose
	// release credential could not be stored is still undoable — and must be
	// undone, or the microVM leaks with no way to reach it after a restart.
	if err := p.commitClaim(key); err != nil {
		return p.undoUnpersistedClaim(ctx, key, c, err)
	}

	p.log.Info("claimed hot sandbox", "pod", key, "claim", c.ID, "addr", c.Address)
	p.pushRunning(pod, c)
	return nil
}

func (p *Provider) UpdatePod(_ context.Context, pod *corev1.Pod) error {
	key := podKey(pod.Namespace, pod.Name)
	if !p.podUIDIsCurrent(key, pod) {
		p.log.Info("ignoring stale UpdatePod for previous pod generation", "pod", key, "uid", pod.UID)
		return nil
	}
	p.mu.Lock()
	p.pods[key] = pod.DeepCopy()
	p.mu.Unlock()
	return nil
}

// adoptExistingClaim binds pod to the claim already held for its key. The read
// and the write are one locked step: verification can drop a row between them,
// and writing an outside copy back would resurrect a sandbox that is gone.
func (p *Provider) adoptExistingClaim(key string, pod *corev1.Pod) (Claim, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	c, ok := p.claims[key]
	if !ok || !p.settled(key) {
		return Claim{}, false
	}
	c.PodUID = string(pod.UID)
	if c.ClaimedAt.IsZero() {
		c.ClaimedAt = metav1.Now()
	}
	p.claims[key] = c
	p.pods[key] = pod.DeepCopy()
	return c, true
}

// settleExpiredClaim decides what a past-deadline row means before its key is
// reused. The cached deadline is not authoritative — the archive lifecycle
// rewrites a claim's lease on the node — so a still-listed claim has its
// deadline refreshed and stays adoptable, only a claim the node no longer
// holds is dropped for replacement, and an unlistable node fails the create
// rather than overwriting what may be a live sandbox's only credential.
func (p *Provider) settleExpiredClaim(ctx context.Context, key string) error {
	c, held := p.claimFor(key)
	if !held || c.Deadline.IsZero() || time.Now().Before(c.Deadline.Time) {
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

// resolveUnverifiedClaim settles a quarantined row before its pod key is reused.
// Claiming over it would strand a live sandbox whose only credential is that
// row, so an unresolvable one fails the create and leaves the Pod pending.
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

// clearStrandedClaim returns a sandbox left behind by an earlier create whose
// claim never reached disk and whose compensating release failed. Until it is
// gone this key cannot be reused: its credential lives only in memory.
func (p *Provider) clearStrandedClaim(ctx context.Context, key string) error {
	p.mu.RLock()
	c, held := p.claims[key]
	_, pending := p.tentative[key]
	p.mu.RUnlock()
	if !held || !pending {
		return nil
	}

	if err := p.releaseDetached(ctx, c); err != nil {
		var he *sandboxd.HTTPError
		if !errors.As(err, &he) || he.StatusCode != 404 {
			return fmt.Errorf("pod %s: a previous sandbox %s could not be returned and its credential is only in memory: %w", key, c.ID, err)
		}
	}
	p.withdrawClaim(key, c.ID)
	p.log.Info("returned a stranded sandbox before reusing its pod key", "pod", key, "claim", c.ID)
	return nil
}

// undoUnpersistedClaim hands a just-claimed sandbox back after its release
// credential could not be stored. A release that also fails keeps the
// in-memory claim so a later DeletePod can still reach the sandbox.
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

// pushRunning stamps the claim identity and Running status onto a copy of the
// pod and notifies the kubelet.
func (p *Provider) pushRunning(pod *corev1.Pod, c Claim) {
	out := pod.DeepCopy()
	if out.Annotations == nil {
		out.Annotations = map[string]string{}
	}
	out.Annotations[AnnClaimID] = c.ID
	out.Status = runningStatus(out, c)
	p.notify(out)
}

// releaseDetached runs on its own deadline: a compensating release must not
// depend on a caller context that may already be canceled.
func (p *Provider) releaseDetached(ctx context.Context, c Claim) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), undoReleaseTimeout)
	defer cancel()
	return p.client.Release(ctx, c.ID, c.Token)
}

// withdrawClaim removes the claim and pod entries together, and only while the
// claim is still the one the caller stored.
func (p *Provider) withdrawClaim(key, id string) {
	p.mu.Lock()
	if cur, ok := p.claims[key]; ok && cur.ID == id {
		delete(p.claims, key)
		delete(p.pods, key)
	}
	delete(p.tentative, key)
	p.mu.Unlock()
}

func ann(pod *corev1.Pod, key, def string) string {
	if pod.Annotations == nil {
		return def
	}
	if v, ok := pod.Annotations[key]; ok && v != "" {
		return v
	}
	return def
}

// claimIP extracts the host of a sandboxd owner_addr ("10.0.0.5:7777").
func claimIP(addr string) string {
	if addr == "" {
		return ""
	}
	if host, _, err := net.SplitHostPort(addr); err == nil && host != "" {
		return host
	}
	return addr
}
