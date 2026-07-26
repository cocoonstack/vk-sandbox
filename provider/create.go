package provider

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/cocoonstack/sandbox-operator/pkg/scale"
	"github.com/cocoonstack/sandbox-operator/pkg/scale/sandboxd"
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
)

func ann(pod *corev1.Pod, key, def string) string {
	if pod.Annotations == nil {
		return def
	}
	if v, ok := pod.Annotations[key]; ok && v != "" {
		return v
	}
	return def
}

// CreatePod claims a hot sandbox from sandboxd for the pod. Two properties
// carry the decentralized design:
//
//   - Adopt-in-place: a same-key pod whose predecessor was deleted without
//     destroy authorization (churn, eviction) finds the preserved claim and
//     adopts it instead of claiming a new VM — pod deletion stayed invisible
//     to the sandbox.
//   - No warm capacity is a typed failure (sandboxd 429/redirect): the pod
//     stays Pending and the operator's L1 path handles fallback; this provider
//     never queues or retries into the node.
func (p *Provider) CreatePod(ctx context.Context, pod *corev1.Pod) error {
	key := podKey(pod.Namespace, pod.Name)

	if rt := ann(pod, AnnRuntime, RuntimeSandboxd); rt != RuntimeSandboxd {
		return fmt.Errorf("pod %s requests runtime %q; this node serves %q", key, rt, RuntimeSandboxd)
	}

	// Adopt-in-place: an existing claim for this key survives pod churn.
	if c, ok := p.claimFor(key); ok {
		p.mu.Lock()
		c.PodUID = string(pod.UID)
		if c.ClaimedAt.IsZero() {
			c.ClaimedAt = metav1.Now()
		}
		p.claims[key] = c
		p.pods[key] = pod.DeepCopy()
		p.mu.Unlock()
		p.saveState()
		p.log.Info("adopted preserved sandbox for replacement pod", "pod", key, "claim", c.ID)
		p.pushRunning(pod, c)
		return nil
	}

	ttl := 0
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
	}
	if spec.Template == "" {
		return fmt.Errorf("pod %s: missing %s annotation", key, AnnTemplate)
	}

	res, err := p.client.Claim(ctx, spec)
	if err != nil {
		return fmt.Errorf("claim sandbox for %s (template %s): %w", key, spec.Template, err)
	}

	c := Claim{ID: res.ID, Token: res.Token, Address: res.OwnerAddr, PodUID: string(pod.UID), ClaimedAt: metav1.Now()}
	p.mu.Lock()
	p.claims[key] = c
	p.pods[key] = pod.DeepCopy()
	p.mu.Unlock()

	// Nothing has told Kubernetes this Pod is running yet, so a claim whose
	// release credential could not be stored is still undoable — and must be
	// undone, or the microVM leaks with no way to reach it after a restart.
	if err := p.persist(); err != nil {
		return p.undoUnpersistedClaim(key, c, err)
	}

	p.log.Info("claimed hot sandbox", "pod", key, "claim", c.ID, "addr", c.Address)
	p.pushRunning(pod, c)
	return nil
}

// undoUnpersistedClaim hands a just-claimed sandbox back after its release
// credential could not be stored. The release runs on its own deadline because
// the caller's context may already be canceled. A release that also fails keeps
// the in-memory claim so a later DeletePod can still reach the sandbox.
func (p *Provider) undoUnpersistedClaim(key string, c Claim, persistErr error) error {
	ctx, cancel := context.WithTimeout(context.Background(), undoReleaseTimeout)
	defer cancel()

	if err := p.client.Release(ctx, c.ID, c.Token); err != nil {
		p.log.Error(err, "could not return a sandbox whose claim failed to persist; keeping the credential in memory",
			"pod", key, "claim", c.ID)
		return errors.Join(
			fmt.Errorf("persist claim for %s: %w", key, persistErr),
			fmt.Errorf("release sandbox %s: %w", c.ID, err),
		)
	}
	// The claim and pod entries were written together, so they are withdrawn
	// together and only while they are still the ones this call stored.
	p.mu.Lock()
	if cur, ok := p.claims[key]; ok && cur.ID == c.ID {
		delete(p.claims, key)
		delete(p.pods, key)
	}
	p.mu.Unlock()
	p.log.Info("returned sandbox after its claim could not be persisted", "pod", key, "claim", c.ID)
	return fmt.Errorf("persist claim for %s: %w", key, persistErr)
}

// UpdatePod records the newest pod object; sandbox pods are immutable at the
// runtime level, so no VM action is ever taken here.
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
