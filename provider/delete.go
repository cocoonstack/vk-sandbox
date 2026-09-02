package provider

import (
	"context"
	"errors"
	"fmt"

	corev1 "k8s.io/api/core/v1"

	"github.com/cocoonstack/sandbox-operator/pkg/sandboxd"
)

func (p *Provider) DeletePod(ctx context.Context, pod *corev1.Pod) error {
	key := podKey(pod.Namespace, pod.Name)
	if !p.podUIDIsCurrent(key, pod) {
		p.log.Info("ignoring stale DeletePod for previous pod generation", "pod", key, "uid", pod.UID)
		return nil
	}

	c, hasClaim := p.heldClaimFor(key)
	if !hasClaim {
		p.forgetPod(key)
		return nil
	}

	verdict, reason := destroyAuthorized(ctx, p.dyn, pod)
	if verdict != authRelease {
		p.log.Info("preserving sandbox: pod deletion is not VM authority",
			"pod", key, "claim", c.ID, "reason", reason)
		p.forgetPod(key) // claim stays for adopt-in-place
		return nil
	}

	p.log.Info("release authorized", "pod", key, "claim", c.ID, "reason", reason)
	if err := p.client.Release(ctx, c.ID, c.Token); err != nil {
		var he *sandboxd.HTTPError
		if errors.As(err, &he) && he.StatusCode == 404 {
			// Already gone (sandboxd treats released-again as 404): converge.
			p.log.Info("sandbox already gone at release", "pod", key, "claim", c.ID)
		} else {
			// Keep claim + pod so the kubelet retries the delete with the
			// release credential intact.
			return fmt.Errorf("release sandbox %s for %s: %w", c.ID, key, err)
		}
	}
	p.mu.Lock()
	delete(p.claims, key)
	delete(p.pods, key)
	delete(p.tentative, key)
	delete(p.quarantined, key)
	p.mu.Unlock()
	p.saveState()
	return nil
}

// forgetPod drops the pod entry only. The claim (if any) is deliberately kept:
// dropping it would orphan the release credential and turn a preserved sandbox
// into an unreleasable one.
func (p *Provider) forgetPod(key string) {
	p.mu.Lock()
	delete(p.pods, key)
	p.mu.Unlock()
}
