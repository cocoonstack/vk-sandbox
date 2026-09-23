package provider

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

	owner := metav1.GetControllerOf(pod)
	verdict, reason := destroyAuthorized(ctx, p.dyn, pod.Namespace, owner)
	if verdict != authRelease {
		p.log.Info("preserving sandbox: pod deletion is not VM authority",
			"pod", key, "claim", c.ID, "reason", reason)
		p.preserveClaim(key, c.ID, owner)
		return nil
	}

	p.log.Info("release authorized", "pod", key, "claim", c.ID, "reason", reason)
	if err := p.releaseClaim(ctx, key, c); err != nil {
		return fmt.Errorf("release sandbox %s for %s: %w", c.ID, key, err)
	}
	p.saveState()
	return nil
}

func (p *Provider) forgetPod(key string) {
	p.mu.Lock()
	delete(p.pods, key)
	p.mu.Unlock()
}

// preserveClaim keeps the claim for adopt-in-place and records the owner the re-check asks about.
func (p *Provider) preserveClaim(key, id string, owner *metav1.OwnerReference) {
	p.mu.Lock()
	delete(p.pods, key)
	if c, ok := p.claims[key]; ok && c.ID == id {
		c.Owner = owner
		p.claims[key] = c
	}
	p.mu.Unlock()
	p.saveState()
}

func (p *Provider) releaseClaim(ctx context.Context, key string, c Claim) error {
	if err := p.client.Release(ctx, c.ID, c.Token); err != nil {
		return err
	}
	p.withdrawClaim(key, c.ID)
	return nil
}
