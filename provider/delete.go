package provider

import (
	"context"
	"fmt"

	"github.com/projecteru2/core/log"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func (p *Provider) DeletePod(ctx context.Context, pod *corev1.Pod) error {
	logger := log.WithFunc("provider.DeletePod")
	key := podKey(pod.Namespace, pod.Name)
	if !p.podUIDIsCurrent(key, pod) {
		logger.Infof(ctx, "ignoring stale DeletePod for previous pod generation pod=%s uid=%s", key, pod.UID)
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
		logger.Infof(ctx, "preserving sandbox: pod deletion is not VM authority pod=%s claim=%s reason=%s", key, c.ID, reason)
		p.preserveClaim(ctx, key, c.ID, owner)
		return nil
	}

	logger.Infof(ctx, "release authorized pod=%s claim=%s reason=%s", key, c.ID, reason)
	if err := p.client.Release(ctx, c.ID, c.Token); err != nil {
		return fmt.Errorf("release sandbox %s for %s: %w", c.ID, key, err)
	}
	p.withdrawClaim(key, c.ID)
	p.saveState(ctx)
	return nil
}

func (p *Provider) forgetPod(key string) {
	p.mu.Lock()
	delete(p.pods, key)
	p.mu.Unlock()
}

// preserveClaim keeps the claim for adopt-in-place and records the owner the re-check asks about.
func (p *Provider) preserveClaim(ctx context.Context, key, id string, owner *metav1.OwnerReference) {
	p.mu.Lock()
	delete(p.pods, key)
	if c, ok := p.claims[key]; ok && c.ID == id {
		c.Owner = owner
		p.claims[key] = c
	}
	p.mu.Unlock()
	p.saveState(ctx)
}
