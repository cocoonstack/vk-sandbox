package provider

import (
	"context"
	"time"

	corev1 "k8s.io/api/core/v1"
)

// OrphanScan compares the node's live sandboxes against the claims table.
// It is strictly audit-only, carrying over two hard-won vk-cocoon rules:
//
//   - Background reconciliation can never prove user intent, so it NEVER
//     releases or destroys anything. It only logs candidates for an explicit,
//     identity-checked cleanup.
//   - A failed list query is NOT an empty list. Treating "query failed" as
//     "zero known sandboxes" once deleted every active VM's state in one
//     sweep (2026-05-15); on failure the whole cycle is skipped.
//
// Returns (orphans, staleClaims, ok): sandboxd rows without a claim, and claim
// entries whose sandbox is gone (reaped at TTL or released elsewhere).
func (p *Provider) OrphanScan(ctx context.Context) (orphans []string, staleClaims []string, ok bool) {
	if p.lister == nil {
		return nil, nil, false
	}
	listed, err := p.lister.ListSandboxes(ctx)
	if err != nil {
		p.log.Info("sandboxd list failed; skipping orphan scan this cycle (failed query is not an empty list)", "err", err.Error())
		return nil, nil, false
	}

	live := make(map[string]struct{}, len(listed))
	for _, s := range listed {
		live[s.ID] = struct{}{}
	}

	p.mu.RLock()
	claimed := make(map[string]string, len(p.claims)) // claim id -> pod key
	for key, c := range p.claims {
		claimed[c.ID] = key
	}
	p.mu.RUnlock()

	for _, s := range listed {
		if _, ok := claimed[s.ID]; !ok {
			orphans = append(orphans, s.ID)
			p.log.Info("possible orphan sandbox: live on node but bound to no pod; audit-only, retaining",
				"sandbox", s.ID)
		}
	}
	for id, key := range claimed {
		if _, ok := live[id]; !ok {
			staleClaims = append(staleClaims, key)
			p.log.Info("claim references a sandbox no longer on the node (TTL reap or external release)",
				"pod", key, "sandbox", id)
		}
	}
	return orphans, staleClaims, true
}

// RunOrphanScan runs OrphanScan on an interval until ctx is done.
func (p *Provider) RunOrphanScan(ctx context.Context, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			p.OrphanScan(ctx)
		}
	}
}

// RunClaimVerification re-checks the claims table against the node until ctx is
// done. It is separate from the orphan scan because that one is an operator
// audit an operator may switch off, while this is what lifts a startup
// quarantine — a claim the node still holds must become usable again.
func (p *Provider) RunClaimVerification(ctx context.Context, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if p.VerifyClaimsAgainstNode(ctx) && !p.hasQuarantined() {
				return // the table is vouched for; nothing left to re-check
			}
		}
	}
}

// RunLeaseWatch publishes the Failed status of pods whose sandbox lease has
// ended. It exists because implementing NotifyPods makes this an asynchronous
// provider — virtual-kubelet then never polls GetPodStatus, so a status pushed
// as Running would stand forever after the reaper destroys the VM. The expired
// status is deterministic (built from the claim's own timestamps), so pushing
// it again each tick patches nothing server-side; no fired-marker is needed.
func (p *Provider) RunLeaseWatch(ctx context.Context, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			p.publishExpiredLeases()
		}
	}
}

func (p *Provider) publishExpiredLeases() {
	now := time.Now()
	var expired []*corev1.Pod
	p.mu.RLock()
	for key, c := range p.claims {
		if c.Deadline.IsZero() || now.Before(c.Deadline.Time) {
			continue
		}
		if _, pending := p.tentative[key]; pending {
			continue
		}
		if _, unverified := p.quarantined[key]; unverified {
			continue
		}
		pod := p.pods[key]
		if pod == nil {
			continue
		}
		out := pod.DeepCopy()
		out.Status = expiredStatus(out, c)
		expired = append(expired, out)
	}
	p.mu.RUnlock()
	for _, pod := range expired {
		p.notify(pod)
	}
}
