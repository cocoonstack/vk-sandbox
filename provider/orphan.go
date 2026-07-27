package provider

import (
	"context"
	"time"
)

const (
	verdictExternal = "external"
	verdictOrphan   = "orphan"
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
// Returns (orphans, staleClaims, ok): sandboxd rows with neither a local claim
// nor a claim_ref (a claim_ref this provider does not hold means an
// apiserver-direct claim — externally owned, not an orphan candidate), and
// claim entries whose sandbox is gone (reaped at TTL or released elsewhere).
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

	// Verdicts are logged on first sight only (#3): the apiserver-direct claim
	// path legitimately holds sandboxes this provider never sees, and re-logging
	// every cycle buried genuine orphans in permanent noise.
	verdicts := make(map[string]string, len(listed))
	for _, s := range listed {
		if _, ok := claimed[s.ID]; ok {
			continue
		}
		if s.ClaimRef != "" {
			verdicts[s.ID] = verdictExternal
			if p.orphanVerdicts[s.ID] != verdictExternal {
				p.log.Info("sandbox claimed outside this provider (claim_ref set, no local pod); not an orphan candidate",
					"sandbox", s.ID, "claimRef", s.ClaimRef)
			}
			continue
		}
		orphans = append(orphans, s.ID)
		verdicts[s.ID] = verdictOrphan
		if p.orphanVerdicts[s.ID] != verdictOrphan {
			p.log.Info("possible orphan sandbox: live on node but bound to no pod; audit-only, retaining",
				"sandbox", s.ID)
		}
	}
	p.orphanVerdicts = verdicts
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
			p.publishExpiredLeases(ctx)
		}
	}
}

func (p *Provider) publishExpiredLeases(ctx context.Context) {
	now := time.Now()
	type candidate struct {
		key   string
		claim Claim
	}
	var candidates []candidate
	p.mu.RLock()
	for key, c := range p.claims {
		if c.Deadline.IsZero() || now.Before(c.Deadline.Time) {
			continue
		}
		if !p.settled(key) || p.pods[key] == nil {
			continue
		}
		candidates = append(candidates, candidate{key: key, claim: c})
	}
	p.mu.RUnlock()
	if len(candidates) == 0 {
		return
	}

	// Failed is terminal — virtual-kubelet refuses further updates for such a
	// Pod — and the cached deadline is not authoritative: the archive lifecycle
	// rewrites a claim's lease on the node. So the node confirms every terminal
	// publication, a still-listed claim just gets its deadline refreshed, and an
	// unlistable node publishes nothing this tick.
	live := map[string]string{}
	if p.lister != nil {
		listed, err := p.lister.ListSandboxes(ctx)
		if err != nil {
			p.log.Info("sandboxd list failed; deferring lease-expiry publication", "err", err.Error())
			return
		}
		for _, row := range listed {
			live[row.ID] = row.Deadline
		}
	}

	for _, cand := range candidates {
		rowDeadline, alive := live[cand.claim.ID]
		if alive {
			p.refreshDeadline(cand.key, cand.claim.ID, parseDeadline(rowDeadline))
			continue
		}
		// The key may have changed hands behind the listing; the expiry
		// belongs to the candidate claim, not to whatever holds the key now.
		p.mu.RLock()
		pod := p.pods[cand.key]
		current, held := p.claims[cand.key]
		p.mu.RUnlock()
		if pod == nil || !held || current.ID != cand.claim.ID {
			continue
		}
		out := pod.DeepCopy()
		out.Status = expiredStatus(out, cand.claim)
		p.notify(out)
	}
}
