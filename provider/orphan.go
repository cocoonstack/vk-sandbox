package provider

import (
	"context"
	"time"
)

const (
	verdictExternal = "external"
	verdictOrphan   = "orphan"
	verdictStale    = "stale"
)

// OrphanScan compares the node's live sandboxes against the claims table and
// only reports: background reconciliation cannot prove intent, and a failed
// listing is not an empty list. Returns orphans, stale claim keys, and ok.
func (p *Provider) OrphanScan(ctx context.Context) (orphans []string, staleClaims []string, ok bool) {
	if p.lister == nil {
		return nil, nil, false
	}
	listed, err := p.lister.Sandboxes(ctx)
	if err != nil {
		p.log.Info("sandboxd list failed; skipping orphan scan this cycle (failed query is not an empty list)", "err", err.Error())
		return nil, nil, false
	}

	live := liveDeadlines(listed)

	p.mu.RLock()
	claimed := make(map[string]string, len(p.claims)) // claim id -> pod key
	for key, c := range p.claims {
		claimed[c.ID] = key
	}
	p.mu.RUnlock()

	verdicts := make(map[string]string, len(listed))
	for _, s := range listed {
		if _, ok := claimed[s.ID]; ok {
			continue
		}
		if s.ClaimRef != "" {
			p.recordVerdict(verdicts, s.ID, verdictExternal,
				"sandbox claimed outside this provider (claim_ref set, no local pod); not an orphan candidate",
				"sandbox", s.ID, "claimRef", s.ClaimRef)
			continue
		}
		orphans = append(orphans, s.ID)
		p.recordVerdict(verdicts, s.ID, verdictOrphan,
			"possible orphan sandbox: live on node but bound to no pod; audit-only, retaining",
			"sandbox", s.ID)
	}
	for id, key := range claimed {
		if _, ok := live[id]; !ok {
			staleClaims = append(staleClaims, key)
			p.recordVerdict(verdicts, id, verdictStale,
				"claim references a sandbox no longer on the node (TTL reap or external release)",
				"pod", key, "sandbox", id)
		}
	}
	p.orphanVerdicts = verdicts
	return orphans, staleClaims, true
}

// RunOrphanScan runs OrphanScan on an interval until ctx is done.
func (p *Provider) RunOrphanScan(ctx context.Context, interval time.Duration) {
	runTicker(ctx, interval, func() bool {
		p.OrphanScan(ctx)
		return true
	})
}

// RunClaimVerification lifts a startup quarantine on its own loop, so switching the audit scan off cannot strand a claim.
func (p *Provider) RunClaimVerification(ctx context.Context, interval time.Duration) {
	runTicker(ctx, interval, func() bool {
		return !p.VerifyClaimsAgainstNode(ctx) || p.hasQuarantined()
	})
}

// RunLeaseWatch pushes Failed for pods whose lease ended; virtual-kubelet never polls
// an asynchronous provider. The status is deterministic, so a repeat push patches nothing.
func (p *Provider) RunLeaseWatch(ctx context.Context, interval time.Duration) {
	runTicker(ctx, interval, func() bool {
		p.publishExpiredLeases(ctx)
		return true
	})
}

// recordVerdict logs a verdict only when it changed since the previous scan (#3).
func (p *Provider) recordVerdict(verdicts map[string]string, id, verdict, msg string, kv ...any) {
	verdicts[id] = verdict
	if p.orphanVerdicts[id] != verdict {
		p.log.Info(msg, kv...)
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
		if !c.expired(now) || !p.settled(key) || p.pods[key] == nil {
			continue
		}
		candidates = append(candidates, candidate{key: key, claim: c})
	}
	p.mu.RUnlock()
	if len(candidates) == 0 {
		return
	}

	// Failed is terminal and the cached deadline is not authoritative, so the node
	// confirms every publication; an unlistable node publishes nothing this tick.
	live := map[string]time.Time{}
	if p.lister != nil {
		listed, err := p.lister.Sandboxes(ctx)
		if err != nil {
			p.log.Info("sandboxd list failed; deferring lease-expiry publication", "err", err.Error())
			return
		}
		live = liveDeadlines(listed)
	}

	for _, cand := range candidates {
		rowDeadline, alive := live[cand.claim.ID]
		if alive {
			p.refreshDeadline(cand.key, cand.claim.ID, rowDeadline)
			continue
		}
		// The expiry belongs to the candidate claim, not to whatever holds the key now.
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

func runTicker(ctx context.Context, interval time.Duration, action func() bool) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if !action() {
				return
			}
		}
	}
}
