package provider

import (
	"context"
	"time"

	"github.com/projecteru2/core/log"
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
	logger := log.WithFunc("provider.OrphanScan")
	if p.lister == nil {
		return nil, nil, false
	}
	listed, err := p.lister.Sandboxes(ctx)
	if err != nil {
		logger.Warnf(ctx, "sandboxd list failed; skipping orphan scan this cycle (failed query is not an empty list) err=%v", err)
		return nil, nil, false
	}

	live := liveDeadlines(listed)

	p.mu.RLock()
	claimed := make(map[string]string, len(p.claims)) // claim id -> pod key
	for key, c := range p.claims {
		claimed[c.ID] = key
	}
	queued := make(map[string]struct{}, len(p.releasing))
	for _, c := range p.releasing {
		queued[c.ID] = struct{}{}
	}
	p.mu.RUnlock()

	verdicts := make(map[string]string, len(listed))
	for _, s := range listed {
		if _, ok := claimed[s.ID]; ok {
			continue
		}
		if _, ok := queued[s.ID]; ok {
			continue
		}
		if s.ClaimRef != "" {
			if p.recordVerdict(verdicts, s.ID, verdictExternal) {
				logger.Infof(ctx, "sandbox claimed outside this provider (claim_ref set, no local pod); not an orphan candidate sandbox=%s claimRef=%s", s.ID, s.ClaimRef)
			}
			continue
		}
		orphans = append(orphans, s.ID)
		if p.recordVerdict(verdicts, s.ID, verdictOrphan) {
			logger.Infof(ctx, "possible orphan sandbox: live on node but bound to no pod; audit-only, retaining sandbox=%s", s.ID)
		}
	}
	for id, key := range claimed {
		if _, ok := live[id]; !ok {
			staleClaims = append(staleClaims, key)
			if p.recordVerdict(verdicts, id, verdictStale) {
				logger.Infof(ctx, "claim references a sandbox no longer on the node (TTL reap or external release) pod=%s sandbox=%s", key, id)
			}
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

// recordVerdict reports whether the verdict changed since the previous scan, so each change logs once (#3).
func (p *Provider) recordVerdict(verdicts map[string]string, id, verdict string) bool {
	verdicts[id] = verdict
	return p.orphanVerdicts[id] != verdict
}

func (p *Provider) publishExpiredLeases(ctx context.Context) {
	logger := log.WithFunc("provider.publishExpiredLeases")
	now := time.Now()
	candidates := map[string]Claim{}
	p.mu.RLock()
	for key, c := range p.claims {
		if !c.expired(now) || !p.settled(key) || p.pods[key] == nil {
			continue
		}
		candidates[key] = c
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
			logger.Warnf(ctx, "sandboxd list failed; deferring lease-expiry publication err=%v", err)
			return
		}
		live = liveDeadlines(listed)
	}

	for key, c := range candidates {
		rowDeadline, alive := live[c.ID]
		if alive {
			p.refreshDeadline(key, c.ID, rowDeadline)
			continue
		}
		// The expiry belongs to the candidate claim, not to whatever holds the key now.
		p.mu.RLock()
		pod := p.pods[key]
		current, held := p.claims[key]
		p.mu.RUnlock()
		if pod == nil || !held || current.ID != c.ID {
			continue
		}
		out := pod.DeepCopy()
		out.Status = expiredStatus(out, c)
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
