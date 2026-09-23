package provider

import (
	"context"
	"maps"
	"strings"
	"time"
)

// ownerRecheckMaxDelay caps the backoff between owner reads for one preserved claim.
const ownerRecheckMaxDelay = 10 * time.Minute

type ownerRecheck struct {
	at    time.Time
	delay time.Duration
}

// RunOwnerRecheck releases pod-less claims once the owner that kept them preserved is gone (#18).
func (p *Provider) RunOwnerRecheck(ctx context.Context, interval time.Duration) {
	runTicker(ctx, interval, func() bool {
		p.recheckOwners(ctx, time.Now(), interval)
		return true
	})
}

// recheckOwners asks about each due preserved claim's owner with the delete verdicts, backing off from base.
func (p *Provider) recheckOwners(ctx context.Context, now time.Time, base time.Duration) {
	p.mu.RLock()
	preserved := map[string]Claim{}
	for key, c := range p.claims {
		if c.Owner != nil && p.pods[key] == nil && p.settled(key) {
			preserved[key] = c
		}
	}
	p.mu.RUnlock()
	maps.DeleteFunc(p.ownerRechecks, func(key string, _ ownerRecheck) bool { _, ok := preserved[key]; return !ok })

	for key, c := range preserved {
		r, seen := p.ownerRechecks[key]
		if seen && now.Before(r.at) {
			continue
		}
		namespace, _, _ := strings.Cut(key, "/")
		verdict, reason := destroyAuthorized(ctx, p.dyn, namespace, c.Owner)
		if verdict != authRelease {
			p.log.V(1).Info("preserved sandbox stays", "pod", key, "claim", c.ID, "reason", reason)
		} else if released, err := p.releasePreserved(ctx, key, c); err != nil {
			p.log.Error(err, "release of a preserved sandbox failed", "pod", key, "claim", c.ID)
		} else {
			if released {
				p.log.Info("released a preserved sandbox", "pod", key, "claim", c.ID, "reason", reason)
			}
			delete(p.ownerRechecks, key)
			continue
		}
		delay := base
		if seen {
			delay = min(2*r.delay, ownerRecheckMaxDelay)
		}
		p.ownerRechecks[key] = ownerRecheck{at: now.Add(delay), delay: delay}
	}
}

// releasePreserved releases a pod-less claim unless a Pod adopted it since the verdict; adoption is refused while the release is in flight.
func (p *Provider) releasePreserved(ctx context.Context, key string, c Claim) (bool, error) {
	p.mu.Lock()
	cur, held := p.claims[key]
	if !held || cur.ID != c.ID || p.pods[key] != nil {
		p.mu.Unlock()
		return false, nil
	}
	p.releasing[key] = struct{}{}
	p.mu.Unlock()
	if err := p.releaseClaim(ctx, key, c); err != nil {
		p.mu.Lock()
		delete(p.releasing, key)
		p.mu.Unlock()
		return false, err
	}
	return true, nil
}
