package provider

import (
	"context"
	"maps"
	"slices"
	"strings"
	"time"
)

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

// recheckOwners asks about each due preserved claim's owner with the delete verdicts, backing off from base, then drains the release queue.
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

	changed := false
	for key, c := range preserved {
		r, seen := p.ownerRechecks[key]
		if seen && now.Before(r.at) {
			continue
		}
		namespace, _, _ := strings.Cut(key, "/")
		verdict, reason := destroyAuthorized(ctx, p.dyn, namespace, c.Owner)
		if verdict == authRelease {
			if p.withdrawForRelease(key, c) {
				p.log.Info("releasing a preserved sandbox", "pod", key, "claim", c.ID, "reason", reason)
				changed = true
			}
			delete(p.ownerRechecks, key)
			continue
		}
		p.log.V(1).Info("preserved sandbox stays", "pod", key, "claim", c.ID, "reason", reason)
		delay := base
		if seen {
			delay = min(2*r.delay, ownerRecheckMaxDelay)
		}
		p.ownerRechecks[key] = ownerRecheck{at: now.Add(delay), delay: delay}
	}
	if p.drainReleases(ctx) || changed {
		p.saveState()
	}
}

// withdrawForRelease moves a pod-less claim off its key into the release queue, so a replacement Pod claims fresh instead of adopting a sandbox on its way out.
func (p *Provider) withdrawForRelease(key string, c Claim) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	cur, held := p.claims[key]
	if !held || cur.ID != c.ID || p.pods[key] != nil {
		return false
	}
	delete(p.claims, key)
	delete(p.quarantined, key)
	p.releasing = append(p.releasing, c)
	return true
}

// drainReleases releases every queued claim it can; a failed release stays queued for the next tick.
func (p *Provider) drainReleases(ctx context.Context) bool {
	p.mu.RLock()
	queued := slices.Clone(p.releasing)
	p.mu.RUnlock()
	released := false
	for _, c := range queued {
		if err := p.client.Release(ctx, c.ID, c.Token); err != nil {
			p.log.Error(err, "release of a preserved sandbox failed", "claim", c.ID)
			continue
		}
		p.mu.Lock()
		p.releasing = slices.DeleteFunc(p.releasing, func(q Claim) bool { return q.ID == c.ID })
		p.mu.Unlock()
		p.log.Info("released a preserved sandbox", "claim", c.ID)
		released = true
	}
	return released
}
