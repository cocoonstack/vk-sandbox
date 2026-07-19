// Copyright 2026 The CocoonStack Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package provider

import (
	"context"
	"time"
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
