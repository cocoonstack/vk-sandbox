# Roadmap

Direction, not commitment — sequenced by current priority. Issues and PRs that
move any of these forward are welcome.

## Near term

- **Engine axis in `NodeInventory`.** sandboxd keys warm pools by
  `(template, net, size)` and boots every sandbox with the hypervisor cocoon
  is configured for on that node; once sandboxd grows a per-pool engine
  axis (`ch` | `fc`), publish it on inventory entries so the control-plane
  warm-pool driver can target mixed-engine pools.

## Medium term

- **Inventory as a stream.** Publish inventory deltas (watch-style) in
  addition to periodic server-side-apply summaries, so the aggregated
  apiserver can serve `watch` without re-list.
- **Serving-cert rotation.** Rotate the self-signed TLS material without a
  provider restart.

## Longer term

- **Optional interactive surfaces.** Evaluate serving kubelet `exec`/`logs`
  behind explicit opt-in flags for clusters that want kubectl-native access,
  without weakening the default no-serve posture.
- **Multi-sandboxd fronting.** One provider process fronting several sandboxd
  instances (per-tenant or per-engine daemons) on the same host.
