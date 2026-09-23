# Security model

## Trust boundaries

- The provider talks to sandboxd with a bearer token read from
  `--sandboxd-token-file`; that token grants claim and release authority over
  the node's warm pools and is protected like any node credential.
- The claims state file (`--state-path`, written 0600) persists sandbox ids and
  their release tokens across restarts. Disclosure grants the authority to tear
  down exactly the sandboxes this node delivered — nothing more — but it is a
  secret.
- Releasing a controller-owned Pod's VM requires its owner to be confirmed
  gone, replaced, deleting, or expired; a bare Pod authorizes its own teardown.
  See [delete authorization](architecture.md#delete-authorization-pod-deletion-is-not-vm-authority).
- The kubelet exec/logs/port-forward surfaces are not served; a change that
  opens them is an attack-surface change and is reviewed as such.

## Reporting a vulnerability

Do not open a public issue. Report privately through GitHub Security
Advisories — "Report a vulnerability" on the repository's Security tab. Fixes
land on `master` and the most recent tagged release.
