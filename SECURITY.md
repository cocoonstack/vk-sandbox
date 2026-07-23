# Security Policy

## Reporting a vulnerability

Please do **not** open public issues for suspected vulnerabilities.

Report privately through GitHub Security Advisories — "Report a vulnerability"
on this repository's Security tab. If that is not possible, contact the
maintainers listed in [MAINTAINERS.md](MAINTAINERS.md) directly.

You will receive an acknowledgment within 3 business days. We aim to ship a
fix, coordinated with the reporter, within 90 days of triage; credit is given
unless you prefer otherwise.

## Supported versions

Security fixes land on `master` and the most recent tagged release.

## Trust boundaries

Useful context when assessing impact:

- The provider talks to sandboxd with a bearer token read from
  `--sandboxd-token-file`; that token grants claim/release authority over the
  node's warm pools and must be protected like any node credential.
- The claim state file (`--state-path`, written 0600) persists sandbox ids and
  their release tokens across restarts. Disclosure of that file grants the
  authority to tear down exactly the sandboxes this node delivered — nothing
  more — but it is still a secret.
- Destroying a VM requires the owner `Sandbox` CR to be confirmed gone or in
  teardown; pod-level state alone is never authority. Bypasses of that
  contract are security-relevant even when no data is exposed.
- The kubelet exec/logs/port-forward surfaces are intentionally not served;
  a change that opens them should be treated as an attack-surface change and
  reviewed as such.
