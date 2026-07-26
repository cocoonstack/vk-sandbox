# Contributing

Thanks for your interest in improving vk-sandbox!

## Before you start

- Read the [Code of Conduct](CODE_OF_CONDUCT.md).
- For anything beyond a small fix, open an issue first so we can agree on the
  direction before you invest in an implementation.

## Developer setup

Go 1.26+ is required.

```bash
make all          # fmt-check vet test build
make race         # unit tests with the race detector
make lint         # golangci-lint when installed
make build-linux  # cross-compile the node binary
```

## Pull requests

- Keep changes focused; unrelated refactors belong in their own PR.
- Commit messages: a one-line summary, optionally followed by a body that
  explains *why* the change is needed.
- The six provider contracts in the README (delete authorization, GVR
  derivation, audit-only orphan GC, stale-UID guard, L0 API hygiene, durable
  release credentials) are load-bearing and pinned by intent tests. Do not
  weaken those tests to make a change pass — if a contract must change, argue
  it in the PR description first.
- CI must be green.

## Developer Certificate of Origin

Contributions are accepted under the
[Developer Certificate of Origin](https://developercertificate.org/). Sign off
your commits (`git commit -s`) to certify that you have the right to submit
the work under this repository's license.

## License

By contributing you agree that your contributions are licensed under
[Apache-2.0](LICENSE).
