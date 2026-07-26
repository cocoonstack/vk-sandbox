# Configuration

vk-sandbox is a flag-driven daemon with no configuration file. Most flags
take their default from an environment variable, so the systemd unit can be
driven entirely from `EnvironmentFile`; an explicit flag always wins over the
environment.

## Flags

| Flag | Environment | Default | Description |
|---|---|---|---|
| `--node-name` | `VK_NODE_NAME` | `vk-sandboxd` | Virtual node name. Must be distinct from a co-located vk-cocoon node |
| `--node-ip` | `VK_NODE_IP` | none | Node `InternalIP` advertised to the apiserver. Omitted from node addresses when empty |
| `--listen-addr` | `VK_LISTEN_ADDR` | `:10260` | Kubelet API listen address. Must differ from a co-located vk-cocoon, which uses `:10250` |
| `--tls-cert` | `VK_TLS_CERT` | none | Kubelet API TLS certificate |
| `--tls-key` | `VK_TLS_KEY` | none | Kubelet API TLS key |
| `--node-cpu` | `VK_NODE_CPU` | `4000` | Advertised node CPU capacity (a scheduling budget only) |
| `--node-memory` | `VK_NODE_MEMORY` | `8Ti` | Advertised node memory capacity |
| `--node-pods` | `VK_NODE_PODS` | `2000` | Advertised node max pods |
| `--sandboxd-url` | `SANDBOXD_URL` | `http://127.0.0.1:7777` | sandboxd base URL |
| `--sandboxd-advertise-addr` | `SANDBOXD_ADVERTISE_ADDR` | host:port of `--sandboxd-url` | `host:port` published in `NodeInventory` for claim routing |
| `--sandboxd-token-file` | `SANDBOXD_TOKEN_FILE` | none | File holding the sandboxd node API token (trailing whitespace trimmed) |
| `--state-path` | `VK_STATE_PATH` | `/var/lib/vk-sandbox/claims.json` | Claims table persistence path; empty disables persistence |
| `--orphan-scan-interval` | -- | `60s` | Audit-only orphan scan cadence; `0` disables the scan |
| `--publish-inventory` | -- | `false` | Server-side-apply this node's `NodeInventory` for the L3 aggregation layer |
| `--publish-interval` | -- | `30s` | `NodeInventory` publish cadence |
| `--node-labels` | -- | `cocoon-sandbox.io/runtime=sandboxd` | Comma-separated extra node labels, `key=value` |

`KUBECONFIG` is read only when in-cluster configuration is unavailable: the
binary tries the in-cluster service-account config first and falls back to the
kubeconfig at `$KUBECONFIG`.

The token file is read once at startup, so rotating the sandboxd token
requires a restart.

## Advertised capacity

`--node-cpu` / `--node-memory` / `--node-pods` are a **scheduling budget**,
not a measurement: the Pod is a placeholder and sandboxd holds the real
microVM. They still have to be set to something, because a node advertising
zero allocatable has every sandbox Pod rejected by the scheduler. Values are
parsed as Kubernetes quantities, so `4000`, `8Ti`, `2000` are all valid.

## Node identity

Beyond the labels from `--node-labels`, the node always advertises:

| Field | Value |
|---|---|
| Label `type` | `virtual-kubelet` |
| Taint | `virtual-kubelet.io/provider=sandboxd:NoSchedule` |
| Addresses | `InternalIP` = `--node-ip` (when set), then `Hostname` = `--node-name` |
| `DaemonEndpoints.kubeletEndpoint.port` | the port parsed out of `--listen-addr` |

The taint is what keeps ordinary workloads off the virtual node; the operator
adds the matching toleration to the sandbox Pods it routes here. See
[Pod contract](pod-contract.md).

## Kubelet API TLS

virtual-kubelet serves the kubelet API over TLS only, so a certificate always
exists:

- if `--tls-cert` and `--tls-key` are both set **and** both point at readable
  regular files, that key pair is loaded;
- otherwise the process self-signs an in-memory P-256 certificate covering
  `--node-name`, `--node-ip`, and `127.0.0.1`.

Self-signing keeps every node's API surface uniform without depending on a
co-located vk-cocoon's certificate; the apiserver connects to kubelet-served
routes with `InsecureSkipTLSVerify`, and for sandboxes those routes are
stubbed regardless. Client certificates are not requested
(`tls.NoClientCert`), and the minimum version is TLS 1.2.

## Claims state file

`--state-path` holds the durable claim table -- the release credentials that
make this provider the only party able to destroy the VMs it delivered. It is
written compactly (shown expanded here) because every claim rewrites it:

```json
{
  "claims": {
    "default/my-sandbox": {
      "id": "sb_...",
      "token": "...",
      "address": "10.0.0.5:7777",
      "podUID": "6f1c...",
      "claimedAt": "2026-07-27T02:15:00Z"
    }
  }
}
```

| Field | Description |
|---|---|
| `id` | sandboxd claim id |
| `token` | Release credential; never leaves the node |
| `address` | sandboxd `owner_addr` for the claim (`host:port`); the host becomes the Pod IP |
| `podUID` | UID of the Pod currently bound to the claim (the stale-request guard) |
| `claimedAt` | When the claim was taken; reported as the Pod start time, which must not move between reads. A table written before this field existed gets it filled in on the next load |

It is written with a tmp-file + atomic rename at mode `0600`, its directory
created at `0700`, and reloaded on startup. Concurrent Pod creates serialize
their writes, so the file always reflects the newest table rather than whichever
snapshot happened to land last. **Treat it as a secret** -- it
contains release tokens. A missing file is not an error; a malformed one is
fatal at startup rather than silently discarding live authority.

## RBAC

The permissions the flags imply are shipped in
[`manifests/rbac.yaml`](https://github.com/cocoonstack/vk-sandbox/blob/master/manifests/rbac.yaml):
the usual virtual-kubelet node/pod/lease/event access, plus `get` on the
owner CR for the delete-authorization read, plus write access to
`nodeinventories` when `--publish-inventory` is on. See
[Installation](installation.md).
