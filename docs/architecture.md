# Architecture

vk-sandbox is a single flag-driven daemon: `main.go` wires a virtual-kubelet
node, and three packages carry the work.

| Package | Responsibility |
|---|---|
| `provider/` | The virtual-kubelet `PodLifecycleHandler`: claim on `CreatePod`, the delete-authorization contract on `DeletePod`, cache-fed status reads, the audit-only orphan scan, and the persisted claims table |
| `inventory/` | The node's L3 contribution: `LiveSource` (a `scale.NodeLiveSource` over sandboxd's index plus the claims table), `NodeInfoSource` (advertise address + warm-pool capacity), and the `Publisher` that server-side-applies one `NodeInventory` per node |
| `sandboxdx/` | The root-token sandboxd surfaces the operator's client does not cover: `GET /v1/sandboxes` (live index) and `GET /v1/info` (per-pool warm capacity) |

`Claim` and `Release` are **not** reimplemented here -- they come from
`github.com/cocoonstack/sandbox-operator/pkg/scale/sandboxd`, so the wire
contract has exactly one home. The same repo supplies `pkg/scale`'s selector
keys and the `InventoryApplier` used for the NodeInventory apply.

## The claim path

```
Pod bound to the virtual node
        |
        v
CreatePod
        |
        +-- runtime annotation != "sandboxd" ------------> error (wrong node)
        |
        +-- a claim already exists for <namespace>/<name>
        |        -> adopt in place: rebind the pod UID, no new VM
        |
        +-- otherwise: POST /v1/claim {template, net, size, ttl_seconds}
                 |
                 +-- error (incl. sandboxd 429 / redirect = no warm capacity)
                 |        -> CreatePod fails, the Pod stays Pending;
                 |           the operator's L1 path handles fallback
                 |
                 +-- ClaimResult {id, token, owner_addr}
                          |
                          v
                 claims[key] = {ID, Token, Address, PodUID}
                 persist the table, then notify the kubelet:
                   phase Running, PodIP/HostIP = host of owner_addr,
                   one synthetic ready container per spec container
                   (ImageID "sandboxd://<claim id>"),
                   annotation sandbox.cocoonstack.io/claim-id = <claim id>
```

`UpdatePod` records the newest Pod object and takes no VM action -- sandbox
Pods are immutable at the runtime level. `GetPod`, `GetPods`, and
`GetPodStatus` are served from the in-memory table, so no control-loop read
ever hits the apiserver or sandboxd.

The kubelet interactive surfaces -- `GetContainerLogs`, `RunInContainer`,
`AttachToContainer`, `PortForward` -- return `ErrNotImplemented` by design:
access to a sandbox goes through the sandbox SDK and preview URLs, not the
kubelet exec path. `GetStatsSummary` returns an empty summary and
`GetMetricsResource` no metric families.

## Delete authorization: pod deletion is not VM authority

A Node-NotReady taint eviction deletes every Pod on a node while the microVMs
keep serving users. `DeletePod` therefore never releases on the strength of
the deletion alone. It reads the Pod's **controller owner reference** through
the dynamic client and decides:

| Owner state | Verdict |
|---|---|
| No controller ownerReference (bare pod) | **release** -- the Pod is its own teardown intent |
| Structured `NotFound` whose `Details.Name` matches the owner | **release** -- owner confirmed gone |
| Owner exists with a non-zero `deletionTimestamp` | **release** -- owner in teardown |
| Owner exists with a different UID than the ownerReference | **release** -- the referenced generation is gone |
| Owner alive | preserve |
| 404 without `Details.Name` (endpoint shape unverified) | preserve |
| Any query error, or no dynamic client configured | preserve |

Preserve drops only the Pod entry; the claim -- and with it the release token
-- stays in the table so a same-name replacement Pod adopts the same VM. The
stale-UID guard runs first: a request carrying a previous Pod generation's UID
is ignored outright.

The owner GVR is derived from `apiVersion` + `kind` with the English plural
rules (`Sandbox` -> `sandboxes`, `policy` -> `policies`), never a naive
`+ "s"`. Even so, a wrong guess is safe: an endpoint-level 404 carries no
`Details.Name` and is read as "unverifiable", not "owner deleted".

On an authorized release, a sandboxd `404` is treated as convergence (already
gone); any other release error keeps the claim and the Pod entry so the
kubelet retries with the credential intact.

## Claims table persistence

The claims table (`{id, token, address, podUID, claimedAt}` per pod key) is
written to `--state-path` as JSON with a tmp-file + rename, mode `0600`,
directory mode `0700`. It is reloaded at startup, so a provider restart keeps
the authority to tear down exactly what it delivered. The binary requires the
flag; only the in-process constructor accepts an empty path, for tests.

A claim is durable before it counts: until its own write lands it is invisible
to status and to adoption, and a create whose write fails returns the sandbox
rather than reporting Running. At startup the reloaded table is checked against
the node's live sandboxes and rows the node no longer holds are dropped. A
failed listing is not an empty list, so an unreadable sandboxd drops nothing —
the rows are quarantined instead: still releasable, but not adoptable and not
Running until a later listing vouches for them. A verification loop, independent of the audit scan so
that switching the audit off cannot strand them, lifts the quarantine as soon
as a listing succeeds and then stops.

## Leases

Every claim carries a lease fixed at claim time — 24 hours by default, the
`ttl-seconds` annotation to override — and nothing in the stack renews one.
sandboxd's reaper destroys the VM at the deadline, so the provider records the
deadline returned with each claim (persisted with it) and a small watch pushes
the Pod `Failed` once it passes. Pushed, not polled: implementing `NotifyPods`
makes this an asynchronous provider, and virtual-kubelet installs no status
poller for those — a status that is not published does not exist. Adoption
refuses a row past its deadline; the fresh claim replaces it.

## Audit-only orphan scan

`--orphan-scan-interval` runs a background comparison of sandboxd's live index
against the claims table. It **reports only** -- background reconciliation
cannot prove user intent, so it never releases:

- sandboxd rows bound to no claim are logged as possible orphans and retained;
- claims whose sandbox is no longer listed (TTL reap, external release) are
  logged as stale.

A failed list is not an empty list: if `GET /v1/sandboxes` errors, the whole
cycle is skipped rather than treating "query failed" as "zero sandboxes".

## NodeInventory publishing (L3)

With `--publish-inventory`, a goroutine server-side-applies one `NodeInventory`
object named after the node, every `--publish-interval`, with field owner
`vk-sandbox`. That is the node's entire L3 write path -- one O(nodes) apply, no
per-sandbox etcd object, the same summarize-on-the-node pattern as
`metrics.k8s.io`.

```
sandboxd GET /v1/sandboxes ---+
  (authoritative live set)    |
                              +--> LiveSource --> entries[]
provider claims table --------+       (name = claim_ref, else sandboxd id;
  (address per claim id)              ID = sandboxd claim id;
                                      phase = Running | Hibernated;
                                      address from this node's own claim)

sandboxd GET /v1/info --------> NodeInfoSource --> {Address, Pools[]}
  (per-pool warm/target)                 advertise address is static config

              entries[] + Address + Pools[]
                          |
                          v
            NodeInventory (server-side apply, one per node)
                          |
                          v
        sandbox-operator aggregated apiserver serves
        `kubectl get sandboxes` from the summaries
```

Entries are read from sandboxd's index rather than the claims table, so
claims the aggregated apiserver made directly against sandboxd are published
too, and a claim whose VM is gone is simply not listed. Each entry carries
sandboxd's own claim id, which is the handle the node's release verb needs --
so a delete through the aggregated path releases exactly that microVM and
never resolves by Kubernetes name. Publish failures are logged, not fatal: the
next tick rebuilds from live state.

## Relation to the sibling repos

**[sandbox-operator](https://github.com/cocoonstack/sandbox-operator)** owns
the record plane -- the `agents.x-k8s.io` CRDs, warm pools, admission, the L1
claim fast path, and the L3 aggregated apiserver. Its runtime mutator is what
routes a sandbox Pod here (see [Pod contract](pod-contract.md)). vk-sandbox
owns the node transaction plane and imports the operator's `pkg/scale` for the
sandboxd client, the selector keys, and the inventory applier. The dependency
points one way: the operator does not import this repo.

**[sandbox](https://github.com/cocoonstack/sandbox)** ships sandboxd, the
node-local daemon holding the warm microVM pools this provider claims from.

**[vk-cocoon](https://github.com/cocoonstack/vk-cocoon)** is the sibling
virtual-kubelet for full cocoon MicroVM Pods. The two providers co-exist on
one physical node as **two distinct virtual nodes**: different node names,
different kubelet listen ports (vk-cocoon `:10250`, vk-sandbox `:10260`), and
different routing labels (`node.kubernetes.io/instance-type=virtual-node` for
vk-cocoon, `cocoon-sandbox.io/runtime=sandboxd` here). Both carry the
`virtual-kubelet.io/provider` taint, which the operator's shared `Exists`
toleration covers. vk-sandbox can reuse the co-located vk-cocoon kubelet
certificate when it is readable, and self-signs otherwise, so its API surface
is uniform either way. The delete-authorization and audit-only-GC contracts
implemented here are carried over from vk-cocoon.
