# Architecture

vk-sandbox is a single flag-driven daemon: `main.go` wires a virtual-kubelet
node, and two packages carry the work.

| Package | Responsibility |
|---|---|
| `provider/` | The virtual-kubelet `PodLifecycleHandler`: claim on `CreatePod`, the delete-authorization contract on `DeletePod`, cache-fed status reads, the audit-only orphan scan, and the persisted claims table |
| `inventory/` | The node's L3 contribution: `LiveSource` (a `scale.NodeLiveSource` over sandboxd's index plus the claims table), `NodeInfoSource` (advertise address + warm-pool capacity), and the `Publisher` that server-side-applies one `NodeInventory` per node |

Nothing on the sandboxd wire is reimplemented here -- `Claim`, `Release`, the
live index (`Sandboxes`) and warm-pool capacity (`Info`) all come from
`github.com/cocoonstack/sandbox-operator/pkg/sandboxd`, so the wire
contract has exactly one home. The same repo supplies `pkg/scale`'s selector
keys and the `InventoryApplier` used for the NodeInventory apply.

## The contracts this provider keeps

The load-bearing rules, carried over from the production vk-cocoon provider and
pinned by intent tests:

1. **Pod deletion is not VM authority.** Node-NotReady taint evictions delete
   every pod on a node while the VMs keep serving users. For a controller-owned
   Pod, `DeletePod` releases only when the owning `Sandbox` is **confirmed gone** (a
   structured NotFound naming it in `Details.Name`), replaced by another UID,
   **in teardown** (deletionTimestamp set), or **expired** (Ready reason
   `SandboxExpired`). Otherwise the claim is preserved, and a
   same-name replacement pod **adopts it in place** — no second claim, same VM. See
   [delete authorization](#delete-authorization-pod-deletion-is-not-vm-authority).
2. **No naive kind pluralization.** The owner GVR is derived with the es/ies
   rules (`Sandbox`→`sandboxes`); an endpoint-level 404 *without*
   `Details.Name` is treated as "GVR guess wrong", never as "owner deleted".
   (A naive `+"s"` once destroyed a live-owner VM.)
3. **Audit-only orphan GC.** Background reconciliation can't prove user
   intent, so the orphan scan only reports; it never releases. A failed
   sandboxd list is **not** an empty list — the cycle is skipped. See
   [audit-only orphan scan](#audit-only-orphan-scan).
4. **Stale-UID guard.** Lifecycle requests carrying a previous pod
   generation's UID are ignored.
5. **L0 API hygiene.** Status reads are served from the provider's own table;
   no control-loop LIST hits the apiserver.
6. **Lease expiry is published, never discovered.** The node grants a lease at
   claim time and its archive lifecycle may rewrite it. After the cached
   deadline, the watcher refreshes a still-listed claim or publishes `Failed`
   on confirmed absence; listing errors defer the decision. virtual-kubelet
   never polls an asynchronous provider. See [leases](#leases).
7. **Release credentials survive restarts.** The claim table (sandbox id +
   release token) persists to a 0600 state file; a provider restart keeps the
   authority to tear down exactly what it delivered. See
   [claims table persistence](#claims-table-persistence).

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
                 claims[key] = {ID, Token, Address, PodUID, ClaimedAt, Deadline}
                 persist the table, then notify the kubelet:
                   phase Running, PodIP/HostIP = host of owner_addr,
                   one synthetic ready container per spec container
                   (ImageID "sandboxd://<claim id>"),
                   annotation sandbox.cocoonstack.io/claim-id = <claim id>
```

`UpdatePod` retries creation for a tentative claim whose persistence and
compensating release failed. Otherwise it records the newest Pod object --
running sandbox Pods are immutable at the runtime level. `GetPod`, `GetPods`, and
`GetPodStatus` are served from the in-memory table, so no control-loop read
ever hits the apiserver or sandboxd.

The kubelet interactive surfaces -- `GetContainerLogs`, `RunInContainer`,
`AttachToContainer`, `PortForward` -- return `ErrNotImplemented` by design:
access to a sandbox goes through the sandbox SDK and preview URLs, not the
kubelet exec path. `GetStatsSummary` returns an empty summary and
`GetMetricsResource` no metric families.

## Delete authorization: pod deletion is not VM authority

A Node-NotReady taint eviction deletes every Pod on a node while the microVMs
keep serving users. For a controller-owned Pod, `DeletePod` reads its owner
through the dynamic client before deciding; a bare Pod is its own authority:

| Owner state | Verdict |
|---|---|
| No controller ownerReference (bare pod) | **release** -- the Pod is its own teardown intent |
| Structured `NotFound` whose `Details.Name` matches the owner | **release** -- owner confirmed gone |
| Owner exists with a non-zero `deletionTimestamp` | **release** -- owner in teardown |
| Owner exists with a different UID than the ownerReference | **release** -- the referenced generation is gone |
| Owner `Sandbox` alive but expired (Ready reason `SandboxExpired`) | **release** -- the operator tore its workload down and no replacement Pod comes |
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

The claims table (`{id, token, address, podUID, claimedAt, deadline}` per pod
key) is written to `--state-path` as JSON with a tmp-file + rename, mode
`0600`, directory mode `0700`. It is reloaded at startup, so a provider
restart keeps the authority to tear down exactly what it delivered. The
binary requires the flag; only the in-process constructor accepts an empty
path, for tests.

A claim is durable before it counts: until its own write lands it is invisible
to Running status and to adoption. A create whose write fails tries to return
the sandbox; failed compensation retains the credential for update retries
or deletion (see [Pod contract](pod-contract.md#failure-and-churn-semantics)).
At startup the reloaded table is checked against
the node's live sandboxes and rows the node no longer holds are dropped. A
failed listing is not an empty list, so an unreadable sandboxd drops nothing —
the rows are quarantined instead: still releasable, but not adoptable and not
Running until a later listing vouches for them. A verification loop, independent of the audit scan so
that switching the audit off cannot strand them, lifts the quarantine as soon
as a listing succeeds and then stops.

## Leases

Ordinary claims request a 24-hour lease by default, with `ttl-seconds` to
override it, and have no automatic renewal. The provider persists the returned
deadline and watches for expiry. It pushes terminal status through `NotifyPods`;
virtual-kubelet installs no status poller for an asynchronous provider.

The cached deadline is not authoritative: the archive lifecycle rewrites a
claim's lease on the node. `Failed` is terminal, so before publishing it — and
before replacing a past-deadline row — the node's listing confirms the sandbox
is really gone; a still-listed claim just has its lease refreshed from the
listing, and an unlistable node defers rather than declares. The publication
itself re-checks identity: if the key changed hands behind the listing (old
Pod deleted, successor claimed fresh), the expiry belonged to the old claim
and nothing is published against the new one.

## Audit-only orphan scan

`--orphan-scan-interval` runs a background comparison of sandboxd's live index
against the claims table. It **reports only** -- background reconciliation
cannot prove user intent, so it never releases:

- sandboxd rows bound to no local claim and carrying no `claim_ref` are logged
  as possible orphans and retained; a non-empty `claim_ref` marks an
  apiserver-direct claim and is recorded as `external`, not an orphan;
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
                                      deadline = the row's lease end;
                                      claimedAt = the row's first grant;
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

**[kubernetes-sigs/agent-sandbox](https://github.com/kubernetes-sigs/agent-sandbox)**
owns the record plane -- the `agents.x-k8s.io` CRDs, warm pools, claims, and
the controller that turns a Sandbox into a Pod. A SandboxTemplate routes that
Pod here by carrying the [Pod contract](pod-contract.md) itself.
**[sandbox-operator](https://github.com/cocoonstack/sandbox-operator)** owns
the L3 aggregated apiserver and the `NodeInventory` CRD
(`sandbox.cocoonstack.io`) this provider publishes into. vk-sandbox owns the
node transaction plane and imports the operator's `pkg/sandboxd` for the
client and `pkg/scale` for selector keys and the inventory applier. The
dependency points one way: the operator does not import this repo.

**[sandbox](https://github.com/cocoonstack/sandbox)** ships sandboxd, the
node-local daemon holding the warm microVM pools this provider claims from.

**[vk-cocoon](https://github.com/cocoonstack/vk-cocoon)** is the sibling
virtual-kubelet for full cocoon MicroVM Pods. The two providers co-exist on
one physical node as **two distinct virtual nodes**: different node names,
different kubelet listen ports (vk-cocoon `:10250`, vk-sandbox `:10260`), and
different routing labels (`node.kubernetes.io/instance-type=virtual-node` for
vk-cocoon, `sandbox.cocoonstack.io/runtime=sandboxd` here). Both carry the
`virtual-kubelet.io/provider` taint, which the operator's shared `Exists`
toleration covers. vk-sandbox can reuse the co-located vk-cocoon kubelet
certificate when it is readable, and self-signs otherwise, so its API surface
is uniform either way. The delete-authorization and audit-only-GC contracts
implemented here are carried over from vk-cocoon.
