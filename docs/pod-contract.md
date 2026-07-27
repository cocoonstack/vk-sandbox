# Pod contract

A sandbox Pod reaches this provider only if the scheduler binds it to the
virtual node, and the provider serves it only if the Pod carries the right
annotations. Both halves are set by sandbox-operator's runtime mutator; this
page documents the contract so it can also be driven by hand.

## Routing a Pod to the virtual node

sandbox-operator's `podruntime` mutator runs when a `Sandbox` selects the
sandboxd runtime (`sandbox.cocoonstack.io/runtime: sandboxd`, or the
operator's default runtime mode). It:

- adds `nodeSelector: cocoon-sandbox.io/runtime=sandboxd`, the label
  vk-sandbox advertises by default (`--node-labels`);
- adds a toleration for `virtual-kubelet.io/provider` with operator `Exists`
  and effect `NoSchedule`, which covers both this node's taint and a
  co-located vk-cocoon node's;
- defaults the claim template annotation to the first container image when it
  is unset;
- rejects the Pod if `spec.nodeName` is pinned or the node selector already
  disagrees -- a misrouted sandbox is a failure, not a silent fallback.

A Pod that arrives here with `sandbox.cocoonstack.io/runtime` set to anything
other than `sandboxd` is rejected by `CreatePod`: this node serves exactly one
runtime.

## Annotations

`template` / `net` / `size` are the operator's `pkg/scale` selector keys
verbatim, so one contract spans the L2 claim gateway and this provider.

| Annotation | Direction | Meaning |
|---|---|---|
| `sandbox.cocoonstack.io/runtime` | in | Must be `sandboxd` (absent is treated as `sandboxd`) |
| `sandbox.cocoonstack.io/template` | in | sandboxd template axis. **Required** -- `CreatePod` fails without it |
| `sandbox.cocoonstack.io/net` | in | Claim network axis; empty means the sandboxd default |
| `sandbox.cocoonstack.io/size` | in | Claim VM size axis; empty means the sandboxd default |
| `sandbox.cocoonstack.io/ttl-seconds` | in | Claim lease in seconds. Absent means 86400 — sandboxd clamps to its 24h maximum — because a pod's sandbox lives until the pod is deleted, and sandboxd's own default of five minutes is sized for ephemeral SDK claims. An explicit `0` still selects that sandboxd default. A non-integer or negative value fails the create |
| `sandbox.cocoonstack.io/claim-id` | out | Written back by the provider: the sandboxd claim id backing the Pod |

The release token is deliberately **not** exposed on the Pod -- it stays in
the node's claims table, which is what keeps VM destruction an authorized,
node-local operation.

## What the Pod looks like once claimed

The provider pushes a synthetic status; nothing in it is reported by a
kubelet, because there is no container runtime on this node:

| Field | Value |
|---|---|
| `status.phase` | `Running` once claimed, `Pending` while the Pod is tracked without a claim |
| `status.podIP` / `podIPs` / `hostIP` | Host part of sandboxd's `owner_addr` -- the microVM's address |
| `status.conditions` | `Initialized`, `Ready`, `PodScheduled` all `True` |
| `status.containerStatuses[]` | One ready, running entry per `spec.containers` entry, with `imageID` = `sandboxd://<claim id>` |

Container specs are otherwise not interpreted: the workload runs inside the
microVM, not as containers on this node. `kubectl logs`, `exec`, `attach`, and
`port-forward` against these Pods are not served -- interactive access goes
through the sandbox SDK and preview URLs.

## Failure and churn semantics

| Situation | Behaviour |
|---|---|
| No warm capacity (sandboxd `429`, or a redirect to warm peers) | `CreatePod` fails typed; the Pod stays `Pending` and the operator's L1 path handles fallback. This provider never queues or retries into the node |
| Missing or invalid `template` / `ttl-seconds` | `CreatePod` fails; no claim is made |
| Pod deleted, owner `Sandbox` still alive | The claim is **preserved**; the VM keeps running |
| A replacement Pod with the same namespace/name | Adopts the preserved claim in place -- same VM, no second claim |
| Pod update | Recorded only; sandbox Pods are immutable at the runtime level |
| Owner `Sandbox` deleted | Release authorized; the microVM is destroyed |

The full decision table for the last two rows is in
[Architecture](architecture.md#delete-authorization-pod-deletion-is-not-vm-authority).

## Lease expiry

Nothing in the stack renews a lease — the e2b keepalive is record-keeping
only — so sandboxd's reaper destroys the microVM at the claim's deadline. The
provider records the deadline returned with each claim and, once it passes,
reports the Pod `Failed` with reason `SandboxLeaseExpired` instead of letting a
dead workload read as Running. The release credential stays valid either way; a
pod meant to outlive 24 hours needs a renewal mechanism in sandboxd first.
