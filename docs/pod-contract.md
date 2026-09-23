# Pod contract

A sandbox Pod reaches this provider only if the scheduler binds it to the
virtual node, and the provider serves it only if the Pod carries the right
annotations. Both halves are written by the SandboxTemplate author; the
agent-sandbox controller copies the pod template into the Pod verbatim.

## Routing a Pod to the virtual node

The pod template of a `SandboxTemplate` (or a bare `Sandbox`) carries:

- `nodeSelector: sandbox.cocoonstack.io/runtime=sandboxd`, the label
  vk-sandbox advertises by default (`--node-labels`);
- a toleration for `virtual-kubelet.io/provider` with operator `Exists`
  and effect `NoSchedule`, which covers both this node's taint and a
  co-located vk-cocoon node's;
- the annotations below; `template` is the claim axis and has no default.

```yaml
apiVersion: extensions.agents.x-k8s.io/v1beta1
kind: SandboxTemplate
metadata: {name: sandboxd, namespace: default}
spec:
  podTemplate:
    metadata:
      annotations:
        sandbox.cocoonstack.io/runtime: sandboxd
        sandbox.cocoonstack.io/template: ghcr.io/cocoonstack/sandbox/rt:24.04
        sandbox.cocoonstack.io/net: none
        sandbox.cocoonstack.io/size: small
    spec:
      nodeSelector: {sandbox.cocoonstack.io/runtime: sandboxd}
      tolerations:
      - {key: virtual-kubelet.io/provider, operator: Exists, effect: NoSchedule}
      containers:
      - name: agent
        image: ghcr.io/cocoonstack/sandbox/rt:24.04
```

A pinned `spec.nodeName` or a disagreeing node selector is not corrected: the
scheduler binds the Pod elsewhere, and a Pod that does arrive here with the
wrong annotations fails `CreatePod` rather than falling back silently.

A Pod that arrives here with `sandbox.cocoonstack.io/runtime` set to anything
other than `sandboxd` is rejected by `CreatePod`: this node serves exactly one
runtime.

## Annotations

`template` / `net` / `size` are the operator's `pkg/scale` selector keys
verbatim, so one contract spans the aggregated apiserver's direct claims and
this provider.

| Annotation | Direction | Meaning |
|---|---|---|
| `sandbox.cocoonstack.io/runtime` | in | Must be `sandboxd` (absent is treated as `sandboxd`) |
| `sandbox.cocoonstack.io/template` | in | sandboxd template axis. **Required** -- `CreatePod` fails without it |
| `sandbox.cocoonstack.io/net` | in | Claim network axis (`none` or an egress lane); empty means the sandboxd default |
| `sandbox.cocoonstack.io/size` | in | Claim VM size axis (`small`/`medium`/`large`); empty means the sandboxd default |
| `sandbox.cocoonstack.io/ttl-seconds` | in | Claim lease in seconds. Absent means 86400, sandboxd's 24h maximum for ordinary claims. An explicit `0` selects sandboxd's five-minute default for ephemeral SDK claims. A non-integer or negative value fails the create |
| `sandbox.cocoonstack.io/claim-id` | out | Published by the provider on the status push: the sandboxd claim id backing the Pod. The provider's own Pod view may drop it after a resync |

The release token is deliberately **not** exposed on the Pod -- it stays in
the node's claims table, which is what keeps VM destruction an authorized,
node-local operation.

## What the Pod looks like once claimed

The provider pushes a synthetic status; nothing in it is reported by a
kubelet, because there is no container runtime on this node:

| Field | Value |
|---|---|
| `status.phase` | `Running` once claimed, `Pending` while the Pod is tracked without a claim |
| `status.podIP` / `podIPs` / `hostIP` | Host part of sandboxd's `owner_addr`, the address serving the claim |
| `status.conditions` | `Initialized`, `Ready`, `PodScheduled` all `True` |
| `status.containerStatuses[]` | One ready, running entry per `spec.containers` entry, with `imageID` = `sandboxd://<claim id>` |

Container specs are otherwise not interpreted: the workload runs inside the
microVM, not as containers on this node. `kubectl logs`, `exec`, `attach`, and
`port-forward` against these Pods are not served -- interactive access goes
through the sandbox SDK and preview URLs.

## Failure and churn semantics

| Situation | Behaviour |
|---|---|
| No warm capacity (sandboxd `429`, or a redirect to warm peers) | `CreatePod` fails typed; the Pod stays `Pending` and virtual-kubelet retries the create with backoff. Under `restartPolicy: Never` virtual-kubelet marks the Pod `Failed` instead and stops retrying. This provider never queues or retries into the node itself |
| Missing or invalid `template` / `ttl-seconds` | `CreatePod` fails; no claim is made |
| Pod deleted, owner `Sandbox` still alive | The claim is **preserved**; the VM keeps running. The owner is re-checked with backoff, and once it is gone, in teardown, expired or replaced the VM is released |
| Pod deleted, owner `Sandbox` expired (Ready reason `SandboxExpired`) | Release authorized: the operator tore the workload down and no replacement Pod comes |
| A replacement Pod with the same namespace/name | Adopts the preserved claim in place -- same VM, no second claim |
| Pod update | Retries a tentative claim left by a failed create; otherwise records metadata only, since running sandbox Pods are immutable at the runtime level |
| Owner `Sandbox` deleted | Release authorized; the microVM is destroyed |

The full delete-authorization decision table is in
[Architecture](architecture.md#delete-authorization-pod-deletion-is-not-vm-authority).

If claim persistence and its compensating release both fail, the provider
retains the Pod and release credential. Its cached claim-id annotation differs
from the Kubernetes Pod, so virtual-kubelet calls `UpdatePod` on its next
resync. That path returns the stranded claim before creating a replacement;
`DeletePod` finds the retained claim by its pod key. The retry rides the
resync, which stops for a Pod that has gone `Failed`, so a template with
`restartPolicy: Never` does not get it.

## Lost claim responses

sandboxd persists a claim before writing the HTTP response, so a transport
failure can leave a live sandbox whose token nobody received. sandboxd is on
loopback, making that a process-death-class rarity, and the stray is bounded
by its lease — or by the archive retention policy, where an archive-enabled
pool may keep it longer — and logged by the orphan scan — so there is deliberately no
recovery machinery for it. The claim still carries the pod key as `claim_ref`,
which is what lets an operator trace such a stray to the Pod it was for.

## Lease expiry

The provider records the deadline returned with each claim. Once it passes,
the lease watcher checks sandboxd: a listed sandbox refreshes its cached
deadline, confirmed absence pushes `Failed` with reason `SandboxLeaseExpired`,
and a failed listing defers the decision. This permits archive retention to
change the lease without falsely reporting a live claim as gone.

Ordinary claims are capped at 24 hours and have no automatic renewal; the e2b
keepalive is record-keeping only. The provider pushes status because
virtual-kubelet never polls an asynchronous provider. The release credential
remains available for authorized teardown.
