# vk-sandbox

A [virtual-kubelet](https://github.com/virtual-kubelet/virtual-kubelet)
provider that serves Kubernetes agent-sandbox semantics (`agents.x-k8s.io`,
driven by [kubernetes-sigs/agent-sandbox](https://github.com/kubernetes-sigs/agent-sandbox))
from **sandboxd** -- the node-local hot-sandbox daemon of
[cocoonstack/sandbox](https://github.com/cocoonstack/sandbox).

One virtual node fronts one sandboxd. A sandbox Pod scheduled to that node
becomes a claim against the node's warm microVM pool; the Pod's IP is the
host part of sandboxd's `owner_addr`; deleting the owning `Sandbox` CR releases
the VM, and so does deleting a bare Pod (no controller owner), owner expiry,
teardown, or owner UID replacement. Kubernetes stays the record-of-intent
and policy plane, and the claim transaction runs entirely on the node.

```
Kubernetes control plane                    cocoon node
+-----------------------------+     +--------------------------------------+
| Sandbox CR (agents.x-k8s.io)|     |                                      |
|            |                |     |   +------------------------------+   |
|            v                |     |   | vk-sandbox                   |   |
| agent-sandbox controller    |     |   |  virtual node, kubelet API   |   |
|  creates the Pod from a     |     |   |  :10260                      |   |
|  template carrying runtime: |     |   +------------------------------+   |
|  sandboxd + selector/tolera.|     |     |  CreatePod       ^ status      |
+-----------------------------+     |     v                 |             |
            |  scheduler binds      |   POST /v1/claim   id/token/addr     |
            |  the Pod to the       |     |                 |             |
            |  virtual node         |     v                 |             |
            +---------------------->|   +------------------------------+   |
                                    |   | sandboxd hot pool            |   |
      NodeInventory (server-side    |   |  warm microVMs, node-local   |   |
      apply, one object per node) <-|---+------------------------------+   |
            |                       +--------------------------------------+
            v
  sandbox-operator aggregated apiserver (L3 read path)
```

## Guides

- [Architecture](architecture.md) -- the claim path, the
  delete-authorization contract, the audit-only orphan scan, NodeInventory
  publishing, and how this provider relates to vk-cocoon and sandbox-operator
- [Installation](installation.md) -- building the binary or image, RBAC,
  and running the virtual node as a systemd service or in-cluster
- [Configuration](configuration.md) -- every flag and environment variable
  the binary accepts, plus the claims state file
- [Pod contract](pod-contract.md) -- the annotations, node labels, and taint
  that route a sandbox Pod here and what the provider writes back
- [Security model](security.md) -- trust boundaries and how to report a
  vulnerability
- [Roadmap](roadmap.md) -- what comes next, by priority

## Repository

Source and issue tracker:
[github.com/cocoonstack/vk-sandbox](https://github.com/cocoonstack/vk-sandbox).
Part of the [cocoonstack](https://cocoonstack.github.io/) MicroVM platform.
