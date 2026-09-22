# Installation

vk-sandbox runs **one process per sandboxd**, on the node that sandboxd runs
on, and registers itself as a virtual node in the cluster. Nothing is
installed cluster-wide except the RBAC and the service account.

Prerequisites:

- a node already running [sandboxd](https://github.com/cocoonstack/sandbox)
  with warm pools configured, reachable from the node (default
  `http://127.0.0.1:7777`), and its node API token;
- credentials that reach the apiserver -- either an in-cluster service account
  or a kubeconfig on the node;
- [kubernetes-sigs/agent-sandbox](https://github.com/kubernetes-sigs/agent-sandbox)
  installed, so the `agents.x-k8s.io` CRDs and the controller exist; a
  SandboxTemplate routes its Pods here by carrying the
  [Pod contract](pod-contract.md). Install
  [sandbox-operator](https://github.com/cocoonstack/sandbox-operator) as well
  when the L3 aggregated apiserver should read this node's `NodeInventory`
  (`--publish-inventory` needs its `nodeinventories.sandbox.cocoonstack.io` CRD).

## Prebuilt binary

Release archives are Linux `amd64`/`arm64` and ship the systemd unit and the
environment template alongside the binary:

```bash
ARCH=$([ "$(uname -m)" = "aarch64" ] && echo arm64 || echo x86_64)
curl -sL "https://github.com/cocoonstack/vk-sandbox/releases/latest/download/vk-sandbox_Linux_${ARCH}.tar.gz" | tar xz
sudo install -m 0755 vk-sandbox /usr/local/bin/
```

## Build from source

```bash
git clone https://github.com/cocoonstack/vk-sandbox.git
cd vk-sandbox
make build                 # -> ./vk-sandbox
make build-linux           # -> ./vk-sandbox-linux-amd64 (static, CGO off)
sudo install -m 0755 ./vk-sandbox /usr/local/bin/
```

A container image builds straight from the repo root; the image is a
distroless static base with the binary as its entrypoint:

```bash
docker build -t vk-sandbox:dev .
```

## Cluster RBAC

Apply the shipped RBAC once per cluster. It creates the `vk-sandbox` service
account in `sandbox-system` and grants:

- the virtual-kubelet essentials -- nodes and `nodes/status`, pods and
  `pods/status`, including Pod deletion to complete termination, events,
  leases, plus read access to secrets, configmaps, and services;
- `get` on the owner CR, the read every controller-owned release is decided from;
- write access to `nodeinventories` for `--publish-inventory`.

```bash
kubectl create namespace sandbox-system
kubectl apply -f manifests/rbac.yaml
```

## Run it: systemd on the sandboxd node

The `packaging/` directory ships a unit and an environment template. The unit
starts after `sandboxd.service`, refuses to start without a kubeconfig and an
env file, runs from `/var/lib/vk-sandbox`, and always restarts after 5s.

```bash
sudo install -d -m 0700 /etc/vk-sandbox /var/lib/vk-sandbox
sudo install -m 0644 packaging/env.example /etc/vk-sandbox/env
sudo install -m 0644 packaging/*.service /etc/systemd/system/vk-sandbox.service
```

Edit `/etc/vk-sandbox/env` -- at minimum the node name and node IP:

```sh
VK_NODE_NAME=<hostname>-sandboxd
VK_NODE_IP=<host-internal-ip>
VK_LISTEN_ADDR=:10260
SANDBOXD_URL=http://127.0.0.1:7777
SANDBOXD_TOKEN_FILE=/etc/vk-sandbox/sandboxd-token
VK_STATE_PATH=/var/lib/vk-sandbox/claims.json
```

`VK_NODE_NAME` must differ from the physical Kubernetes node and any
co-located vk-cocoon node. `VK_LISTEN_ADDR` must not conflict with the host
kubelet or vk-cocoon's `:10250`.

Install the sandboxd node token and the kubeconfig the unit expects:

```bash
printf '%s' '<sandboxd-node-token>' | sudo tee /etc/vk-sandbox/sandboxd-token >/dev/null
sudo chmod 0600 /etc/vk-sandbox/sandboxd-token
# the unit refuses to start without KUBECONFIG=/etc/cocoon/kubeconfig
sudo test -r /etc/cocoon/kubeconfig
```

The unit also points `VK_TLS_CERT` / `VK_TLS_KEY` at the co-located vk-cocoon
kubelet certificate. Those paths are optional -- when they are absent the
process self-signs an in-memory certificate (see
[Configuration](configuration.md#kubelet-api-tls)).

Start it:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now vk-sandbox
journalctl -u vk-sandbox -f
```

To publish this node's inventory to the operator's aggregated apiserver, add
the flag to the unit's `ExecStart` (or run the binary directly):

```bash
/usr/local/bin/vk-sandbox --publish-inventory --sandboxd-advertise-addr "<node-address>:7777"
```

Replace `<node-address>` with an address reachable from the operator. The
default loopback claim URL is only reachable on the sandboxd host.

## Run it: in-cluster

The binary prefers in-cluster configuration, so the same image can run as a
DaemonSet on the sandboxd nodes with `serviceAccountName: vk-sandbox`,
`hostNetwork: true` (so `SANDBOXD_URL` can stay on loopback), the sandboxd
token mounted from a Secret, and `VK_STATE_PATH` on a `hostPath` volume so the
release credentials survive a Pod restart. Derive a distinct virtual node
name from the physical node; assigning `spec.nodeName` directly would make
virtual-kubelet update the real node. Use these container environment entries
in this order so Kubernetes expands the physical name before adding the suffix:

```yaml
env:
  - name: VK_HOST_NODE_NAME
    valueFrom:
      fieldRef:
        fieldPath: spec.nodeName
  - name: VK_NODE_NAME
    value: "$(VK_HOST_NODE_NAME)-sandboxd"
```

## Verify

```bash
kubectl get node <VK_NODE_NAME> -o wide
kubectl describe node <VK_NODE_NAME> | grep -E 'Taints|type=|sandbox.cocoonstack.io/runtime'
```

The node should be `Ready`, labelled `type=virtual-kubelet` and
`sandbox.cocoonstack.io/runtime=sandboxd`, and tainted
`virtual-kubelet.io/provider=sandboxd:NoSchedule`. With
`--publish-inventory`, one object named after the node appears:

```bash
kubectl get nodeinventories.sandbox.cocoonstack.io <VK_NODE_NAME> -o yaml
```

Then create a `Sandbox` with the sandboxd runtime and watch the Pod go
`Running` with the host part of sandboxd's `owner_addr` as its Pod IP -- see
[Pod contract](pod-contract.md).

## Development

```bash
make all        # deps fmt lint test build
make race lint
```
