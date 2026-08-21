# Nano1 ARM64 build and inference node

Nano1 is an NVIDIA Jetson Orin Nano Super used for native ARM64 builds and
GPU-only inference. It is separate from the NAS coordinator and the krick-1
Intel GPU workers.

The host currently provides:

- Ubuntu 24.04 on `aarch64`;
- JetPack 7.2 with CUDA 13.2 and TensorRT 10.16;
- NVIDIA container runtime;
- Docker Engine, Buildx, and Compose;
- 8 GB shared system and GPU memory; and
- a GitHub Actions runner labeled `nano1`, `jetson`, and `orin-nano`.

The runner service is
`actions.runner.ai-pipestream-protomolt.nano1.service`. It runs as the
dedicated `protomolt-runner` user. Check it locally with:

```shell
sudo systemctl status actions.runner.ai-pipestream-protomolt.nano1.service
sudo journalctl -u actions.runner.ai-pipestream-protomolt.nano1.service -n 100
```

The runner user belongs to `docker`, `video`, and `render`. The device groups
allow the host verifier to inspect the Jetson GPU. They do not automatically
pass GPU devices into a container.

## Native ARM64 build proof

The `Nano1 ARM64 Smoke` GitHub workflow is manual-only. It verifies the host,
builds the agent-host distribution, builds one coding worker image natively,
and checks the resulting image architecture and toolchain. It does not push an
image or alter the production Docker Publish workflow.

Choose the C++ worker for the shorter smoke. Choose the Java worker when the
full JDK and GraalVM ARM64 image needs proof. The workflow removes its local
test image when it exits.

The runner user belongs to the host `docker` group. That membership is
root-equivalent authority on Nano1. Only trusted, manually dispatched
workflows may select the `nano1` label. Do not add `pull_request`, `push`, or
public fork triggers to a Nano1 workflow. Coding workers and model processes
must never receive the Docker socket.

The production multi-architecture workflow continues to use GitHub-hosted
ARM64 runners until Nano1 has enough successful smoke history and an explicit
promotion review.

## Inference role

Nano1 hosts text-embeddings-inference, but the build runner and inference
processors are separate capacities. A busy build must not silently consume
the memory reserved for inference, and an inference container must not
receive the runner's Docker authority.

## Embeddings

The `tei-gpu` service provides normalized 384-dimensional embeddings from
`BAAI/bge-small-en-v1.5`. The model fits the Orin's shared memory alongside
the build runner and is useful for agent memory, source-code retrieval, and
local search. Both its public model revision and the TEI source revision are
pinned.

TEI is compiled natively for the Orin's `sm_87` CUDA capability. The build uses
the CUDA-only Candle backend, so an unavailable GPU is a startup failure. CPU
inference is not compiled into this image. Flash Attention is disabled because
this small BERT model does not need it and the simpler CUDA path is easier to
verify on Jetson. The source patch also adds TEI's missing exact `8.7` runtime
compatibility case. The service uses TEI's gRPC build, including server
reflection, unary embeddings, bidirectional streaming embeddings, health, and
per-request timing metadata. ProtoMolt can therefore register the endpoint
without translating an HTTP-only API.

Build, start, and verify TEI on Nano1 with:

```shell
sudo -u protomolt-runner -H env PROTOMOLT_NANO1_TEI_LIVE=1 \
  scripts/nano1-tei-live.sh
```

The first native build compiles TEI and takes much longer than later cached
builds. Model files persist in the `tei-model-cache` volume. The live check
uses a pinned ARM64 grpcurl container to verify reflection, gRPC health, model
identity, vector dimensions, finite values, normalization, and semantic
ordering for related and unrelated sentences.

TEI listens on `127.0.0.1:8083` by default. Set `NANO1_TEI_BIND` to Nano1's
Tailscale address only when direct tailnet access is needed. Do not bind the
unauthenticated API to every interface. A public route should terminate TLS
and authentication before forwarding to this endpoint.

With `NANO1_TEI_BIND` set to the host's Tailscale IPv4 address, tailnet members
can reflect and invoke `nano1:8083` with grpcurl or register it directly in
ProtoMolt. Tailscale encrypts that route and applies tailnet identity policy.
Internet exposure still requires an authenticated proxy.

The verified tailnet deployment uses `nano1:8083`. TEI used about 677 MiB of
host memory at idle in that deployment. Treat the observation as deployment
evidence, not a scheduling limit. The hard container limit remains 3 GiB.

Stop it with:

```shell
sudo -u protomolt-runner -H docker compose \
  -f deploy/nano1/compose.yml down
```

TEI provides the first useful model while keeping its cache on Nano1's local
disk. Its processor advertisement should identify the pinned model, embedding
dimensions, CUDA capability, concurrency limit, and current in-flight count.
TEI and the ARM64 build processor should publish different capability and
capacity records.

## Mesh publisher

`scripts/nano1-mesh-publisher.py` advertises Nano1 as one node with two
independent leased processors:

| Processor | Readiness gate | Advertised capacity |
|---|---|---|
| `nano1-tei` | NVIDIA-runtime container, pinned model identity, live normalized 384-dimensional embedding | TEI's reported concurrent-request limit |
| `nano1-arm64-builder` | Full native host gate, including ARM64, JetPack, CUDA, TensorRT, Docker, and power mode | One trusted build at a time |

The publisher renews only processors whose gate passes. A failed processor
keeps no new lease and becomes ineligible when its previous lease expires. If
the host gate or publisher stops, the node presence expires and the directory
cascades expiry to all of its processors. No CPU inference or CPU model offload
path exists.

The TEI endpoint is advertised only after its live gRPC gate passes. The
ARM64 advertisement uses the `arm64-build-capability` capability. It
describes a healthy build host; the bounded task API in planned work will add
remote execution without exposing Nano1's Docker socket.

Place `PROTOMOLT_MCP_TOKEN` in a root-readable or runner-readable environment
file outside the repository, then run the publisher as the existing
`protomolt-runner` account:

```shell
sudo -u protomolt-runner -H env PROTOMOLT_NANO1_MESH_LIVE=1 \
  scripts/nano1-mesh-publisher.py
```

The defaults publish every 30 seconds with 90-second leases. Set
`PROTOMOLT_NANO1_MESH_INTERVAL_SECONDS`,
`PROTOMOLT_NANO1_MESH_TTL_SECONDS`, or
`PROTOMOLT_NANO1_MESH_STATE` when the host needs different scheduling or state
paths. `--once` performs one publication and returns, which is the live
acceptance form.

For boot persistence, install the publisher, its host gate, and the supplied
systemd unit without cloning a writable repository onto Nano1:

```shell
sudo install -d -o root -g root -m 0755 \
  /opt/protomolt-mesh/scripts /opt/protomolt-mesh/deploy/nano1 /etc/protomolt
sudo install -o root -g root -m 0755 scripts/nano1-mesh-publisher.py \
  /opt/protomolt-mesh/scripts/nano1-mesh-publisher.py
sudo install -o root -g root -m 0755 deploy/nano1/check-host.sh \
  /opt/protomolt-mesh/deploy/nano1/check-host.sh
sudo install -o root -g root -m 0644 \
  deploy/nano1/protomolt-mesh-publisher.service \
  /etc/systemd/system/protomolt-mesh-publisher.service
sudoedit /etc/protomolt/nano1-mesh.env
sudo chmod 0600 /etc/protomolt/nano1-mesh.env
sudo systemctl daemon-reload
sudo systemctl enable --now protomolt-mesh-publisher.service
```

The environment file must contain `PROTOMOLT_MCP_TOKEN`. It may override
`PROTOMOLT_MCP_ENDPOINT`, lease timing, the state path, or the advertised
addresses described above. The unit runs as `protomolt-runner`, keeps its state
under `/var/lib/protomolt-runner/mesh`, and restarts after a process failure.
Its Docker group access is privileged host authority, so only the fixed,
root-owned publisher and gate are installed under `/opt/protomolt-mesh`.

Inspect its current result without exposing the token:

```shell
sudo systemctl status protomolt-mesh-publisher.service
sudo journalctl -u protomolt-mesh-publisher.service -n 100
```

Native `linux/arm64` images produced here run on both the Jetson and the
Raspberry Pi fleet when their base images support ARM64. GPU images remain
Jetson-specific. Use the manual smoke workflow to prove a build target before
moving it into a trusted release workflow.

## Host verification

Run the same read-only checks used by the smoke workflow:

```shell
deploy/nano1/check-host.sh
```

The script verifies the architecture, JetPack, CUDA, TensorRT, Docker, NVIDIA
runtime, and MAXN_SUPER power mode. It starts no container and changes no host
state.
