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

Nano1 can also host a DJL Serving processor, but the build runner and inference
processor are separate capacities. A busy build must not silently consume the
memory reserved for inference, and an inference container must not receive the
runner's Docker authority.

The official DJL Serving 0.36 GPU image does not publish an ARM64 manifest. A
Nano1 deployment therefore needs a JetPack 7.2 compatible image and an engine
that uses the installed NVIDIA stack. CPU inference and CPU model offload are
not acceptable fallbacks. Live acceptance must prove GPU execution, bounded
memory use, model health, and a typed request through ProtoMolt before the
processor advertises availability to the mesh.

Start with one small model that fits comfortably beside host services. Keep
the model cache on Nano1's local disk. The processor advertisement should name
its schema, model, provider, GPU capability, concurrency limit, and current
in-flight count. The ARM64 build processor should publish a different
capability and capacity record.

## Host verification

Run the same read-only checks used by the smoke workflow:

```shell
deploy/nano1/check-host.sh
```

The script verifies the architecture, JetPack, CUDA, TensorRT, Docker, NVIDIA
runtime, and MAXN_SUPER power mode. It starts no container and changes no host
state.
