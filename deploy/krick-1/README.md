# krick-1 Muse Glimmer stack

`deploy/krick-1/` runs Meta Muse Glimmer as a persistent local delegation
worker on the krick-1 workstation beside the existing Kimi worker:

- `glimmer-vllm` serves the verified local Muse Glimmer artifacts from a
  pinned `intel/llm-scaler-vllm:0.21.0-b3` derivative. The b3 image ships
  Transformers 5.8.0, which rejects `model_type muse_glimmer`, so
  `Dockerfile.glimmer-vllm` pins `transformers==5.15.0`. The model loads from
  the pinned host directory `/work/models/muse-glimmer` mounted read-only at
  `/models`, serves `/models/Muse-Glimmer-30B` as `muse-glimmer-30b`, and runs
  online FP8 on one B70 mapped through `/dev/dri` with a read-only
  `/dev/dri/by-path` bind that oneCCL initialization needs. The baseline is
  text-only (`--limit-mm-per-prompt=image=0,video=0`) with a 4096 context;
  larger contexts are opt-in through `GLIMMER_MAX_MODEL_LEN`.
  The launch also pins float16 model and Mamba cache dtypes, a 64-token KV
  block, one concurrent sequence, 4096 batched tokens, 98 percent GPU memory
  utilization, Flash Attention, and eager execution. These are the settings
  exercised by the live single-B70 service.
  `--cpu-offload-gb` stays at zero: inference is GPU-only and there is no CPU
  model offload or fallback. This Intel vLLM build does not expose the
  upstream `--swap-space` option, so the launch does not pass it.
- `kimi-worker` is the existing Kimi delegation worker. Kimi authentication
  enters through the host `~/.kimi-code` mount and is never baked into an
  image or written to this package.
- `glimmer-worker` is an agent host using the `openai` provider. It reaches
  the model at `http://glimmer-vllm:8011/v1` over the Compose network and
  sends no credential to the sidecar.

The sidecar publishes `127.0.0.1:8011` on the host loopback only. The
`pipeline-ovms` service on host ports 9000 and 9002 is not managed by this
stack; it is currently stopped with user approval to free the B70.

## OpenVINO embeddings sidecar

`compose.embeddings.yml` is a separate Compose project running OpenVINO
Model Server (`openvino/model_server:latest-gpu`, OVMS 2026.2.1 at
verification time) for sentence embeddings on the same B70. The pinned
artifacts in `/work/models/ovms-embedder` were extracted once from the
cached `git.rokkon.com/ai-pipestream/embedder-ovms-models:minilm-mpnet-gpu`
image:

```shell
docker create --name tmp-embedder-models git.rokkon.com/ai-pipestream/embedder-ovms-models:minilm-mpnet-gpu
docker cp tmp-embedder-models:/models /work/models/ovms-embedder
docker rm tmp-embedder-models
```

The `config-gpu.json` there declares two DAG pipelines: `mpnet_pipeline`
(768-dimensional embeddings) and `minilm_pipeline` (384-dimensional). The
tokenizer models run on CPU; the embedding models compile for the GPU. The
REST endpoint publishes `127.0.0.1:8091` on the host loopback only:

```shell
docker compose -f deploy/krick-1/compose.embeddings.yml up -d
curl -fsS -H 'content-type: application/json' \
  -d '{"instances":[{"strings":"hello"}]}' \
  http://127.0.0.1:8091/v1/models/mpnet_pipeline:predict
```

The sidecar shares the B70 with `glimmer-vllm`: both embedding models
together used about 0.3 GiB of VRAM at verification, inside the headroom
vLLM leaves unreserved. It is an always-on service (`restart:
unless-stopped`), like the rest of this stack.

| Variable | Default | Purpose |
| --- | --- | --- |
| `OVMS_IMAGE` | `openvino/model_server:latest-gpu` | OVMS image (2026.2.1 at verification time) |
| `OVMS_MODELS_DIR` | `/work/models/ovms-embedder` | pinned artifacts mounted read-only at `/models` |
| `EMBEDDER_HOST_PORT` | `8091` | loopback host port for the OVMS REST endpoint |
| `OVMS_RENDER_GID` | `990` | host render group for `/dev/dri` access |

## Run

```shell
./gradlew :protomolt-agent-host:installDist
docker compose -f deploy/krick-1/compose.yml build
docker compose -f deploy/krick-1/compose.yml up -d
docker compose -f deploy/krick-1/compose.yml logs -f
```

`PROTOMOLT_MCP_TOKEN` is required. Put it in a `.env` file next to the
compose file (gitignored) or export it in the shell. The MCP bearer token is
the current compatibility path for worker authentication; sidecar workload
identity and mTLS between the workers, the sidecar, and the coordinator are
future transport work and are not configured here.

## Configuration

| Variable | Default | Purpose |
| --- | --- | --- |
| `GLIMMER_VLLM_IMAGE` | `protomolt-glimmer-vllm:0.21.0-b3-transformers-5.15.0` | local tag for the pinned derivative |
| `GLIMMER_MODEL_DIR` | `/work/models/muse-glimmer` | pinned local artifacts mounted read-only at `/models` |
| `GLIMMER_MODEL_PATH` | `/models/Muse-Glimmer-30B` | model directory the server loads |
| `GLIMMER_SERVED_NAME` | `muse-glimmer-30b` | served model id, also passed to the Glimmer worker |
| `GLIMMER_HOST_PORT` | `8011` | loopback host port for the sidecar |
| `GLIMMER_MAX_MODEL_LEN` | `4096` | verified context baseline; larger values are opt-in |
| `GLIMMER_MAX_BATCHED_TOKENS` | `4096` | verified batch-token limit |
| `GLIMMER_MAX_NUM_SEQS` | `1` | verified single-request concurrency baseline |
| `GLIMMER_GPU_MEMORY_UTILIZATION` | `0.98` | verified fraction of B70 memory available to vLLM |
| `GLIMMER_SPECULATIVE_CONFIG` | empty (disabled) | opt-in DFlash experiment, see below |
| `GLIMMER_DEVICE_SELECTOR` | `level_zero:0` | the single B70 for inference |
| `GLIMMER_HF_HUB_OFFLINE` | `1` | serve only from the local artifacts |
| `PROTOMOLT_MCP_ENDPOINT` | `https://protomolt.rokkon.com/mcp` | coordinator MCP endpoint |
| `PROTOMOLT_MCP_TOKEN` | none, required | MCP bearer token |
| `KRICK_KIMI_IDENTITY` | `kimi-worker` | Kimi worker identity |
| `KRICK_GLIMMER_IDENTITY` | `glimmer-worker` | Glimmer worker identity |
| `KRICK_KIMI_WORKSPACE` | `/work/worktrees/protomolt/kimi` | Kimi worker worktree |
| `KRICK_GLIMMER_WORKSPACE` | `/work/worktrees/protomolt/glimmer` | Glimmer worker worktree |
| `KRICK_AGENT_UID` / `KRICK_AGENT_GID` | `1000` | container user matching the host |
| `KRICK_AGENT_STATE_DIR` | `~/.local/state/protomolt-agents` | durable host state |
| `KRICK_PROTOMOLT_GIT_DIR` | `/work/main/dev-tools/protomolt/.git` | linked-worktree Git metadata |

## DFlash experiment (opt-in, two GPUs)

Speculative decoding is disabled by default. One FP8 main model plus one FP8
assistant exceeds the 32 GiB of a single B70 (33.21 GiB loaded, negative KV
capacity), so DFlash is only an experiment for a two-GPU setup. To try it,
map a second card, raise tensor parallelism, and set
`GLIMMER_SPECULATIVE_CONFIG` to a JSON object that names an explicit
assistant path, for example:

```shell
GLIMMER_SPECULATIVE_CONFIG='{"method":"dflash","model":"/models/Muse-Glimmer-30B-DFlash","num_speculative_tokens":4}'
```

The variable is empty by default and the server flag is omitted entirely
unless it is set.

## Tailnet access

The sidecar stays on the host loopback. To reach it over the tailnet, forward
Tailscale Serve HTTPS to it on a dedicated path so existing routes are left
unchanged:

```shell
tailscale serve --bg --set-path /glimmer http://127.0.0.1:8011
```

## Verification

`deploy/krick-1/check-deployment-statics.sh` runs static checks without
containers or a GPU and fails when the compose file drifts from the verified
baseline: the read-only `/dev/dri/by-path` bind and pinned `/models` mount
must be present, CPU offload must stay zero, the unsupported swap flag must
stay absent, and DFlash must remain opt-in only.

`scripts/muse-glimmer-live.sh` checks the model list and one bounded chat
completion against the running sidecar, then requires log or metrics evidence
that the B70/XPU loaded the model. It deploys nothing and changes nothing.

`scripts/krick1-embeddings-live.sh` checks the OVMS model status, runs one
bounded embeddings request against `mpnet_pipeline`, requires the returned
vector to be 768-dimensional, then requires log evidence that the embedding
models compiled for the GPU. It deploys nothing and changes nothing.
