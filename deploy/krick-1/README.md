# krick-1 Muse Glimmer stack

`deploy/krick-1/` runs Meta Muse Glimmer as a persistent local delegation
worker on the krick-1 workstation beside the existing Kimi worker:

- `glimmer-vllm` serves `meta-models/Muse-Glimmer-30B` from a pinned
  `intel/llm-scaler-vllm:0.21.0-b3` derivative. The b3 image ships
  Transformers 5.8.0, which rejects `model_type muse_glimmer`, so
  `Dockerfile.glimmer-vllm` pins `transformers==5.15.0`. Inference runs with
  online FP8 and the DFlash assistant on one B70 mapped through `/dev/dri`
  and `ONEAPI_DEVICE_SELECTOR`. `--cpu-offload-gb` and `--swap-space` stay at
  zero: inference is GPU-only and there is no CPU model offload or fallback.
  `--limit-mm-per-prompt=image=0` keeps the server text-only.
- `kimi-worker` is the existing Kimi delegation worker. Kimi authentication
  enters through the host `~/.kimi-code` mount and is never baked into an
  image or written to this package.
- `glimmer-worker` is an agent host using the `openai` provider. It reaches
  the model at `http://glimmer-vllm:8011/v1` over the Compose network and
  sends no credential to the sidecar.

The sidecar publishes `127.0.0.1:8011` on the host loopback only. The
`pipeline-ovms` service on host ports 9000 and 9002 is a separate deployment;
this stack does not define, alter, or stop it.

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
| `GLIMMER_MODEL` | `meta-models/Muse-Glimmer-30B` | served model id, also passed to the Glimmer worker |
| `GLIMMER_MODEL_CACHE` | `/work/models/huggingface` | host model cache mounted into the sidecar |
| `GLIMMER_HOST_PORT` | `8011` | loopback host port for the sidecar |
| `GLIMMER_MAX_MODEL_LEN` | `32768` | vLLM context length |
| `GLIMMER_SPECULATIVE_CONFIG` | `{"method":"dflash","num_speculative_tokens":4}` | DFlash assistant configuration |
| `GLIMMER_DEVICE_SELECTOR` | `level_zero:0` | the single B70 for inference |
| `GLIMMER_HF_HUB_OFFLINE` | `0` | set `1` to serve only from the local cache |
| `PROTOMOLT_MCP_ENDPOINT` | `https://protomolt.rokkon.com/mcp` | coordinator MCP endpoint |
| `PROTOMOLT_MCP_TOKEN` | none, required | MCP bearer token |
| `KRICK_KIMI_IDENTITY` | `kimi-worker` | Kimi worker identity |
| `KRICK_GLIMMER_IDENTITY` | `glimmer-worker` | Glimmer worker identity |
| `KRICK_KIMI_WORKSPACE` | `/work/worktrees/protomolt/kimi` | Kimi worker worktree |
| `KRICK_GLIMMER_WORKSPACE` | `/work/worktrees/protomolt/glimmer` | Glimmer worker worktree |
| `KRICK_AGENT_UID` / `KRICK_AGENT_GID` | `1000` | container user matching the host |
| `KRICK_AGENT_STATE_DIR` | `~/.local/state/protomolt-agents` | durable host state |
| `KRICK_PROTOMOLT_GIT_DIR` | `/work/main/dev-tools/protomolt/.git` | linked-worktree Git metadata |

## Tailnet access

The sidecar stays on the host loopback. To reach it over the tailnet, forward
Tailscale Serve HTTPS to it on a dedicated path so existing routes are left
unchanged:

```shell
tailscale serve --bg --set-path /glimmer http://127.0.0.1:8011
```

## Live smoke

`scripts/muse-glimmer-live.sh` checks the model list and one bounded chat
completion against the running sidecar, then requires log or metrics evidence
that the B70/XPU loaded the model. It deploys nothing and changes nothing.
