# Agent hosts on Krick

`compose.yml` runs the two persistent agent hosts on the Krick workstation:
one Kimi worker and one Codex coordinator. Model execution stays on Krick by
design. This stack is not part of the NAS Portainer coordinator stack
(`../portainer/`) and must not be deployed there; the agents reach the
coordinator over its public MCP endpoint, `https://protomolt.rokkon.com/mcp`.

Both agents come from the same image, `protomolt-agent-host:local`, built from
`apps/agent-host/Dockerfile`. Provider authentication stays in the host's
`~/.kimi-code` and `~/.codex` directories, which the containers mount read
and write so provider sessions survive restarts. Nothing in this directory
carries a credential.

## Prerequisites

- Docker with the Compose plugin.
- The agent host distribution: `./gradlew :protomolt-agent-host:installDist`
  (the image build copies it).
- An MCP bearer token for the coordinator in `PROTOMOLT_MCP_TOKEN`, either
  exported or in `deploy/krick/.env` (gitignored).
- Kimi Code installed and authenticated on the host (`~/.kimi-code`), and the
  Codex CLI authenticated on the host (`~/.codex`). The image carries the
  Codex CLI itself; the Kimi binary enters through the `~/.kimi-code` mount.
- A host `~/.gitconfig` with the commit identity the agents should use. It is
  mounted read-only. Git push credentials are not mounted.
- Two dedicated git worktrees and the state directory, created once from the
  repository root:

```shell
git fetch origin
git worktree add -b agent/kimi-worker \
  /work/worktrees/protomolt/kimi origin/main
git worktree add -b agent/codex-coordinator \
  /work/worktrees/protomolt/codex origin/main
mkdir -p ~/.local/state/protomolt-agents
```

Choose different branch names if either example branch already exists. The
agents implement and commit inside these worktrees. The state directory holds
each host's cursor and provider session record; back it up with the worktrees.
The compose stack also mounts the source repository's shared `.git` directory
at its original absolute path because linked worktrees refer to it there. Set
`KRICK_PROTOMOLT_GIT_DIR` if the main checkout moves.

## Run

```shell
docker compose -f deploy/krick/compose.yml build
docker compose -f deploy/krick/compose.yml up -d
docker compose -f deploy/krick/compose.yml logs -f
```

Stop with `docker compose -f deploy/krick/compose.yml down`. The bind-mounted
state survives `down`; each host resumes its delegation cursor and its Kimi
ACP session or Codex thread on the next start.

The coordinator's bootstrap objective lives in `codex-objective.txt` next to
the compose file. Edit it before the first start. It is processed exactly
once; the saved host state suppresses it afterwards. To reset the deployment,
stop the stack and clear `~/.local/state/protomolt-agents`, then start again.

## Variables

| Variable | Required | Default | Purpose |
|---|---|---|---|
| `PROTOMOLT_MCP_TOKEN` | yes | none | Bearer token the agent hosts send to the MCP endpoint |
| `PROTOMOLT_MCP_ENDPOINT` | no | `https://protomolt.rokkon.com/mcp` | Coordinator MCP endpoint |
| `KRICK_KIMI_IDENTITY` | no | `kimi-worker` | Worker identity the coordinator offers tasks to |
| `KRICK_CODEX_IDENTITY` | no | `codex-coordinator` | Coordinator identity |
| `KRICK_KIMI_WORKSPACE` | no | `/work/worktrees/protomolt/kimi` | Worktree the Kimi worker operates in |
| `KRICK_CODEX_WORKSPACE` | no | `/work/worktrees/protomolt/codex` | Worktree the Codex coordinator operates in |
| `KRICK_AGENT_STATE_DIR` | no | `~/.local/state/protomolt-agents` | Host directory for cursor and provider session state |
| `KRICK_PROTOMOLT_GIT_DIR` | no | `/work/main/dev-tools/protomolt/.git` | Shared Git metadata used by the linked worktrees |
| `KRICK_AGENT_UID` / `KRICK_AGENT_GID` | no | `1000` | Container process id; match the host user so mounts stay writable |

## Without containers

The same two agents run directly on the host Java runtime:

```shell
./gradlew :protomolt-agent-host:installDist
export PROTOMOLT_MCP_TOKEN='replace-with-the-server-token'

apps/agent-host/build/install/protomolt-agent-host/bin/protomolt-agent-host \
  --endpoint https://protomolt.rokkon.com/mcp \
  --role worker \
  --identity kimi-worker \
  --provider kimi \
  --workspace /work/worktrees/protomolt/kimi \
  --state ~/.local/state/protomolt-agents/kimi-worker.json \
  --token-env PROTOMOLT_MCP_TOKEN

apps/agent-host/build/install/protomolt-agent-host/bin/protomolt-agent-host \
  --endpoint https://protomolt.rokkon.com/mcp \
  --role coordinator \
  --identity codex-coordinator \
  --provider codex \
  --workspace /work/worktrees/protomolt/codex \
  --state ~/.local/state/protomolt-agents/codex-coordinator.json \
  --bootstrap deploy/krick/codex-objective.txt \
  --token-env PROTOMOLT_MCP_TOKEN
```

Run each in its own terminal session or under a process supervisor. See
[docs/apps/agent-host.md](../../docs/apps/agent-host.md) for the CLI surface
and the recovery semantics.

## Live acceptance

`scripts/agent-host-live.sh` (from the repository root) proves the full
delegation lifecycle against the coordinator with real provider processes,
including a worker restart across a checkpoint. It is opt-in and skips unless
its environment contract is met.

## Embeddings sidecar

`compose.embeddings.yml` runs a text-embeddings-inference sidecar on the
workstation's NVIDIA GPU, separate from the agent stack. It serves pinned
`BAAI/bge-m3` as 1024-dimensional normalized embeddings over HTTP on
`127.0.0.1:8096` (loopback only, unauthenticated; the same exposure rule as
the Nano1 and MacBook endpoints applies). It replaces the retired DJL Serving
embedder, whose Rust engine has no working CUDA path.

```shell
docker compose -f deploy/krick/compose.embeddings.yml up -d

curl -s http://127.0.0.1:8096/embed \
  -H 'Content-Type: application/json' \
  -d '{"inputs": ["Krick verifies its embedding sidecar."]}'
```

Stop with `docker compose -f deploy/krick/compose.embeddings.yml down`. Model
files persist in the `tei-model-cache` volume. `TEI_IMAGE`, `TEI_MODEL_ID`,
`TEI_MODEL_REVISION`, and `EMBEDDER_HOST_PORT` override the image, model, and
port. The 1024-dimensional output matches the MacBook Air's `bge-m3` endpoint
in dimension; indexes built from one are not comparable with the 384- and
768-dimensional endpoints on Nano1 and krick-1.

Verified 2026-08-21 against the running container on the RTX 4080 SUPER:
about 1400 embeddings/s with `bge-m3` and 9200 embeddings/s with
`bge-small-en-v1.5`, unit-normed vectors in both cases.
