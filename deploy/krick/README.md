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
- The workspace and state directories, created once:

```shell
mkdir -p /work/worktrees/protomolt/kimi /work/worktrees/protomolt/codex \
  ~/.local/state/protomolt-agents
```

The workspaces are ordinary git worktrees the agents implement in. The state
directory holds each host's cursor and provider session record; back it up
with the worktrees.

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
