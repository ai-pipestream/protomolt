# Agent host

`protomolt-agent-host` keeps a Codex process, a Kimi process, or a local
OpenAI-compatible model attached to the delegation tools on a ProtoMolt
server. The server remains the coordinator and transcript authority. The host
owns the model process, local workspace, MCP cursor, and provider session.

The host long-polls `/mcp` over a pooled HTTP/2 client. A model receives only
events relevant to its role and must return a JSON command batch. The host
checks all of the following before calling a tool:

- every relevant event cursor appears exactly once and in order;
- the tool is allowed for the configured worker or coordinator role;
- worker and coordinator identities cannot be overridden by model output;
- the command count is between 1 and 16; and
- every MCP command succeeds before the saved cursor advances.

`host-ack` records an explicit decision to take no protocol action. Invalid
model output gets one repair turn. A second invalid response leaves the cursor
unchanged so the event batch remains available after restart.

A model that narrates around its answer is not invalid output. The ACP
provider joins every message chunk of a turn, so a Kimi reply arrives as the
notes it wrote between tool calls followed by the command batch, sometimes in a
Markdown fence; the host takes the last complete JSON object in the reply as
the turn and ignores the text around it. A reply with no complete object is
still rejected.

## Build

```shell
./gradlew :protomolt-agent-host:installDist
```

The launcher is
`apps/agent-host/build/install/protomolt-agent-host/bin/protomolt-agent-host`.
The MCP endpoint must be the ProtoMolt serve endpoint, such as
`https://protomolt.rokkon.com/mcp`. A schema registry endpoint does not expose
the delegation tools.

A container image carrying the launcher and the Codex CLI is built from
`apps/agent-host/Dockerfile` (the distribution must be built first). The Kimi
CLI ships inside its own configuration directory and enters the container
through a mount; provider authentication is always mounted, never baked in.
[deploy/krick/](../../deploy/krick/README.md) defines the workstation stack
that runs one Kimi worker and one Codex coordinator from that image, plus the
exact host-Java commands for the same two agents.

Persistent coding workers can instead use the language-specific Java and C++
images. They add compilers, build systems, gRPC generators, and common agent
tools while preserving the same agent-host entrypoint and provider behavior.
See [Coding workers](coding-workers.md) for the image and security boundaries.

## Kimi worker

Kimi runs as one long-lived `kimi acp` child process. Its ACP session id is
saved before event processing. A replacement host uses `session/load` to
continue the same Kimi conversation.

```shell
export PROTOMOLT_MCP_TOKEN='replace-with-the-server-token'

apps/agent-host/build/install/protomolt-agent-host/bin/protomolt-agent-host \
  --endpoint https://protomolt.rokkon.com/mcp \
  --role worker \
  --identity kimi-worker \
  --provider kimi \
  --workspace /work/worktrees/protomolt/kimi \
  --state /var/lib/protomolt/agents/kimi-worker.json \
  --token-env PROTOMOLT_MCP_TOKEN
```

The default ACP permission policy selects an `allow_once` choice only when it
is the sole allow choice. Multiple choices, including questions represented as
permission choices, are cancelled instead of guessed. Use
`--acp-permissions reject` for a read-only agent.

## Codex coordinator

Codex runs one `codex exec --json` process per turn with a JSON output schema,
the workspace-write sandbox, and automatic approval review. The first turn
saves the returned thread id. Later turns use `codex exec resume`.

A coordinator can start with a plain-text bootstrap objective. The objective
is processed once and must produce at least one `delegation-offer`. The saved
state prevents the bootstrap from running twice.

```shell
printf '%s\n' \
  'Delegate the implementation to kimi-worker, review its evidence, and request corrections when checks are incomplete.' \
  > /var/lib/protomolt/agents/codex-objective.txt

apps/agent-host/build/install/protomolt-agent-host/bin/protomolt-agent-host \
  --endpoint https://protomolt.rokkon.com/mcp \
  --role coordinator \
  --identity codex-coordinator \
  --provider codex \
  --workspace /work/worktrees/protomolt/codex \
  --state /var/lib/protomolt/agents/codex-coordinator.json \
  --bootstrap /var/lib/protomolt/agents/codex-objective.txt \
  --token-env PROTOMOLT_MCP_TOKEN
```

The objective file is input, not a credential. Provider authentication stays
in the normal Codex and Kimi configuration directories. The MCP bearer token
is read from the environment named by `--token-env`; token material is not
accepted as a command-line option or written to host state.

## OpenAI-compatible local worker

`--provider openai` attaches a local OpenAI-compatible chat-completions
endpoint, such as an Intel llm-scaler-vllm sidecar on the same machine. This
provider requires `--provider-endpoint` with the server base URL and `--model`
with the served model id; both options are rejected for the Kimi and Codex
providers.

```shell
apps/agent-host/build/install/protomolt-agent-host/bin/protomolt-agent-host \
  --endpoint https://protomolt.rokkon.com/mcp \
  --role worker \
  --identity glimmer-worker \
  --provider openai \
  --provider-endpoint http://127.0.0.1:8011/v1 \
  --model muse-glimmer-30b \
  --workspace /work/worktrees/protomolt/glimmer \
  --state /var/lib/protomolt/agents/glimmer-worker.json \
  --token-env PROTOMOLT_MCP_TOKEN
```

The provider posts each turn to `<base>/chat/completions` with a JSON object
response request, so the host-side closed command schema still validates
every reply and a malformed reply gets the same single repair turn as the
process providers. A bounded in-memory conversation lets the repair turn see
the rejected answer. The client sends no bearer credential and no arbitrary
headers, which matches a loopback sidecar; endpoint response bodies are never
copied into error messages.

[deploy/krick-1/](../../deploy/krick-1/README.md) runs a Muse Glimmer sidecar
and one worker per provider on the krick-1 workstation.

## Recovery

The state file is replaced atomically and uses owner read/write permissions on
POSIX filesystems. It contains:

- the role, identity, provider, and normalized workspace;
- the last committed delegation cursor;
- the Kimi ACP session id or Codex thread id;
- whether the bootstrap turn completed; and
- a parsed command batch and next command position when execution is pending.

The server transcript remains the lifecycle authority. Configure the serve
process with repository-backed delegation storage so server restarts preserve
the cursor and worker sequence scopes. See [gRPC service](../surface/grpc-service.md)
for the repository endpoint, object, and encryption-key reference settings.
The NAS coordinator stack wires all of it: [deploy/portainer/](../../deploy/portainer/README.md)
runs a repo-service beside the coordinator and documents the transcript key
variable.

If the local state has a nonzero cursor but the server no longer knows the
worker, the host stops rather than registering a new identity against a lost
transcript. When the loss is real, which is what a coordinator redeployed over
empty volumes means, rerun with `--reset-on-transcript-loss`: the host reports
the cursor and any partly executed batch it is discarding, drops them, keeps
the provider session (it belongs to the agent, not to the coordinator that
forgot it), and registers from the start. The flag is off by default because
resuming against a transcript that is gone would invent continuity.
A pending command batch resumes at the next locally recorded
command. There is still a narrow ambiguity if a remote mutation succeeds and
the machine fails before its local command position is saved. Caller-supplied
idempotency keys on MCP mutation tools are needed to close that final gap.

## A turn that accepts without submitting

A worker turn runs only when the coordinator sends a frame, and the host filters
out the worker's own frames. So a turn that accepts a task and submits no
candidate for it leaves that task where nothing can move it: the coordinator is
waiting for the worker, and the worker is waiting for an event that will not
arrive unless the coordinator sends one for some other reason. The task sits
until its lease expires, which can be many minutes later and carries no
explanation.

The host reports that as it happens, naming the task and the commands the turn
actually returned:

```
agent-host: accepted task <id> and submitted no candidate for it. This turn
returned [delegation-accept]. A worker turn runs only on a coordinator frame, so
unless the coordinator sends another one this task will not progress and its
lease will expire unworked.
```

It is a report rather than a refusal. Accepting now and finishing on a later
frame is legitimate when the coordinator has more to say, so the host does not
pretend to know the task is doomed. What it removes is the case where a stalled
task and a task still being worked on look identical from outside.

A model that reliably returns only an accept is not a host fault, and this
message is how you tell that apart from a slow one.

Use `--once` to connect, perform one poll, and exit for deployment smoke tests.
