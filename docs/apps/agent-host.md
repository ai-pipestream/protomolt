# Agent host

`protomolt-agent-host` keeps a coding agent (Codex, Kimi, Cursor, Antigravity,
Muse Code) or a local OpenAI-compatible model attached to the delegation tools
on a ProtoMolt server. The server remains the coordinator and transcript authority. The host
owns the model process, local workspace, MCP cursor, and provider session.

The host long-polls `/mcp` over a pooled HTTP/2 client. A model receives only
events relevant to its role and must return a JSON command batch. The host
checks all of the following before calling a tool:

- every relevant event cursor appears exactly once and in order;
- the tool is allowed for the configured worker or coordinator role;
- worker and coordinator identities cannot be overridden by model output;
- the command count is between 1 and 16;
- every command is the delegation verb's own request message; and
- every MCP command succeeds before the saved cursor advances.

A command's arguments are the canonical proto3 JSON of the request message the
verb takes: `delegation-accept` carries an `AcceptTaskRequest`,
`delegation-candidate` a `SubmitCandidateRequest`, and so on for the six other
delegation commands. The host parses the arguments into that message, refusing
any member no field declares, and applies the message's own validation rules.
Those are the rules the coordinator applies when the call arrives, message-level
CEL included. A refusal quotes the rule's own words, so the repair turn reads
`command 0 delegation-candidate: candidate a completion candidate must
reference at least one commit or artifact; saying done is not evidence` rather
than a paraphrase. `host-ack` is the one command with no request message: it is
host-local, takes a single bounded `reason`, and records an explicit decision to
take no protocol action.

The command schema offered to providers that take a structured-output schema,
and the argument contract stated in the prompt, are rendered from the same
request descriptors. Every object in the schema is closed and names every
member it accepts; a member the message does not require is offered as
nullable, because proto3 JSON reads a null as the field being absent. The
identity members the host sets itself (the worker id on a worker command, the
sender and recipient on a message) are left out of what the model is asked
for. A rule added to `delegation_actions.proto` therefore reaches the prompt,
the schema, and the check on the reply without a second edit here.

Invalid model output gets one repair turn. A second invalid response leaves the
cursor unchanged so the event batch remains available after restart.

A rejected model reply (the parse, required-action, or deliverable-contract
checks) is a `ModelReplyException`, tracked separately from transport, MCP,
and provider failures. `run()` backs it off in minutes instead of seconds —
1, 2, 4, 8, capped at 15 — because retrying the identical batch on a short
clock just resends the same bad prompt and, with a process model whose
context grows every turn, burns tokens without changing the outcome.
Consecutive rejections on the same saved cursor count as one batch; a
successful batch, or a rejection after the cursor has advanced, resets the
count. After `--max-batch-failures` consecutive rejections on one batch
(default 6) the host gives up: it logs the batch's cursor and rejection
count, closes the provider, and stops, leaving the cursor untouched so a
restart re-presents the same batch to an operator or a fixed model instead
of retrying it forever. `protomolt-agent-host` then exits with status 3.
Every other `AgentHostException` keeps the original uncapped seconds-scale
retry.

## Deliverable contracts

An offer whose spec names a deliverable contract (see
[Agent delegation](../transform/delegation.md#deliverable-contract)) reaches the model
with the contract's type name and its rendered JSON Schema in place of the descriptor
bytes, and the prompt names the type each open task's candidate must return. The host
keeps every contract it was offered in its state file, keyed by task, so a restart still
knows how to read a deliverable. A candidate's `result` is parsed with the contract's
type and checked with the coordinator's own gate before the command is sent: a missing
result, a result for a task without a contract, a wrong type, or a rule violation is
a repair turn here, worded as the coordinator would word it, and never costs an attempt.
Providers with enforced structured output receive the schema again whenever the known
contracts change; with no contract known the schema leaves the deliverable out, since an
open object is not expressible to a strict endpoint.

A model that narrates around its answer is not invalid output. The ACP
provider joins every message chunk of a turn, so a Kimi reply arrives as the
notes it wrote between tool calls followed by the command batch, sometimes in a
Markdown fence; the host takes the last complete JSON object in the reply as
the turn and ignores the text around it. A reply that stops one or more closing
braces or brackets short of a well-formed object is completed before parsing;
that is the only repair applied. A reply with no complete object is still
rejected, and the rejection names the line and column where parsing failed so
the repair prompt tells the model what to fix.

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
response request, so the host-side check against the request messages still
validates every reply and a malformed reply gets the same single repair turn as the
process providers. A bounded in-memory conversation lets the repair turn see
the rejected answer. The client sends no bearer credential and no arbitrary
headers, which matches a loopback sidecar; endpoint response bodies are never
copied into error messages.

[deploy/krick-1/](../../deploy/krick-1/README.md) runs a Muse Glimmer sidecar
and one worker per provider on the krick-1 workstation.

## Cursor worker

`--provider cursor` runs one long-lived `agent acp` child process, the ACP
server the Cursor CLI ships, and drives it the way the Kimi provider drives
`kimi acp`. Cursor advertises the `cursor_login` authentication method, which
the provider runs after the handshake; the credential itself is the CLI's
existing login or `CURSOR_API_KEY` in the host's environment. Cursor also
raises two blocking extension requests during a turn, and an unattended host
answers both without a person: a multiple-choice question is skipped with a
reason that sends the agent to the coordinator through `delegation-message`,
and a plan is accepted. The ACP session id is saved and reloaded across host
restarts.

```shell
apps/agent-host/build/install/protomolt-agent-host/bin/protomolt-agent-host \
  --endpoint https://protomolt.rokkon.com/mcp \
  --role worker \
  --identity cursor-worker \
  --provider cursor \
  --workspace /work/worktrees/protomolt/cursor \
  --state /var/lib/protomolt/agents/cursor-worker.json \
  --token-env PROTOMOLT_MCP_TOKEN
```

Two things to know before relying on it. A Cursor team administrator can
disable headless use for the whole organisation, which stops `agent acp` as
well; check that setting first. And the model is chosen in the CLI's own
configuration, not through `--model`, because ACP has no model parameter.

## Antigravity worker

`--provider antigravity` runs one `agy` process per turn over the CLI's
stream-json pipe: the packet goes in as one `user` line on stdin, the answer
is the terminal `result` event, and `--json-schema` makes the CLI enforce the
host's closed command schema on that answer, which no other provider gets
from its vendor. The conversation id from the first turn is saved and passed
as `--conversation` on every later one, so the model keeps its context across
turns and host restarts. `--model` selects the Antigravity model.

```shell
apps/agent-host/build/install/protomolt-agent-host/bin/protomolt-agent-host \
  --endpoint https://protomolt.rokkon.com/mcp \
  --role worker \
  --identity agy-worker \
  --provider antigravity \
  --model gemini-3.7-flash-high \
  --workspace /work/worktrees/protomolt/agy \
  --state /var/lib/protomolt/agents/agy-worker.json \
  --token-env PROTOMOLT_MCP_TOKEN
```

Use Antigravity CLI 1.1.24 or later: earlier builds hang on exit when both
stdout and stderr are pipes, which is what a child process launched from the
host always has. Every turn is a separate process, so the usage numbers on
each result are per turn and the provider sums them for the session.

## Muse Code worker

`--provider muse` runs one long-lived `muse serve` session host and speaks
MSP, Muse's JSON-RPC session protocol, over its stdio. The host starts one
session in the workspace with the `allowAll` approval mode, so no tool call
waits for a person, and resumes that session across host restarts; when the
serve process no longer has it, a fresh session is started and the new id is
saved. `--model` selects the Muse model. The turn's token usage comes from
the host's own accounting and is summed for the session.

```shell
apps/agent-host/build/install/protomolt-agent-host/bin/protomolt-agent-host \
  --endpoint https://protomolt.rokkon.com/mcp \
  --role worker \
  --identity muse-worker \
  --provider muse \
  --model muse-spark-1.3 \
  --muse-sandbox off \
  --workspace /work/worktrees/protomolt/muse \
  --state /var/lib/protomolt/agents/muse-worker.json \
  --token-env PROTOMOLT_MCP_TOKEN
```

Sandbox posture is fixed when `muse serve` starts and is the operator's
choice. Muse's default sandbox has no network and no writable home, so a
worker that must run Gradle or fetch from a git remote needs
`--muse-sandbox off`; leave it on for a review-only worker. The host always
passes `--trust-workspace`, which is what lets the checkout's own skills and
rules load. Exit code 5 from `muse serve` means the installed build has no
SDK surface and will not serve; the host reports that once and exits rather
than retrying. Remote MCP servers over HTTP are documented for Muse but were
not wired in the 1.0.3 build; the worker skill reaches the coordinator through
the host, not through Muse's own MCP configuration.

## Recovery

The state file is replaced atomically and uses owner read/write permissions on
POSIX filesystems. It contains:

- the role, identity, provider, and normalized workspace;
- the last committed delegation cursor;
- the provider session: a Kimi or Cursor ACP session id, a Codex thread id, an
  Antigravity conversation id, or a Muse session id;
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
