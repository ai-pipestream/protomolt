# Goal 4: agent coordination starter

This is the implementation and acceptance plan for the second starter. It does
not describe unfinished operations as available. Goal 3's correction starter
remains a separate package.

## Prerequisite

Land the reviewed revision/attempt binding change first. External reviews name
the exact task, attempt, and revision. The coordinator checks this identity and
records the decision under one lock. Invalid typed candidates and missing
required evidence cannot reach review. Receipt integrity does not prove a
worker's report or a reviewer's semantic judgment true.

## Reuse and bounded additions

- Reuse TaskSpec, DeliverableContract, CompletionCandidate, CheckEvidence,
  TaskMessage, the delegation reducer, encrypted repository transcript adapter,
  AgentHost, task console, and existing signed-record projector/verifier.
- Add a catalog adapter for the existing TaskRejected stream frame. Its request
  names worker, task, attempt, and bounded reason. AgentHost may accept or reject
  an offer. Unsupported fixture work terminates as rejected with a reason rather
  than taking a lease and waiting for expiry. This is protocol parity, not a new
  task lifecycle.
- Extend the bounded browser offer API with descriptor bytes and a type name.
  Derive JSON Schema on the server from the complete descriptor/import closure.
  Bound the offer body separately from messages/reviews and enforce the real
  frame limit. Supply a sample descriptor plus source; allow callers to upload
  their own descriptor set. Do not trust caller-supplied rendered schema.
- Add a small CoordinationReport example with field bounds and a cross-field
  count rule. It is a caller-defined deliverable inside the existing envelope,
  not a new service. The fixture provider only handles this example; live
  providers receive whichever validated contract the caller supplies.
- Export deterministic transcript bytes from the same snapshot as the signed
  record. Any record's referenced artifacts must all be supplied and rehashed
  before full artifact verification is claimed.
- Mount the existing DelegationService contract on native gRPC through the same
  catalog actions used by MCP and REST. Share contract-to-action matching and
  the existing server authorization interceptor; do not introduce another task
  service or authentication path. Native binary Any preserves the offered type.
- Extend the remote ACP adapter with `delegation/<RpcName>` commands for those
  existing methods. Recover caller-defined Any descriptors from authenticated
  task history and render each event with its own task/attempt contract, even
  after adapter restart. gRPC schema validation alone is not evidence of gRPC
  coordination, and an ACP-capable provider is not evidence of this ACP surface.

## Starter runtime

One versioned image-only Compose package starts serve, repo-service, PostgreSQL,
RustFS, initialization jobs, and an explicitly labeled fixture AgentHost worker.
Use existing persistence adapters, with separate durable volumes for registry,
transcripts, database, worker cursor/session/workspace, encryption key, and
signing identity. No model account, local JDK, or image build is required for the
fixture. Keep the one-command docker compose up -d entry point: init services
generate secrets once, and service entrypoints read their own mounted secret
files before launching the existing process. Verify those wrappers against the
actual images. Do not rely on Compose interpolating values generated later.

Generate distinct operator, console, and coordination credentials. The worker
gets a coordination-scoped credential, never the operator token or signing key.
Current worker-coordinate authorization does not bind a credential to a worker
identity or distinguish review from submission. The starter is a trusted
single-operator deployment; AgentHost enforces its worker tool allowlist. Do not
claim isolation against an arbitrary client holding that credential.

Generate signing seed and matching public trust with a helper in the serve
distribution, using existing receipt primitives. Preserve and verify identity
on restart; never silently replace inconsistent persisted key material. The
independent verifier remains verification-only.

The fixture uses the real AgentHost command validation, MCP transport, cursor,
state, and replay paths. It writes actual report bytes as a content-addressed
artifact and submits that digest in top-level candidate artifacts. It must not
invent a commit or a check execution. Its reported checks are labeled fixture
checks. Human review from the console accepts/revises only the displayed
candidate. Optional external coordinator agents use the same bound review API.

Optional real-provider configuration uses the existing Kimi CLI adapter and
host-owned authentication, workspace, and state. Never package credentials or
mount a Docker socket into the agent. Verify artifact transfer explicitly when
the worker runs on another host. No inference deployment changes are needed.

## Qualification

1. Contracts compile with all imports; lint and compatibility checks pass.
   Request and response validation fixtures exercise bounds, required evidence,
   wrong type, invalid cross-field result, and unsupported contract rejection.
2. On a clean Compose installation, login, create a typed task, exchange a
   question/answer or guidance with the worker, and inspect the candidate and
   its actual artifact. The browser can upload a caller contract and explain
   validation failures without exposing operator credentials.
3. Reject invalid candidates before review. Request revision, refuse a delayed
   decision for the older revision/attempt without mutation, and accept the
   inspected replacement. A rejected unsupported offer does not linger.
   Exercise delegation itself over native gRPC, MCP, and the remote ACP adapter,
   including custom typed results and existing scoped authorization.
4. Restart coordinator and worker; recover task history, identity, cursors, and
   subsequent conversation/review. Show useful errors on missing dependencies
   rather than falling back to an empty transcript.
5. Export the selected signed record, public trust, exact signed transcript,
   and referenced artifact bytes. Verify offline with networking disabled;
   tampering/missing bytes must fail. Post-acceptance messages cannot produce a
   transcript that differs from the exported record's snapshot.
6. Run a bounded real Kimi CLI task separately. Record source/image revision,
   provider and CLI/model identification when available, exact operations and
   test evidence. Publish a provider matrix distinguishing this live proof,
   scripted provider tests, historical reports, and untested providers. No
   generalized live-support claims from the fixture.
7. Pass local checks and hosted CI before merge. Publish immutable images and
   the downloadable package; test native amd64/arm64 where advertised and
   anonymous download/pull. Qualify the downloaded package through the browser,
   protocol operations, restart, and offline verification before completion.

## COORD-1 recovery requirements

Preserve the named backlog in `docs/plans/starter-contract-inventory.md`:
`accepted_without_candidate_has_next_action`,
`missing_followup_expires_without_acceptance`,
`checkpoint_reoffer_names_new_attempt`, `pending_batch_restart_preserves_cursor`,
and `scope_change_cannot_reuse_old_acceptance`. For waiting states, the console
must state the reason, responsible actor, and available recovery action. Reuse
the existing lease, cancellation, checkpoint and new-offer lifecycle; do not
invent pause/resume. Test changed scope/checks under a fresh attempt so earlier
evidence cannot count toward its acceptance. Only rejected, blocked, failed,
cancelled, or expired attempts may be re-offered. An accepted task stays terminal;
new work after acceptance uses a new task. Separate pending-command recovery
from claims of exactly-once remote effects; inspect the existing crash window
between a remote mutation and local command-position persistence explicitly.

The custom-contract test must upload a distinct type/descriptor at runtime and
obtain a result from a compatible protocol worker or live provider. Loading only
the built-in sample is not proof of caller-defined contracts. Invalid results
must expose field/rule feedback before review, and evidence must distinguish a
check independently executed by the qualification harness from a worker report.

Goal 5's generated asynchronous workflow template and Goal 7's Jev adapter remain
separate work. The starter's reported checks are evidence to inspect, not an
automatic claim that an external reviewer independently ran them.
