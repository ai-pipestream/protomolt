# Goal 5 starter binding: remaining implementation

This is a design inventory, not an available API. Source examined:
`f8b6382469502537f95fc6dcd03750ad8adfcb8b`. The launch helper is in PR #331.

## Reuse and ownership

- Keep delegation offers, candidates, acceptance and transcript storage in the
  existing coordinator. `apps/serve/DelegationRuntime` currently constructs a
  manual reviewer even with persistent repository storage. Add explicit trusted
  authoring-reviewer configuration rather than assuming sample acceptance is
  mounted or changing the default review policy for unrelated tasks.
- Keep workflow jobs in `JdbcWorkflowRunStore` and `WorkflowRunWorker`.
  `ProtoMoltServe` already opens the jobs database, mounts its operations and runs
  the worker and optional Kafka relay. Do not add a second queue or job language.
- Keep artifacts, run evidence, versioned workflows and signing in their existing
  stores. The launch authorization directory needs its own persistent volume.
  A signed receipt is evidence for a recorded run, not a substitute for acceptance
  or a claim that a later asynchronous run completed.
- Production cannot depend on `samples`. Before mounting, extract the reviewed
  authoring contracts and helpers to a production module, preserving the existing
  wire identities and import paths or recording an explicit migration. Decide
  ownership before adding a serve dependency; do not copy implementations into
  the server. Sample scripted-worker and fixture code can remain demonstration
  code, packaged separately from the coordinator.

## External service and scripted author

Use a small separate TCP gRPC service with reflection and two unary operations:
a pure text normalization followed by a durable write. The write request carries
an explicit operation UUID and bounded content; the response carries that UUID
and the content digest. Proto annotations validate UUIDs, required fields and
size limits on both boundaries. Matching response identity and digest are handler
checks, as is atomic persistence of the operation key and response.

The write service returns the stored response for a matching retry and rejects
the same key with different content. Its durable record is the demonstrated
effect. It must commit before returning success and survive service restart.
Do not imply the workflow engine creates exactly-once behavior for arbitrary
services. The authored mapping must carry the stable operation UUID from input;
the current worker does not automatically inject a job/step id into RPC messages.
Use distinct operation IDs for fixture cases and actual job input.

The scripted author must discover the service through reflection, invoke actual
RPC tests, retain the full descriptor closure, compile its source, record a run
and receipt, and submit the existing authored deliverable through delegation.
It must not submit canned evidence marked PASSED without running those checks.
The independent reviewer runs the pinned caller fixtures. Label the author as
scripted in the console and tutorial; any model-provider demonstration is a
separate result.

## Protocol and browser entry

Retain the existing delegation and workbench operations. Mount a narrowly scoped
accepted-workflow launch operation only after its production contract is reviewed;
reuse the reviewed request/result and bind the coordinator-owned authorization
store. Expose it through the existing action/gRPC bridge so MCP and ACP use the
same validation and handler. Do not present a generic named SubmitWorkflow call
as enforcing independent acceptance: it has a different authority boundary.

The browser path should be: start the scripted authoring task, inspect observed
checks and any requested revision, launch the accepted workflow with input, then
inspect the durable job, checkpoints and evidence. Infrastructure review failure
must be visible with a bounded retry action bound to the current attempt/revision.
The coordinator currently retains `reviewFailure` privately. Public status and
retry semantics need reviewed contracts and stale-result tests before wiring.
Do not solve the hidden failure by marking a candidate accepted or by silently
retrying external calls indefinitely. Job cancellation remains unsupported.

## Qualification sequence

1. Real PostgreSQL worker/store reconstruction preserves completed checkpoints.
   Fail after the second RPC's remote effect but before its checkpoint; retry
   must repeat the same operation key and produce one durable external effect.
   This test does not substitute for killing the running worker process.
2. Repeat through the packaged worker with process termination at that boundary,
   restart it and inspect checkpoints, job status, external records and events.
   Also restart the fixture service to prove its idempotency record is durable.
3. Submit a documented Kafka envelope using the same stored executable source,
   input and job UUID as the accepted launch. Verify duplicate delivery does not
   create another acceptance event; a changed payload conflicts. Kafka's named
   workflow resolver must resolve that exact source, not a mutable replacement.
   The existing consumer stops on a conflict; document recovery explicitly.
4. Exercise the browser and supported protocol paths from the third image-only
   Compose template. Test a visible review infrastructure failure and recovery,
   invalid deliverables, refused stale review, valid launch and restart.
5. Publish only after green CI, exact-source image builds, anonymous artifact
   download/pull and native AMD64/ARM64 acceptance. Record digests and source;
   keep a local test, hosted CI, merge, release and deployment as separate facts.

Existing `WorkflowRunKafkaIT` already covers a broker request through PostgreSQL,
real TCP gRPC execution and validated lifecycle events. Extend that path for the
accepted workflow example; it currently uses an unrelated two-step embedding
fixture and does not establish acceptance binding or crash-window idempotency.
