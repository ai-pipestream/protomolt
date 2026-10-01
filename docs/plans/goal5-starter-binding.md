# Goal 5 starter binding: remaining implementation

This is a design inventory, not a mounted API. The launch helper and production
authoring module landed in PRs #331 and #332. The service contracts are in PR #334.
The action adapters now validate requests and successful replies with the native
engine, bind reply task/job identity to the request, and sanitize typed failures.
Catalog tests cover generated, dynamic and JSON input; in-process gRPC tests cover
operator authentication, scoped denial, validation and status/trailer mapping.
The real launcher is also exercised through the catalog without repeating fixtures
on a matching retry. These tests do not qualify a deployed MCP/ACP or browser path.

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
- Production cannot depend on `samples`. Extract the reviewer, launcher,
  acceptance/validation helpers and keyed store to `transform/workflow/authoring`
  (`protomolt-workflow-authoring`), using Java package
  `ai.protomolt.proto.workflow.authoring`. Keep generic preflight, fixture and
  receipt helpers in workflow core. Move the two sample authoring/launch proto
  files without changing import paths, descriptor names, generated Java names or
  Any type URLs. Record their frozen generated-package exception in AGENTS;
  handwritten implementation follows ADR-002. Both samples and serve can depend
  on authoring without a cycle through jobs-service or workflow core. Add the
  production module to the BOM. The scripted author and external fixture remain
  separately packaged demonstration code.

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

Retain the existing delegation and workbench operations. Add a contributed
`ai.protomolt.proto.workflow.authoring.v1` service with
`LaunchAcceptedWorkflow`, reusing the reviewed request/result and binding the
coordinator-owned authorization store. Review that contract before its handler.
Register its action and descriptor through the existing catalog/contributed-service
bridge, so gRPC, MCP and ACP use the same validation and handler. This avoids the
dependency cycle a central service-contract RPC would introduce. Do not present a generic named SubmitWorkflow call
as enforcing independent acceptance: it has a different authority boundary.

`GetAcceptedWorkflow` takes a validated task UUID and returns the existing
`WorkflowAcceptedCandidate` selector computed by the server. It performs no
fixture calls or launch effects and does not certify semantic correctness. Both
methods preserve the exact task ID spelling returned by delegation: existing
task IDs are string keys, not UUID aliases. Only the launch/job UUID is normalized.
The caller must not reconstruct server-computed protobuf hashes in another
language. Both operations initially require the trusted operator caller boundary;
the coordination worker token alone must not grant either operation. Authorization
must precede transcript lookup, including the read-only selector query.

Before mounting, add typed handler failures and adapter tests for these mappings:

- Invalid request annotations or unsupported rules: INVALID_ARGUMENT.
- Missing, nonaccepted or stale selected task: FAILED_PRECONDITION, with no effects.
- Reused launch UUID with different intent or a conflicting job: ALREADY_EXISTS.
- Unavailable or timed-out fixture/repository transport: UNAVAILABLE or
  DEADLINE_EXCEEDED, retaining the original launch UUID for a retry.
- Storage corruption or an unclassified storage error: INTERNAL; do not label it
  as a successful launch or infer that no partial commit occurred.
- An invalid successful response: the existing catalog boundary rejects it as
  DATA_LOSS before exposing success.

MCP/ACP must preserve the corresponding stable action error code and must not
convert infrastructure failure into acceptance. Add error categories explicitly;
do not classify failures by matching exception prose. No handler or mount is
provided by the service descriptor alone.

The browser path should be: start the scripted authoring task, inspect observed
checks and any requested revision, launch the accepted workflow with input, then
inspect the durable job, checkpoints and evidence. Infrastructure review failure
must be visible with a bounded retry action bound to the current attempt/revision.
The coordinator currently retains `reviewFailure` privately. Public status and
retry semantics need reviewed contracts and stale-result tests before wiring.
Do not solve the hidden failure by marking a candidate accepted or by silently
retrying external calls indefinitely. Job cancellation remains unsupported.

## Qualification sequence

Release prerequisite found during recovery review: worker mutations in
`JdbcWorkflowRunStore` currently update by job UUID alone. A PostgreSQL regression
reproduced an expired attempt completing again after its replacement completed.
Before restart qualification, bind worker writes to an immutable job/owner/attempt
claim and check RUNNING state and lease validity atomically with the mutation and
outbox insertion. A rejected stale write must not trigger another stale failure
or requeue. External parked completion retains its separate row-lock contract.
This guard is not implemented yet; see the local `feat/goal5-claim-fencing` work.

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
