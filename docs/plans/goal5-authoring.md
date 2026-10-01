# Goal 5: contract-driven pipeline authoring

Status: implementation in progress; original source inventory at
`c5dec4ca812c603e0706e315a3acb86be221265a`. Retry protection, verification helpers
and the sample reviewer landed in PRs #325, #326 and #327. Strict workflow
validation and promotion retry protection landed through PR #329, including
PR #328. Launch contracts, implementation and production extraction landed in
PRs #330–#332. Worker claim protection landed in #333; lookup/launch contracts,
action adapters and the opt-in coordinator mount landed in #334, #335 and #337.
External fixture contracts landed in #336; their service implementation is in
PR #338. See `goal5-starter-binding.md` for current evidence and remaining gates.
The third starter is not implemented or published yet.

## Outcome

An agent discovers and tests an external gRPC service, submits a workflow under
the caller's deliverable contract, and receives independent verification before
promotion. The accepted workflow can run asynchronously without another agent
planning each invocation. Fixed execution structure does not imply identical
external results or exactly-once side effects.

Reuse the existing `WorkflowDeliverable`, delegation candidate/review envelope,
workflow compiler and workbench, jobs store, and signed record format. Do not
create another workflow language or general orchestration service.

## Existing operations and required extensions

- **Existing:** `TaskSpec.contract` and `WorkflowDeliverable` carry the workflow,
  descriptor closure, fixtures, check evidence, recorded run and receipt.
  Annotations validate shape; independent verification of referenced content is
  a handler obligation. See `starter-contracts.md`, Agent workflow deliverable.
- **Existing:** workflow workbench registers compile, record, replay, promotion,
  export and verification in `WorkflowWorkbenchActions`. Promotion stores an
  immutable version; it is not proof that the reported acceptance tests ran.
- **Extended:** `SubmitWorkflow` and the Kafka `WorkflowRunRequest` share
  `WorkflowRunSubmitter`. Current insert deduplicates by UUID but returns the
  old row without comparing workflow/input. Define and test payload identity
  before relying on a retry as evidence that the same work was submitted.
- **Extended:** `CompleteStep` and `completeParkedStep` currently recognize an
  existing checkpoint by step name alone. Require payload equality for a retry;
  a different response must conflict without mutating job, checkpoint or outbox.
  The authoritative decision must occur under the existing database row lock.
- **Existing, to qualify:** workflow worker checkpoints, lease recovery and
  external completion. A successful remote effect before checkpoint persistence
  can be replayed; the example external service must enforce an explicit stable
  idempotency key and refuse its reuse with different content.
- **New starter binding:** independently resolve and hash deliverable artifacts,
  compare inline and stored workflow, compile against approved descriptors and
  endpoints, run fixtures, match reported checks to independently observed
  results, and verify that the signed record names that exact run and work.
  Only then allow review/promotion. Reuse existing APIs where possible; any new
  public contract must be reviewed and validated before its handler is added.

## Delivery order

1. Review retry identity and error semantics for submission and external
   completion, including concurrent requests and Kafka failure handling.
   Add failing regression tests, then implement the bounded jobs changes.
2. Wire an independently checked authored deliverable to the existing workbench.
   Verify the descriptor/import closure, artifacts, fixtures and receipt binding;
   reject altered content, unexecuted checks and a receipt from another run.
3. Add a small external gRPC fixture service and an authoring task. Exercise
   reflection/discovery, actual RPC tests, candidate submission, review, compile,
   recorded run, immutable promotion and asynchronous submission. Clearly label
   a scripted author; keep any live-agent proof separate.
4. Qualify durable asynchronous execution with PostgreSQL, restart/resume,
   completed-step preservation and the remote-effect/checkpoint crash window.
   Include an executable Kafka request-envelope example using the same job
   identity and inspect its resulting job and events.
5. Package the third image-only Compose starter, document the browser/protocol
   entry point, and qualify the published download on each advertised platform.
   Record local tests, hosted CI, merge, publication and deployment separately.

## Acceptance gates

Preserve AUTHOR-1's named requirements:

- `artifact_and_inline_workflow_match`
- `required_checks_independently_verified`
- `receipt_names_same_run`
- `job_payload_reuse_conflicts`
- `external_completion_reuse_conflicts`
- `restart_preserves_completed_steps`

Also demonstrate identical retries without duplicate events, racing different
payloads with one committed outcome, explicit remote-side idempotency during
recovery, the Kafka envelope path, and a clean install without a host JDK or
local image build. Invalid deliverables must not reach semantic acceptance.
Required contract changes must compile, validate fixtures with ProtoMolt's
runtime validator, and pass lint/compatibility. Do not describe the template as
available until its published package passes its acceptance checks.

## Open design decisions

- Submission identity must define named-workflow changes, descriptor/profile
  changes, JSON/protobuf normalization, and stored failed Kafka submissions.
- Completion retry identity must use the snapshotted output contract and compare
  the actual response, including the row-lock race after pre-validation.
- Verify the bridge between versioned protobuf workflows and the jobs runtime's
  stored workflow JSON before selecting the promotion/submission path.
- Separate asynchronous job identity from the recorded run and delegation task;
  bind them explicitly in evidence rather than treating their IDs as aliases.

## Reviewed first implementation

Sol's source review confirmed both retry gaps and found that validation failure
in `CompleteStepAction` currently calls unconditional `markFailed` after reading
the job. A concurrent successful completion can therefore be overwritten.

The first implementation compares immutable submission fields after insert
uniqueness resolution. Completion parses the response against the stored step
contract and preserves the submitted JSON for compatibility with existing
checkpoints. Retry equality is structural JSON equality (object member ordering
is ignored); alternate protobuf spellings are not promised equivalent. Under the row
lock, an existing matching response returns success, a different response
returns a conflict, and a validation rejection can fail only the still-waiting
matching step. Every losing request leaves checkpoint, status and outbox intact.
Malformed protobuf JSON remains an input error without mutation. No new RPC is
needed: the existing responses carry `ok`, `status` and `error`.

Sol owns submission changes; the coordinating agent owns completion changes,
contract decisions and final review. Regression evidence must include real
PostgreSQL concurrency in addition to in-memory action tests.

The current Kafka conflict behavior is fail-stop: the request consumer logs the
conflict and exits without committing that offset. The previous job and outbox
are preserved. An operator must resolve the conflicting record before restarting
the consumer; this change does not add a dead-letter topic or automatic recovery.
The starter must explain this limitation rather than imply continued consumption.

Local verification of this first slice: completion regression failed against
the old behavior, then passed after the fix. The final jobs suite passed 108
tests with no failures or skips, including PostgreSQL insert/completion races,
changed named definitions and legacy checkpoint retries. The full Gradle build,
Buf lint and compatibility checks passed. All hosted CI checks passed for
`a92ef98d8ec68a313db1388bc97af28ddcbce8e9`. PR #325 merged as
`3e7a36c14f898655eae0e611d8407a632c2da2ae`; GitHub and Forgejo main were synced.

## Authoring bridge findings

`WorkflowCompiler` compiles executable definitions to durable `Workflow` and
sanitizes endpoint addresses. No lossless reverse conversion to jobs submission
exists. Retain an executable source artifact or approved endpoint bindings, then
recompile and compare the durable workflow before submission. Do not infer
execution targets from worker-controlled receipt text.

Independent acceptance must compare every claimed artifact reference with the
repository's full reference, not just its digest. Resolve descriptor imports,
rerun required fixtures, compare enclosing and deliverable check evidence,
and verify the receipt with all artifact bytes supplied. Then bind the verified
manifest's run ID and workflow fingerprint to independently loaded run evidence.
Existing signature verification and self-reported PASSED checks alone do not
establish these conditions.

## Authoring contract and verification implementation

The draft `WorkflowAuthoringDeliverable` composes the existing deliverable with
an executable-source reference and named input/output fixture references.
`WorkflowAuthoringPolicy` is caller-owned: its artifact belongs in the immutable
task offer's context or equivalent trusted reviewer configuration. It pins the
descriptor artifact, expected fixtures, and exact target/method/TLS permissions.
The candidate cannot replace that policy. Fixture references are checked against
policy directly; the old deliverable's fixture list remains recorded-run evidence
and is not required to duplicate every acceptance fixture.

`WorkflowAuthoringPreflight` is a library helper, not a mounted API. It resolves
full artifact references, requires exact embedded descriptor bytes, validates and
compiles the source, compares the durable workflow, and checks every call against
policy before any execution. Its first fixture path supports synchronous unary
gRPC steps; structured-generation, external-completion and fan-out steps are
explicitly refused. The existing runtimes continue to support their own modes.
The retained executable source and durable workflow must enable workflow-level
contract validation so subsequent execution preserves that boundary.
All descriptor imports, including built-in option definitions, must be present
in the pinned artifact; general schema-resolver fallbacks are not admitted here.
Only successful preflight can construct the result accepted by fixture execution.

`WorkflowAuthoringFixtures` matches the exact caller-owned fixture list, loads
and validates all input/expected messages before the first RPC, then executes
the workflow and compares actual output messages. It validates mapped requests
before calls and successful responses before the next step, rejects unknown
fields, and stores observed outputs. Real unary gRPC tests cover mismatched
expectations, malformed bytes, annotation-invalid fixtures and responses.
References pin exact stored bytes, while invocation and output equality use
parsed protobuf messages; equivalent binary encodings are accepted explicitly.

`WorkflowAuthoringReceipt` authenticates a receipt, compares its exact projection
to independently loaded run evidence and the compiled fingerprint, and verifies
all referenced artifact bytes and metadata. It does not establish fixture success
or authorize a worker to supply its own run evidence. Callers must obtain that
evidence from the configured run repository.

The sample reviewer below supplies candidate/check/policy/run binding. Promotion
and asynchronous submission remain separate work. Tests of these helpers alone
do not qualify the third starter. The helper slice passed 163 workflow tests and eight starter
contract tests with no failures or skips, plus Buf lint and compatibility against
main. The wrapper also passes the existing dynamic deliverable-contract boundary
with its complete descriptor/import closure.

## Candidate review binding in progress

The sample `WorkflowAuthoringReviewer` implements the existing reviewer interface.
Its operator-selected policy reference must also occur in the immutable offered
task context. It validates the offered result contract and local sample shape,
requires the three supported authoring checks, and compares inner and enclosing
evidence by check name. It resolves reported artifacts without treating their
PASS text as proof, loads run evidence from the coordinator repository, verifies
the signed receipt and recorded fixture references, replays the run, then executes
the caller's acceptance fixtures independently. Unknown required checks are refused.

The acceptance verdict includes task, attempt, revision, exact candidate and offer
digests, policy digest, recorded run ID and verified manifest digest. The existing
coordinator rechecks the live candidate under its lock before applying that
decision. This review does not promote or submit work; those effects must follow
durable acceptance of that exact candidate. Transport/deadline and storage errors
cannot produce acceptance. Deterministic contract, mapping and fixture failures
request revision. This remains a sample adapter, not a mounted endpoint.

Validation parity uses the reviewed optional `validate_contract` workflow field.
False or omission retains legacy per-step/per-edge settings and serialized
fingerprints. Authoring preflight requires true, and the sample deliverable's CEL
rule checks the same durable flag before review. The jobs submitter validates
strict inputs before insertion. The shared runner validates strict inputs, mapped
requests, successful responses, resumed checkpoint values and final output;
external completion also validates before checkpointing. Replay applies the same
declared rules. The setting is retained in the executable source and durable
workflow fingerprint, so promotion cannot silently remove it.

Strict checkpoint inputs are parsed with pinned descriptors, including Java
callers that provide another descriptor with the same type name. Skipped resumed
steps preserve the last successful response. For strict fan-out, FAIL_FAST rejects
invalid projected inputs before any channel opens; CONTINUE retains only valid
successful branches under the existing policy. Workflow execution violations use
the existing nonretryable VALIDATION category. A preinsert input refusal creates
no job or event; the existing Kafka consumer may record a failed-at-birth envelope
through its separate failure path. Strict replay also checks derived final output
without an output artifact and refuses to certify an absent final output.

Local checks cover 174 workflow tests, 113 jobs tests and 48 sample tests, all
passing without skips, plus Buf lint, compatibility and refreshed browser
descriptor generation. This is not yet the async starter's restart, Kafka,
remote-effect idempotency or published-download qualification.
The full Gradle build also passed; the added schema-coverage assertions passed
in a separate targeted test after that build.
JSON Schema exposes `validateContract` as a boolean; the sample's requirement
that the nested durable flag be true remains runtime CEL, recorded in
`x-protomolt-cel`. No generator behavior changed. Artifact identity, policy
authority, endpoint permissions and lifecycle checks remain handler obligations.

Also qualify review infrastructure failure handling in the mounted starter. The
current coordinator keeps a thrown reviewer exception in `reviewFailure` and
leaves the candidate pending; that field is not currently exposed by its public
views. The sample correctly refuses acceptance on an unavailable fixture service,
but a usable starter still needs visible failure/retry handling rather than a
candidate that silently lingers. Cancellation and stale-review protection remain
owned by the existing coordinator.

Local acceptance-adapter verification: all 48 sample tests passed with no failures
or skips, including eight authoring-review tests. The delegation suite's 179
passing tests were restored from the Gradle cache for unchanged delegation code.
The new integration submits a scripted worker's candidate through the gRPC
delegation service, observes the independent fixture rerun and accepted frame,
then reconstructs a coordinator from a shared in-memory transcript repository.
This proves accepted attempt/revision restoration in that test; it is not yet a
process-restart, external-worker, async-job or published-starter qualification.

## Acceptance-to-execution binding

Promotion stores `VersionedWorkflow`; jobs currently execute a snapshotted JSON
definition. The starter must bind both representations rather than submit a
mutable registry name. Load acceptance from the trusted `TranscriptRepository`,
reduce the complete transcript without findings, and select the matching offer,
candidate and terminal acceptance by task, attempt and revision. A reviewer's
return value or display verdict is not a durable acceptance record.

Before the first promotion, independently verify the accepted candidate under
the pinned policy and validate the launch input. Persist a structured launch
authorization at a recoverable, write-once launch identity before any promotion
or job insertion. It must bind the offer and candidate digests, policy, source,
input, exact promoted envelope and job UUID. Recovery must verify these bindings
and reuse the authorization instead of repeating live fixture calls after a
partially completed launch. Changed content at the same identity must conflict.
The sample implementation below provides this persistence mechanism; it is not
an available endpoint.

Use the caller's launch UUID as the keyed authorization and job identity. An
intentional new input needs a new launch UUID. Derive the promotion version from
the accepted task/attempt/revision and offer/candidate identity, and use the
accepted frame's timestamp in its envelope, so separate launches of the same
accepted workflow agree on promotion bytes. The authorization store needs atomic
create-if-absent with exact-content conflict detection; content-addressed artifact
storage alone does not provide that keyed guarantee. Bind the accepted frame or
prefix rather than the whole transcript, which can grow with unrelated tasks.

The promotion prerequisite now returns the stored envelope and original timestamp
for identical retries, including a concurrent identical winner. Previously each
call generated a fresh timestamp, which conflicted with whole-envelope immutable
storage. Different workflow bytes under the same name/version remain a conflict.
The action now also renders the envelope into its declared `Struct` response
instead of placing a different message type into that field. The real MCP/Git
registry regression failed on the old retry behavior and passed with the fix;
focused tests cover changed content, race winners and storage failures.
Registry visibility now requires matching committed HEAD bytes. A failed Git
commit leaves its file and index untouched; that file is not a promoted version
until committed. Existing registry commits still include the whole index, so a
later registry write can include a previously staged path. This change does not
promise one dedicated commit per promotion. Registry failures are mapped through
the repository's declared I/O boundary. Local affected suites passed: 180 workflow
tests, 101 registry tests and 129 server tests (one opt-in live-provider test skipped).

Acceptance tests for the binding must cover absent, revised, cancelled and stale
acceptance with no side effects; a manual acceptance that fails independent
verification; restart after authorization and after promotion but before job
submission; exact retry producing one version and one job acceptance event;
changed source/input under a reused launch identity; and durable evidence tying
the job source snapshot to the promoted workflow. These tests precede the full
PostgreSQL, Kafka, remote-effect and published-starter qualification.

## Launch contract slice

`transform/workflow/authoring/src/main/proto/ai/protomolt/proto/samples/starter/v1/workflow_launch.proto`
owns the four launch messages, with
no new service or mounted endpoint. `WorkflowAcceptedCandidate` identifies the
accepted task, attempt and revision and pins deterministic digests of its
TaskSpec, CompletionCandidate and accepted TranscriptEntry.
`WorkflowAuthoringLaunchRequest` requires a caller UUID and an input artifact.
`WorkflowAuthoringLaunchAuthorization` reuses that request, the policy reference,
the existing authored deliverable and VersionedWorkflow. The result names the job
UUID and authorization artifact; it asserts job existence, not execution success.

Sol reviewed the identity, validation and retry design before runtime fixtures.
Request/response validation covers required fields, UUIDs, digests, attempt and
revision bounds, artifact media/redaction/size rules and promoted-workflow equality.
JSON Schema can expose field constraints; CEL relationships remain runtime rules.
No OpenAPI endpoint or generator change belongs to this sample contract slice.

The handler must perform the following checks beyond annotation validation:

- Reload and reduce the trusted transcript; require terminal acceptance of the
  selected attempt/revision and matching worker, offer and candidate. Reject
  unknown fields in the selected messages, including the decoded typed result.
- Compute identity digests with deterministic protobuf serialization of those
  parsed messages. These server identities are not cross-language canonical
  protobuf hashes. Check the full referenced artifact metadata and bytes.
- Before first authorization, independently verify the accepted deliverable,
  pinned policy, fixtures, run and receipt. Parse and validate launch input under
  the pinned descriptor before live fixture calls or launch side effects.
- Derive the version as `accepted-` plus the accepted-identity message digest;
  copy the accepted coordinator frame's validated `sent_at` into `created_at`.
  Validate the promoted workflow fingerprint and exact compiled bytes.
- Atomically create or compare authorization under the launch UUID. Reject a
  changed intent without promotion or insertion. A crash or competing verifier
  before this write may repeat fixture calls; fixture services need their own
  idempotency where calls have effects.
- On recovery, validate the stored authorization, current acceptance and pinned
  artifacts without rerunning live fixtures. Reuse its promotion envelope and
  submit the exact admitted inline JSON/input with the launch UUID as job UUID.
  Confirm a matching job and committed version before returning the result.
- Resolve the result artifact back to the exact keyed authorization and check
  `job_id == request.launch_id`. The caller must not supply its own authorization
  as a substitute for the trusted keyed record.

Malformed input, unaccepted/stale identity, failed independent checks and identity
conflicts stop before launch effects. Storage or transport failures propagate and
cannot produce a success result; retry uses the same UUID. Missing artifacts and
unsupported validation rules fail closed. A lost response or client disconnect
does not revoke persisted authorization. Delegation has cancellation, but the
asynchronous workflow job store currently has no cancellation operation or
cancelled state. Job and pre-insertion launch cancellation are unsupported in
this slice and need explicit contracts before implementation. Their acceptance tests
must prove crash recovery and concurrent same/different UUID payload behavior
before the starter is described as available.

Local contract checks passed: complete-import Java generation/compilation, Buf
lint and compatibility, and all 55 sample tests with no skips. Six new runtime
fixture tests cover valid requests/results, missing fields, UUID/hash formats,
attempt/revision bounds, inclusive 4 MiB limits, redaction/media rules and
promoted-workflow equality. Generated launch JSON Schemas expose UUID/required
constraints and preserve CEL as `x-protomolt-cel`; no endpoint is advertised.
Fabricated but well-formed hashes deliberately pass annotation fixtures, making
the remaining trusted-storage verification obligation explicit.

## Sample launch implementation and remaining qualification

`WorkflowAuthoringLauncher` reloads durable acceptance through
`WorkflowLaunchAcceptance`, checks the exact requested identity, verifies pinned
artifacts and validates input before live fixtures or launch effects. Initial
authorization requires independent fixture execution even if the task was
accepted manually. Recovery checks the stored authorization and evidence again,
reuses its promotion envelope, and does not call live fixtures. The result is
validated before promotion and returned only after a matching committed workflow
version and job are present. This remains a sample helper with no mounted RPC.

`FileSystemWorkflowLaunchAuthorizationRepository` uses an operator-owned directory,
process and JVM locks, bounded deterministic protobuf records, atomic rename and
file/directory fsync. Unsupported filesystem durability operations fail the call;
there is no non-atomic fallback. This is trusted local coordinator storage, not
authentication against an operator replacing a valid record. Concurrent ledger
tests use separate instances in one JVM; power-loss and multiple-process behavior
still need deployment qualification.

Promotion and job insertion are separate operations. An unrelated submitter can
claim the launch UUID after the launcher's initial lookup. The launcher then
fails without changing that job, but authorization and promotion can remain.
Retries reject the different job. No atomic transaction across ledger, Git and
job storage is promised. Before authorization, competing verifiers may repeat
fixture calls; external effects still require service-side idempotency.

Local verification passed all 67 sample tests without failures or skips. The
launch integration uses real delegation and fixture gRPC calls, filesystem
authorization/artifact storage and a Git workflow registry, with simulated job
persistence behind the real submitter. It covers failure after promotion before
insertion, lost response after insertion, reopened stores with fixtures unavailable,
matching retries, changed intent, unbound jobs and an unrelated insertion race.
Separate tests cover accepted identity selection and ledger conflicts/corruption.
Additional recovery checks cover authorization persisted before a failed promotion
and refusal of a lifecycle-clean changed candidate under an existing authorization.
Selected offer and candidate envelopes also reject unknown protobuf fields; that
regression failed before the boundary fix and passes with it.
This does not qualify PostgreSQL execution, process restart, Kafka or a starter.

A direct-executor in-process acceptance test hung during worker teardown; the
test now uses ordinary in-process executors and passes. No production runtime fix
is claimed. Mounted starter qualification must exercise worker disconnect and
shutdown as well as visible review failure/retry behavior.

Next: finish review and CI for this binding, then qualify the external-service
authoring task, persistent asynchronous execution and crash-window idempotency,
Kafka submission, and the image-only starter on native AMD64 and ARM64. Job
cancellation remains unsupported and must not be presented as available.

## Worker claim protection

Recovery testing reproduced a stale worker completing again after an expired
lease had been reassigned and the replacement attempt completed. Worker writes
now require the original immutable `WorkerClaim` (job UUID, owner and positive
attempt). JDBC checks RUNNING state, owner, attempt and an unexpired lease in the
same update transaction as its outbox event. Lease and retry timing use the
database clock. Rejected writes throw `ClaimLostException` without mutation;
the worker stops without failing or requeueing the replacement attempt, including
when the checkpoint observer wraps that exception. External parked completion
keeps its separate WAITING-state row-lock gate.

The six worker mutation signatures intentionally no longer accept a bare UUID.
Internal store implementers must enforce the claim rather than reload a newer
claim for an old caller. Remote effects already performed cannot be undone by
this guard; retried external services still need idempotency keys. This does not
add lease renewal or change configured workflow deadlines.

Local jobs validation passes 117 tests with no skips, including PostgreSQL and
Kafka. Tests reject stale owners, old attempts and expired leases across all six
worker writes without row/event changes; a checkpoint observer losing its claim
does not settle the replacement. A separate PostgreSQL test reconstructs the
store and worker after remote success but failed checkpoint persistence, preserves
the prior checkpoint and deduplicates the repeated request in its fixture service.
That fixture's deduplication is in memory and the gRPC transport is in-process;
external-service durability and process-kill qualification remain required.
The reviewed helpers and two contract files were extracted in PR #332 to
`protomolt-workflow-authoring` so production hosts need not depend on samples.
The protobuf files are unchanged, including their import paths, descriptor names
and generated Java package. Handwritten Java uses the module's ADR-002 package.
All 67 authoring/sample tests pass after extraction, and complete descriptor-set
bytes match the pre-extraction baseline. Compatibility CI compares complete
descriptor sets with Buf FILE rules; a deliberately removed launch file fails
that check. No service is mounted by this extraction.
