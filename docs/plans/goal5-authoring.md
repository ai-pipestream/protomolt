# Goal 5: contract-driven pipeline authoring

Status: implementation plan and source inventory at `c5dec4ca812c603e0706e315a3acb86be221265a`.
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
