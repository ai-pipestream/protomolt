# Starter contracts: Goal 1

This document records the reviewed Goal 1 contracts. Goal 2 now has a local
[recorded correction sample](../tutorials/recorded-correction.md) using those
contracts. It does not mount an evaluation service, implement a Jev adapter, or
change the validation or structured-generation engines.
See the [operation inventory and implementation backlog](starter-contract-inventory.md)
for existing, extended, and new operations. The baseline is main
`dc49800cd66950356a883d99141e00427df6ad65`. This is the first goal in the agreed
sequence: contracts, data correction, easy startup/protocol consistency, agent
coordination, authored workflows, comparison evidence, optional Jev adapter.

The [existing court experiment and browser workbench](starter-contract-inventory.md#existing-court-experiment-and-frontend)
are evidence and reuse candidates, not architectural requirements. The small
contact example below is a fast contract fixture. Carry forward court enrichment
as a representative regression scenario for incomplete and unsupported results;
do not require its historical harness or frontend interaction model for first use.

## Design independence and two API designs

### Inventory review before implementation

Keep the seven-goal order, but reconcile the existing capability inventory before
starting Goal 2 handlers or committing to the Goal 3/4 app architecture. Goal 1's
compiled contracts and fixture results remain valid; the earlier operation
inventory is not proof that all reusable product capabilities were considered.

Use `settings.gradle`, the documentation index, generated action inventory,
application routes, examples, deployment manifests, and existing website research
as coverage indexes. For each capability family record its source and tests,
runtime wiring/configuration, evidence level (source, local test, or live proof),
reuse/extend/replace/defer decision, and affected goal. Check documented limitations
against code. A missing entry in the RPC inventory is not evidence of missing
implementation. Deep inspection is required for components on the proposed
starter and coordination paths; unrelated families may be explicitly deferred
with a reason.

Include the existing document-platform composition and role-node implementation
in the packaging comparison:
[DocumentPlatform](../../apps/document-platform/src/main/java/ai/protomolt/proto/platform/DocumentPlatform.java),
[DocumentPlatformConfig](../../apps/document-platform/src/main/java/ai/protomolt/proto/platform/DocumentPlatformConfig.java),
and [its Compose deployment](../../deploy/document-platform/compose.yml).
This is a reuse candidate, not a requirement to load the entire document platform
for a correction task. Its documented startup still builds a local distribution.

The review is complete when every capability family has a disposition and every
proposed starter dependency has source-backed ownership, configuration, and an
acceptance case. Then revise only the affected contracts/backlog items. This is
a bounded design gate, not a demand to rerun every integration or provider test,
and not authorization to implement the remaining goals.

The completed [coverage reconciliation](inventory-coverage.md) and its complete
module roster record this review. Source inspection is explicitly separated from
fresh test results and future integration acceptance cases.

### App entry-point comparison

The user reports that the earlier court experiment and frontend worked, but
delegated tasks sometimes lingered awaiting intervention or pursued the wrong
direction, and setup was confusing. These are reported experiences, not a proven
diagnosis of a particular state transition. Network request counts alone do not
measure whether coordination is effective.

Compare two designs before choosing the app's primary interaction model:

1. **Workflow API:** refine the contracts below around named workflows, typed
   inputs/deliverables, validation, bounded correction, evaluation, and recorded
   outcomes. Reuse existing workflow/job operations rather than adding a second
   execution service. Interactive task orchestration is not mandatory for this
   path.
2. **Coordination API:** refine existing task, worker, checkpoint, guidance,
   candidate, and review operations around explicit ownership and actionable
   progress. Preserve useful distributed execution and durable state, while
   allowing a simpler app-facing interaction model.

These are design alternatives for app entry points, not authorization to build
two replacement backends. Share schema resolution, validation, projections,
evaluation identities, evidence, and receipts where their semantics match. Keep
task and run lifecycles explicit; an adapter must define their mapping rather
than collapse them into an ambiguous status. No new public RPCs are specified by
this comparison alone.

Review both designs against the same scenarios: first task from a clean install,
invalid deliverable, schema-valid but unsupported answer, ambiguous assignment,
requested scope change, worker disconnect, coordinator restart, stale review,
and a task waiting for human input. For each scenario identify the actor that
owns the next action, the evidence available, and the recovery operation. A
waiting task should expose why it is waiting, who must act, and the bounded
retry/escalation policy; human approval must never become automatic after a
timeout. Changes to scope or acceptance criteria need explicit revision identity
and must not inherit earlier acceptance evidence.

Treat these as proposed acceptance requirements until implementation and tests
establish them. Measure setup steps, time to first recorded result, recoverability,
and unnecessary user interventions before choosing the app default. Court data
and frontend components can help prove the designs without fixing their shape.
The review-binding patch remains a prerequisite for automatic external review.

## First workflow: correct-contact

The [stored-definition fixture](../../samples/src/main/resources/starter/correct-contact.workflow.json)
uses the existing authoring `CompiledWorkflow` and `SchemaSource.type`. Register
the [correction schema](../../apps/correction/src/main/proto/ai/protomolt/proto/correction/v1/correction.proto)
and full import closure before resolving that type. A real host must resolve and
pin the descriptor closure before checking/compiling the definition; a mutable
registry name alone is not an immutable contract. Sources or descriptor-set input
are alternatives supported by the same existing `SchemaSource` envelope.

The caller then uses `RunWorkflowRequest.workflow_name = "correct-contact"` with
input in `RunWorkflowRequest.input`, or configured `SubmitWorkflow` with an
explicit job UUID. The fixture is checked and compiled in tests; it is not
automatically installed in a server. `starter-correction` is a model catalog
configuration name, not a bundled provider or a live-test claim.

The workflow accepts bounded `RawContact` data, including malformed contact text.
Its typed edge copies the source identity, contact text, and internal notes, then
projects into `ContactGrounding` and validates before generation. The projection
excludes internal notes from the model's grounding. Internal notes do not reach
the model. The structured step fills `CorrectedContact`, with at most three
attempts and runtime validation before returning it. Existing structured output
and workflow machinery are sufficient; no additional GenerateStructured RPC is
needed for this template.

The target permits an absent email only when `needs_review` is true. This is a
schema-valid unresolved result, not an automatically accepted correction. The
starter's acceptance policy must check record ID equality, evidence
support, and `needs_review`; unresolved or unsupported corrections go to human
review. A syntactically valid invented email can pass field rules, which is why
the fixtures distinguish validation from semantic judgment.

Goal 2 connects this definition to recorded runs, receipt signing/verification,
and optional semantic evaluation. Recording must use configured artifact/run
storage; a bare `RunWorkflow` success does not imply a signed receipt. Keep the
permitted source/result artifacts and bounded generation-attempt provenance, then
use existing record operations.
Do not add an unimplemented evaluation step to the checked fixture.

### Input authority and evidence

For this first fixture, the authority is the submitted immutable `RawContact`
message and its derived `ContactGrounding`, not a mutable document ID or a search
hit. Record the workflow/version and complete descriptor closure, then bind the
exact stored permitted source, grounding, result, and policy artifacts to the run.
The evaluator resolves this context through the authoritative run or task offer;
`evidence_sha256` identifies the exact stored evidence `Any`, not an independently
asserted account of what the provider saw. Replacing source bytes or a projection
requires new evidence and evaluation; record ID equality alone is insufficient.

Existing `RunEvidence.input_artifact`, step request/response artifacts, and edge
fingerprints supply the recording primitives. The
[recorder](../../transform/workflow/src/main/java/ai/protomolt/proto/workflow/WorkflowRunRecorder.java)
stores sensitivity-redacted snapshots and bounded attempt provenance, not an
unrestricted raw transcript of every model attempt. Goal 2 must define which
permitted bytes are retained and independently reconstruct the actual grounding
from that record. If redaction prevents the required support check, report
insufficient evidence; do not infer removed values or disable redaction merely
to make replay pass. Projection exclusion from a provider is distinct from
storage/logging policy.

The minimal correction has no retrieval, indexing, or screening dependency.
For the later court/document scenario, pin document version/content digest and
any parsing/chunk/index/policy versions that influenced retrieval. Define cited
span offsets against the identified text encoding and normalization before
testing quotations; an index hit or a filename is not a source citation.
Mechanical validity, source support, disclosure policy, and human acceptance
remain separate decisions. A valid incomplete result goes to review.

## Agent workflow deliverable

The caller can set `TaskSpec.contract` to the descriptor closure and full name of
[`WorkflowDeliverable`](../../samples/src/main/proto/ai/protomolt/proto/samples/starter/v1/workflow_deliverable.proto).
The worker packs it into existing `CompletionCandidate.result`, retaining the
existing attempt, revision, check evidence, and output-reference envelope.

The sample carries the existing durable `Workflow`, content-addressed workflow
and descriptor artifacts, input/output fixtures, existing `CheckEvidence`, a run
ID, and an artifact containing an existing `SignedWorkRecord`. This does not add
a second workflow language, receipt format, or task lifecycle. Its annotations
refuse empty workflows, absent evidence, failed/duplicate checks, and missing
artifact references. These remain worker reports until checked independently.

Before acceptance the reviewer must also verify that:

- Stored workflow bytes match the inline workflow and its artifact digest.
- The descriptor artifact includes every import, annotation, and selected type.
- Workflow checking succeeds against those descriptors and approved endpoints.
- Reported checks exactly cover `TaskSpec.required_checks` and match the enclosing
  candidate evidence; selected acceptance tests actually ran on these artifacts.
- The recorded run uses the same workflow, descriptors, fixtures, and outputs.
- The signed record verifies under the configured trust snapshot and its manifest
  refers to that work. An artifact's SHA-256 syntax alone establishes none of this.

## Provider-neutral evaluation

[`EvaluationService.Evaluate`](../../inference/proto/src/main/proto/ai/protomolt/proto/inference/v1/evaluation.proto)
is a new unary contract in the existing inference contract module. It has no
handler, catalog action, ACP command, or MCP tool in this change. Generated gRPC
stubs are contract artifacts, not an available endpoint.

`EvaluateRequest` contains a caller-scoped UUID, an immutable binding, projected
evidence, a rubric, and a configured evaluator profile. The binding names either
an agent task/attempt/revision or a recorded workflow run, and binds result,
descriptor, evidence, projection, rubric, and policy digests. `EvaluateResponse`
contains only a successful judgment: it echoes the identity/binding, returns one
typed answer per question, and identifies the actual provider/model/version.
There is no provider-controlled acceptance boolean.

Questions independently request choice, ordinal score, or probability. Choice
options are unique labels. Score levels are unique labels in ascending order;
the score is the probability-weighted zero-based level index. Probability answers
are the probability that the supplied proposition is true. Explicit zero is valid;
absence, nonfinite values, and values outside the declared ranges are invalid.
Provider confidence is optional and must not be invented when unavailable.

### Required validation boundaries

1. Bound serialized request/response size to 1 MiB before parsing. Reject unknown
   fields in this new contract. Resolve configured profiles under caller authority;
   keep credentials outside requests and evidence. Treat evaluation payloads as
   internal data, as their metadata annotations declare; apply access control and
   configured redaction to logs and stored artifacts.
2. Run ProtoMolt request validation, then resolve authoritative stored artifacts.
   Recompute all SHA-256 identities over their exact stored bytes. The descriptor
   budget is 4 MiB, including imports, bounded before parsing/resolution. Pin full
   descriptor and projection configuration closures, not just the root file.
3. Unpack the stored candidate against its declared result type and run its
   rules before provider invocation. Independently derive the permitted evidence
   projection from that candidate/context. Match the submitted evidence to it;
   never let a caller validate a digest against another caller-controlled claim.
   For task candidates, resolve the immutable offer for that exact task/attempt.
   Require `descriptor_sha256` and `result_type` to match its `TaskSpec.contract`,
   and require the pinned policy/check evidence to cover the offer's required
   checks. The transcript's offer is authoritative; a separately supplied valid
   descriptor is not a replacement contract. A changed scope, contract, or check
   set requires a new offer/attempt and fresh evaluation. For workflow subjects,
   resolve the same recorded run's pinned output contract instead. These checks
   require stored state; no extra caller-asserted contract digest replaces them.
4. For evidence, require a canonical `type.googleapis.com/<full-name>` type URL
   (at most 512 characters), resolve it in the pinned projection closure, parse
   its payload, and validate it. A required `Any` envelope alone does not validate
   its contents. A zero-length payload is legal only if the resolved message's
   rules accept its default value. An empty or unknown type URL is inadmissible.
5. Snapshot the evaluator profile's resolved provider/model, parameters, and
   configuration digest with the request. Retries must use this snapshot even
   if an administrator changes the mutable profile name. Check active lifecycle,
   authorization, policy availability, and evidence completeness before invocation.
6. Run ProtoMolt validation on every successful response before returning it or
   applying policy. Require exact request ID and binding equality. Match question
   IDs and kinds one-to-one; reject missing, extra, or duplicate answers. Choice
   distributions must contain exactly the supplied options; score distributions
   must preserve the requested level order. Sum probabilities within 1e-6 of 1;
   verify weighted scores within 1e-6. A choice must be one of the requested options.
7. Apply the pinned local acceptance policy. Store the complete request, resolved
   configuration identity, response, and policy result as evidence. Under the
   coordinator's review lock, recheck task/attempt/revision and immutable result
   identity before issuing a decision. Late, canceled, stale, or incomplete work
   cannot accept a candidate. Workflows likewise require the same recorded run.

Only steps expressible within one message are annotation checks. Stored digest
resolution, request/response matching, freshness, provider availability, and
semantic truth require handlers, policy, or review. The test suite explicitly
shows that an empty `Any` can pass envelope validation; this is not permission
for the future host to skip step 4. No global change to existing response handling
is made here; the new service's response gate is mandatory when it is implemented.

### Failure, retry, and cancellation contract

The proposed service has no success-shaped failure message. Define these gRPC
statuses in its handler, with stable diagnostic codes and field paths in error
details; avoid echoing sensitive raw evidence. Equivalent catalog error envelopes
must retain the same code and paths when MCP/ACP mounting is added.

- `INVALID_ARGUMENT`: malformed request, `contract_violation`, or
  `evidence_type_invalid` (including undecodable payload).
- `NOT_FOUND`: `artifact_missing`, unknown registered type, or unknown profile.
- `FAILED_PRECONDITION`: `unsupported_rule`, unavailable policy/schema dependency,
  `digest_mismatch`, or `candidate_stale`. None is a favorable judgment.
- `ALREADY_EXISTS`: `idempotency_conflict`, same caller/operation/request UUID with
  different deterministic request bytes. Identical completed retries return the
  original recorded response. In-flight identical retries attach to the same work.
- `UNAUTHENTICATED` / `PERMISSION_DENIED`: caller admission or authority failure.
- `RESOURCE_EXHAUSTED`: request/descriptor budget or provider rate limit. Include
  retry guidance where available; do not silently shorten the rubric.
- `UNAVAILABLE`: retryable provider/transport failure. `INTERNAL` with
  `evaluator_response_invalid` means a malformed upstream result; never a success.
- `DEADLINE_EXCEEDED` / `CANCELLED`: no subsequent review mutation from this request.

Deterministic request serialization is for deduplication within this contract;
embedded Any payload bytes remain exact. Persist the UUID association and profile
snapshot before invocation. Retry at most three transport attempts within the
original deadline, with bounded backoff and provider retry hints. Keep structured
generation's separate three-attempt repair budget distinct. A provider response
lost before durable recording can cause another billable provider call; do not
promise exactly-once inference. Completed results and committed review decisions
must not be duplicated. Canceled or terminally failed UUIDs retain their recorded
terminal status; an intentional new attempt requires a new UUID and fresh binding.
Define a cancellation/decision race by whichever commits first under the review
lock: cancellation cannot undo a previously committed decision.

`RunWorkflow` remains synchronous and does not promise idempotency. Existing jobs
continue independently of a submitter disconnect; no new job-cancel operation is
invented here. Uncheckpointed workflow steps may repeat; Goal 2/3 must enforce
explicit side-effect idempotency and conflict detection for reused job/step IDs.

Unsupported validation extensions or uncompilable CEL in the declared contract
are schema admission failures. The future host must verify the rule-source
capabilities it actually loaded, and refuse unknown rules rather than assuming
all extensions were understood. The current house validator's supported rules
are exercised here; this goal does not claim every host currently implements
that full schema-admission policy.

## JSON Schema, OpenAPI, and evolution

The existing JSON Schema generator reads the validation rule model. It represents
required fields, numeric ranges, string/collection bounds, enum restrictions and
oneof structure where supported, with message CEL recorded as `x-protomolt-cel`.
That extension describes a rule; ordinary JSON Schema clients do not execute CEL.
Digest equality, profile pinning, authorization, and candidate freshness stay
runtime-only. Protobuf bytes, implicit/explicit presence, and 64-bit JSON numbers
also require care when comparing generated schemas to protobuf validation.

The OpenAPI generator currently derives routes/types separately. In particular,
annotation-derived required fields and bounds are not guaranteed to propagate.
Goal 3 owns sharing the rule translation and its parity tests; this goal only
records the gap and exercises schema generation. No claim that OpenAPI fully
enforces these contracts is made. See the [coverage record](starter-contract-verification.md).

All existing wire signatures remain unchanged. New definitions add files/types;
the samples are caller-owned examples. Before freezing the new service, review
required-field evolution: adding a required field to an existing message is a
behavioral break even when a protobuf compatibility checker permits it. Preserve
field numbers and enum values; reserve removed numbers and names. Unknown question
and answer kinds require explicit version negotiation, never reinterpretation.

## Review-binding prerequisite

The separate `/work/worktrees/protomolt/review-binding` patch was rechecked. Its
23 changed files add attempt/revision fields, validate the open candidate under
the coordinator lock, propagate identity through the bridge/host/console, and
add stale-review tests. It remains uncommitted on the same baseline; both remote
main refs were checked and still match `dc49800c`. This goal does not land it.
Automatic external review requires that patch's separate review, tests, and
landing first. Contract design and the existing pre-review validation tests do
not depend on claiming that it is already shipped.
