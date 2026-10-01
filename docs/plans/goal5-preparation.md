# Remote workflow preparation

Status: proposed contract design, not an available operation. Builds on main
`70c7ae91f22d0e8a460074a12050279d4bcdb133`. The coordinator owns the protobuf
definitions; implementation follows contract fixtures and design review.
Land message definitions first. The current contributed-service adapter refuses
partial service mounts, so add the RPC to the service descriptor only with its
handler. Adding it earlier would break the existing lookup/launch mount.

## Operation and authority

Add `PrepareWorkflowCandidate` to `WorkflowAuthoringService`. A remote worker
supplies executable workflow source; the coordinator derives policy, fixtures,
recording and signed evidence. Preparation does not submit a candidate, accept
it, promote a workflow or insert a job. Reuse `WorkflowAuthoringDeliverable` and
the existing delegation candidate envelope.

Require a new `workflow-author` scope and an authenticated principal matching
the current task holder. The deployment must configure the principal name as
the delegation worker ID. A request cannot supply an authoritative worker ID.
Unrestricted operator status does not bypass holder equality for this operation.
Browser launch authority is a separate design and is not granted by this scope.

`ActionCatalog.dispatch` currently checks the caller but calls the handler
without it. Add a backward-compatible `ProtoAction.execute(request, context,
caller)` overload whose default delegates to the existing method. Pass the
resolved caller through typed, JSON and unary-through-streaming dispatch.
Preparation overrides the authenticated overload; its legacy overload fails
closed. Do not use a gRPC thread context as authority for other transports.

## Wire contract

The request contains only task ID, attempt, candidate revision, preparation UUID
and executable source JSON. Preserve task ID spelling, bound attempt/revision
to 1..1024, require a canonical lowercase preparation UUID, and limit source to
1 MiB of UTF-8 bytes. Represent source as bytes so the byte limit and retry
identity do not depend on Unicode character counting; handlers reject malformed
UTF-8 and JSON before calls. The request cannot supply policy, expectations,
check verdicts, run ID or signing identity.

The response binds task ID, attempt, revision, preparation UUID, selected offer
fingerprint and source digest to the existing typed authored deliverable. The
offer fingerprint hashes the selected immutable offer entry, not the growing
transcript. Specify deterministic protobuf serialization in the wire comments;
clients consume the fingerprint rather than recreating it.

Annotations enforce shape, bounds and required fields. Handler obligations
enforce caller/holder equality, a clean trusted transcript, a current LEASED
attempt, unexpired lease and the next candidate revision for new execution.
Completed replay follows the separate state rules below. The selected offer
must contain the configured policy reference and supported deliverable contract.
Reject unsupported rules or check names before any external call. Validate the
successful response, including its nested deliverable, before returning it.

## Intent, execution and retry

Complete deterministic preflight before reserving a revision: strict UTF-8/JSON,
schema closure, compilation, target/method/TLS and template checks, offer/policy
validation, and supported checks. Fixable source errors must leave the revision
available for correction. Content-addressed preflight artifacts may be orphaned.

Before external calls, atomically reserve task/attempt/revision and preparation
UUID in a keyed intent store. Persist the exact source bytes, authenticated
holder, selected offer, policy and their fingerprints. A changed UUID for an
already reserved revision, changed source under the same UUID, or reuse of a UUID
for any different task/attempt/revision tuple conflicts, including a later
attempt of the same task. Scope this namespace to the coordinator store.
Use one authoritative record per tuple under a global cross-process lock, with
UUID uniqueness verified under that lock, or a transaction with both unique
constraints. A secondary index is rebuildable; independent create-if-absent
files cannot provide atomic reservation of both identities.

Use the caller-pinned first acceptance fixture as recorded-run input, and run
all pinned acceptance fixtures. Compile against the pinned descriptor closure
and approved targets. Derive a stable run identity from the persisted intent.
Store artifacts and the exact signed response before marking preparation
complete. Completed retries return the stored response without rerunning calls,
after authorization and binding checks. Permit replay in LEASED only for the
currently expected revision, and in CANDIDATE only for that submitted revision.
Require the same holder, attempt and persisted offer binding in both cases.
LEASED replay also requires an unexpired lease. CANDIDATE replay may return the
stored response after wall-clock expiry: it performs no new calls, and the
existing review lifecycle retains pending candidates independently of lease
timers. It still requires the submitted revision and current holder/attempt.
Refuse replay in ACCEPTED or other terminal phases, after supersession, or once
a revision request advances the expected revision. Never execute in CANDIDATE.

Incomplete retries may repeat fixture and recorded-run RPCs. Remote services
must enforce the caller's stable operation key. Cancellation or deadline expiry
does not roll back effects or erase intent. Do not promise exactly-once effects.
Before each new execution phase and completion commit, recheck the active lease
and revision. A concurrent cancellation can still race an in-flight remote RPC;
completion may coexist with cancellation. Preparation never authorizes
submission: the delegation reducer must independently enforce the active
holder/attempt/revision when admitting the candidate. Recheck immediately
before returning, without claiming atomicity across these separate stores.

Recovery must inspect an existing immutable recorded run before calling
`WorkflowRunRecorder` again: timestamped evidence cannot be overwritten with a
new recording under the same ID. Distinguish failed evidence, incomplete intent
and completed response explicitly. Do not silently start a replacement run.
A stored failed run makes preparation terminal, even when its original cause
was a transport failure. Return FAILED_PRECONDITION with a stable
`preparation-attempt-failed` reason on subsequent calls. The worker reports the
attempt failed through delegation; the coordinator must explicitly reoffer a
new attempt for corrected source or execution. The remote-author acceptance
test must demonstrate this recovery, rather than leave a task lingering.
UNAVAILABLE is retryable only while no immutable failed run exists; recovery
must inspect storage before choosing that classification. This first contract
does not introduce multiple preparation generations inside one task revision.
Incorrect output from a live fixture also persists a terminal rejected intent,
even if no run evidence exists yet, and requires the same new-attempt recovery.
It must not repeatedly execute a known failing source on same-ID retries.
The first response after a failed run is stored must also be terminal, carrying
the bound run ID as structured error detail. A stored successful run is verified
against the intent, source and fixture input and reused while remaining phases
resume; it is never rerecorded with new timestamps.

Current artifact and run repositories do not establish power-loss durability.
The initial guarantee is process-restart recovery with intact storage. Missing
or corrupt referenced bytes fail closed. Strengthening storage durability is
required before claiming host-power-loss recovery. Require both receipt trust
and a configured signing identity before mounting preparation.
Before live calls, check that the configured issuer, key ID and public key are
active for workflow-run receipts in the current trust snapshot. Recheck per
operation so trust rotation cannot produce a receipt the reviewer would refuse.

## Example-specific correctness

The starter policy must check NormalizeText followed by WriteRecord and verify
that the input operation UUID reaches WriteRecord unchanged. A constant fixture
UUID can pass one example but fail future jobs. Use an auditable source-template
restriction for this starter, alongside a second input with a different UUID.
Keep this restriction out of the general workflow language.

Server-derived checks report observed evidence, not semantic truth. Independent
review reruns the pinned fixtures after candidate submission. The worker packs
the returned deliverable into `CompletionCandidate.result`, mirrors its checks,
and includes workflow/source/receipt artifact references as existing review
requires.

## Implementation and acceptance backlog

1. Coordinator: protobuf request/response and intent state definitions, complete
   imports, native valid/invalid fixtures, Buf lint and compatibility. Record
   JSON Schema/OpenAPI byte encoding and bounds coverage; holder, lifecycle,
   digest and template checks are runtime obligations. No generator changes.
2. Sol: caller propagation and scope; Luna: typed/JSON/streaming tests proving
   the authenticated identity survives dispatch and spoofed holders fail.
3. Sol: keyed preparation store and recovery; Luna: concurrent reservations,
   changed intent, exact completed retry, cancellation, supersession and restart
   tests. Define terminal failure versus retryable infrastructure errors before
   coding; never clear a reservation to hide a failed run.
4. Sol: bounded preparation handler and opt-in mount; Luna: invalid source and
   unsupported rules make no calls, forged checks cannot enter the response,
   invalid responses fail closed, and trust/signing configuration fails early.
5. Coordinator: real remote scripted author proof through discovery, preparation,
   submission, independent review and launch, without shared writable storage or
   an operator token. Continue the worker-kill, Kafka, browser and image-only
   Compose acceptance work in the parent Goal 5 plan.

Use INVALID_ARGUMENT for invalid input, PERMISSION_DENIED for holder mismatch,
FAILED_PRECONDITION for inactive state or unsupported contract,
ALREADY_EXISTS for conflicting intent, UNAVAILABLE/DEADLINE_EXCEEDED for
retryable transport failures, INTERNAL for corrupt stored evidence and DATA_LOSS
for an invalid successful upstream response. Sanitize errors. Contract-valid
but incorrect fixture output is a preparation rejection, not a transport retry.

## Schema coverage and current proof

The new messages compile through the authoring Gradle module with all imports.
Buf lint and complete-descriptor FILE compatibility against `70c7ae91` pass.
Runtime validator fixtures are a separate gate; compilation cannot prove CEL
rules execute correctly. No new service method or available-operation claim is
included in this contract-only slice.
The authoring module suite passes 35 tests with no failures or skips, including
five preparation tests over generated and dynamic messages. They cover UUID
spelling, counter limits, raw and multibyte source limits, nested required fields
and the source-digest CEL rule. Shape-valid invalid JSON and untrusted hashes
remain explicit handler cases. Compatibility also passes against `ffb5a3ed`.

`ProtoJsonSchemaGenerator` represents protobuf bytes as base64 strings and
explicitly leaves raw-byte length constraints to runtime. Its shared field-rule
translation labels bytes with `x-protomolt-runtime-rules`; the OpenAPI generator
uses that translation. Source byte limits therefore require server validation,
not client string-length checks. Integer bounds and UUID/pattern constraints
use the existing scalar translation. CEL is recorded under `x-protomolt-cel`,
not executed by ordinary JSON Schema/OpenAPI validators. The response's source
digest equality and inherited deliverable CEL rules remain runtime checks.
Hash recomputation, trusted transcript membership, lease/holder checks, signature
trust and executable-source semantics are handler obligations in every dialect.
No generator changes belong in this slice. Verify the concrete mounted OpenAPI
document when adding the RPC; it has no advertised operation today.

## Persisted state contract

`workflow_preparation_state.proto` defines an immutable intent and one exclusive
pending, completed or failed state. Intent retains exact source and policy bytes,
the selected offer, authenticated holder, binding and stable run ID. Native CEL
binds request identity to the response identity, selected offer to holder/task/
attempt, and terminal outcomes to the intent's binding and run. Hashes, policy
contents and transcript membership still require independent handler checks.
The run ID is exactly `prepare-` plus the canonical preparation UUID. Existing
run evidence must still match the source, compiled workflow and pinned input;
the ID alone is insufficient to adopt it.

Reject intent over 8 MiB before reservation and successful responses over 4 MiB
before completion. The store must reject records over 16 MiB, unknown fields
and noncanonical bytes. Separate bounds leave room for intent plus response;
reservation cannot consume all the space required by a terminal record.
Read corruption is an error, never an absent reservation. Recompute source and
offer hashes, parse/validate/hash the policy snapshot and check its full pinned
reference before use. Only pending may transition to completed or failed; an
exact terminal retry returns the prior record and a changed terminal outcome
conflicts. A fixture-rejected terminal state can precede RunEvidence creation;
its reserved run ID does not assert a recorded run exists.

Serialize execution per task/attempt/revision across threads and processes, not
just individual ledger writes. Hold that lock through recovery, external calls
and terminal-state persistence to prevent concurrent timestamped run recordings.
Use a separate short global reservation lock to enforce tuple and UUID uniqueness
atomically. Establish one lock order: per-intent execution lock, then global
reservation lock; never hold the global lock during external calls. A process
crash releases execution ownership while leaving the pending intent recoverable.
Every resumed execution rechecks the delegation lease before calls. Locking the
ledger does not lock the transcript and does not replace admission checks.

Contract fixtures must exercise each state through generated and dynamic
messages, absent state and pending=false, request/offer binding mismatches,
policy snapshot metadata, unsupported failure reasons and terminal run/binding
mismatches. No implementation or mounted operation is established by these
stored-message definitions.

The storage SPI is `WorkflowPreparationRepository`: snapshot `find`, and
`withExclusiveIntent` whose callback receives `current`, `reserveOrMatch`,
`complete` and `fail`. Session use after callback return is invalid. This slice
defines the interface only; filesystem implementation and concurrency/failure
tests follow. Local validation passes all 44 authoring tests without skips,
including nine state-contract tests, plus Buf lint and full-descriptor
compatibility against the request/response contract commit `2fea5dc8`.

## Handler integration findings

Integration checkpoint (2026-10-01): the lease/offer admission helper and signing
identity check are implemented and reviewed in PRs #347 and #346, respectively.
Their separate local authoring suites passed 67 and 61 tests without skips.
The integration branch combines them with the preparation store and static
fixture admission; these results do not yet prove the combined handler.
PR #343's fixture readiness fix landed on both main remotes at `f4440599`.

The next implementation is `WorkflowCandidatePreparer`, with explicit trusted
repositories, runner, action context, policy reference, current-trust supplier,
signing identity and clock. An explicit trusted source-template callback checks
starter restrictions after generic preflight and before intent reservation.
The production authoring module must not depend on samples. Transport integration
must preserve distinct invalid-input, inactive-state, conflict, terminal-failure,
corrupt-evidence and invalid-response outcomes; terminal errors include the
persisted preparation binding and run ID. No preparation RPC is available yet.

The initial handler compiles but remains under review. Before mount, prove that
request validation precedes ledger access, source schema pinning precedes any
schema resolution, and artifact reads check actual size and content digest.
Fixture success alone does not certify the later recorded invocation: compare
its output with the first pinned expectation, including on recovery. Verify the
generated receipt before persisting a successful response. Regression tests for
these boundaries belong to handler acceptance, not a post-release follow-up.

`DelegationReducer.TaskState` exposes phase, holder, attempt and candidate
revision, but not lease expiry. A clean reduction alone does not establish an
unexpired wall-clock lease. The preparation handler must validate and scan the
selected offer and subsequent renewals for that exact attempt, then compare the
latest expiry with an injected clock. Test expired-but-not-yet-marked-EXPIRED
transcripts, valid renewal, stale-attempt renewal and exact expiry. Preserve the
selected original offer fingerprint when a renewal extends its lease.

`WorkflowAuthoringFixtures.execute` combines static fixture admission and live
execution. Preparation needs the static portion before reservation, including
all fixture bytes, native validation and expected output type. Extract a reusable
admitted-fixtures value rather than validating only the first fixture before
reserving. The independent reviewer must continue to admit and rerun the same
caller-pinned fixtures; do not duplicate or weaken its checks.

`RecordSigning` holds a signer with a key ID, not an exposed public key. Before
live calls, prove the configured signer matches the active trusted key (for
example with a local signature verification probe) as well as checking issuer,
subject authorization and time bounds. Matching the key ID alone is insufficient.
Do not save a probe as workflow evidence or expose signing material.

`WorkflowRunRecorder.record` saves failed immutable evidence before rethrowing
execution failure. The handler must inspect the run repository after an error,
including uncertain storage errors, before deciding whether the same preparation
can retry. Successful evidence recovery must check source-derived workflow,
pinned input and referenced artifacts, then resume remaining receipt/response
work without another recording.

## Store implementation evidence

The filesystem implementation now enforces per-tuple execution ownership,
global UUID reservation, hash-bound policy snapshots, canonical outer records,
and immutable terminal outcomes. Sessions reject cross-thread and post-callback
use. Publication forces the temporary file, atomically renames it and forces the
directory; a read barrier covers a prior interruption after rename. Exact policy
bytes are preserved even when their valid protobuf encoding is noncanonical.

All 58 authoring tests pass locally with no skips. Thirteen repository tests
cover reservation conflicts, changed terminal outcomes, corruption and digest
mismatches, concurrent UUID reservation, session misuse and failures before and
after rename. A separate process test starts competing JVMs, kills the lock
holder after reservation, and verifies the contender recovers the same pending
intent. This proves process recovery with intact storage; it does not prove
host power-loss recovery, RPC admission, or the workflow-worker checkpoint
crash window. No preparation RPC is mounted by the store implementation.
