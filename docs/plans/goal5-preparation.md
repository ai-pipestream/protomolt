# Remote workflow preparation

Status: proposed contract design, not an available operation. Builds on main
`70c7ae91f22d0e8a460074a12050279d4bcdb133`. The coordinator owns the protobuf
definitions; implementation follows contract fixtures and design review.

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

Before external calls, atomically reserve task/attempt/revision and preparation
UUID in a keyed intent store. Persist the exact source bytes, authenticated
holder, selected offer, policy and their fingerprints. A changed UUID for an
already reserved revision, changed source under the same UUID, or reuse of a UUID
for another task conflicts. Scope this namespace to the coordinator store.

Use the caller-pinned first acceptance fixture as recorded-run input, and run
all pinned acceptance fixtures. Compile against the pinned descriptor closure
and approved targets. Derive a stable run identity from the persisted intent.
Store artifacts and the exact signed response before marking preparation
complete. Completed retries return the stored response without rerunning calls,
after authorization and binding checks. Permit replay in LEASED only for the
currently expected revision, and in CANDIDATE only for that submitted revision.
Require the same holder, attempt and persisted offer binding in both cases.
Refuse replay in ACCEPTED or other terminal phases, after supersession, or once
a revision request advances the expected revision. Never execute in CANDIDATE.

Incomplete retries may repeat fixture and recorded-run RPCs. Remote services
must enforce the caller's stable operation key. Cancellation or deadline expiry
does not roll back effects or erase intent. Do not promise exactly-once effects.
Before each new execution phase and completion commit, recheck the active lease
and revision. A concurrent cancellation can still race an in-flight remote RPC;
it must prevent a newly completed preparation from authorizing submission.

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

Current artifact and run repositories do not establish power-loss durability.
The initial guarantee is process-restart recovery with intact storage. Missing
or corrupt referenced bytes fail closed. Strengthening storage durability is
required before claiming host-power-loss recovery. Require both receipt trust
and a configured signing identity before mounting preparation.

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
