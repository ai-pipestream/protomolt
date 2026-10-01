# Bounded remote workflow author

Status: contracts, transport adapters, and task reads are merged. Task mutations
and the opt-in host mount are under review. The installed remote-author workflow
has not passed its process-level acceptance test yet. Preparation and
accepted-workflow launch remain separate services.

## Purpose and existing behavior

A remote author must register, receive its assigned contract, accept an offer,
prepare executable workflow source, submit the resulting candidate and observe
review. Giving it `worker-coordinate` also permits coordinator review and offers.
The existing agent host limits model commands locally, but that does not narrow
the server authority of a bearer token. The new starter requires server-side
principal checks through the same catalog on gRPC and MCP.

Reuse `DelegationBridge`, its coordinator, durable transcript, existing worker
messages and the preparation service. Do not change the legacy coordinator API
or create a second task state machine. The operator assigns the task ID to the
author; task discovery across the entire coordinator is not required.

## Operations

A separately mounted `WorkflowAuthorTaskService` requires `workflow-author`:

- `RegisterWorkflowAuthor` reuses `RegisterWorkerRequest` and
  `RegisterWorkerResponse`. Require `worker_id == Caller.name()` before invoking
  the bridge. Provider and capability fields are metadata, never authority.
- `AcceptWorkflowTask` reuses `AcceptTaskRequest` and `AcceptTaskResponse`.
  Require the authenticated identity, exact task, attempt, current offeree
  and authoring deliverable contract. The coordinator owns the state transition.
- `SubmitWorkflowCandidate` reuses `SubmitCandidateRequest` and
  `SubmitCandidateResponse`, including the existing deliverable Any registry.
  Require authenticated identity, current task attempt and candidate revision.
  Enforce the offered deliverable contract before independent review. Preparation
  output does not grant acceptance authority.
- `GetWorkflowAuthorContext` is new. Its required task UUID and attempt bound the
  read to one assigned task. Return the selected original offer entry, its digest,
  the configured policy artifact reference, permitted calls, and the pinned
  service descriptor reference and exact descriptor bytes. Use existing messages
  for references and offers; do not introduce another schema source dialect.
- `ReadWorkflowAuthorEvents` exposes persisted review and terminal outcomes for
  the original assigned worker and attempt. It requires task identity, a
  nonnegative cursor, and a bounded batch. There is no omitted-task global read.

The descriptor payload is a complete FileDescriptorSet. A client uses those bytes
with the existing dynamic `SchemaSource.descriptor_set_base64` workflow field.
That service contract has no generated Java message; importing its entire dynamic
service into the generated authoring module merely to return SchemaSource would
expand the dependency surface unnecessarily.

## Validation and authority

New request messages use ProtoMolt validation annotations for UUID, attempt,
byte/count bounds and required fields. Successful responses are validated too.
Unknown fields are rejected at these handler boundaries, including decoded Any.
Legacy response messages have weak annotations: adapters must additionally check
success, identity and response consistency rather than treating their current
annotations as complete validation.

Handler checks independently load and validate trusted transcript state, bind
principal to offeree or holder as the operation requires, select the matching
offer, require the supported authoring contract
and configured policy, and verify referenced bytes against full metadata and
hashes. The descriptor is capped at 4 MiB, the trusted transcript at 8 MiB and
responses at 16 MiB. Configure clients for that response bound, which accommodates
the descriptor plus the original offer's deliverable descriptor and metadata.
Context is a snapshot, not a lease renewal or an
execution authorization. Mutations recheck current lifecycle at the coordinator.
New candidate submissions must also fit the existing 1 MiB delegation frame
limit, including their envelope. The author handler validates a conservative
maximum-width envelope before dispatch and returns invalid input for violations;
these are not retryable transport failures. The 16 MiB request bound and 4 MiB
preparation deliverable bound do not override this submission limit. An exact
already-committed retry acknowledges history without publishing another frame.
Do not rely on a read-then-call check as the only protection against cancellation,
reassignment or a newer attempt. The review must identify which checks are atomic
in the existing coordinator and which need an explicit guarded entry.

Return the policy reference and permitted calls, not the complete policy or
fixture bytes. This API does not provide arbitrary artifact reads or make fixture
expectations worker-selectable. No secrecy claim is made for sample fixtures. Credentials and
receipt private keys are never returned.

The author cannot offer tasks, review candidates, promote or launch workflows,
impersonate another worker, read unrelated task transcripts or write arbitrary
artifacts. Those denials require actual authenticated transport tests, including
existing coordinator operations reached with the same author credential.

## Service binding

The host exposes the author task service only when preparation is configured.
It shares the existing delegation bridge, transcript repository, artifact store,
and policy. No additional coordinator or author-owned transcript is created.

Author and coordinator RPCs reuse three request/response pairs. Those types do
not determine authority. The host supplies a complete, explicit RPC-to-action
map for the author service to both gRPC and REST. A missing method, missing
action, incompatible type, or reflected proxy must refuse the mount; it must
never select a coordinator action as a fallback. Automatic bindings for other
services exclude actions reserved by explicit maps. MCP uses the same scoped
catalog actions.

## Retries and outcomes requiring review

Registration currently rejects a second live registration. Do not promise
idempotent registration without verifying or extending the bridge behavior.
Acceptance and submission must distinguish a matching committed retry from stale
or conflicting work without emitting duplicate transcript frames. A lost response
must not leave the starter unable to determine what happened. Recover from the
durable own-task status; never infer acceptance from preparation success.

Cancellation or lease expiry stops new preparation and submission. Status must
remain readable for the original assigned attempt after a terminal outcome, while
not leaking a later attempt reassigned to another principal. Context access before
acceptance and after completion needs explicit rules independent of mutation
permissions. Unsupported contracts/rules fail closed before execution or review.

Use existing transport error categories: invalid input, permission denied, inactive
state, conflict, unavailable and deadline. Do not turn semantic rejection into a
malformed-contract error. Review feedback and retry identities must bind the exact
candidate attempt/revision and evaluation invocation; this API must not bypass
existing review-binding protections.

## Acceptance and implementation order

1. Review the operations and lifecycle/race obligations, then define protobufs.
2. Compile complete imports, run lint and compatibility, and exercise valid and
   invalid fixtures with ProtoMolt's validator. Record JSON Schema/OpenAPI
   translation coverage and runtime-only obligations without generator changes.
3. Implement narrowly scoped adapters and guarded reads. Test identity attacks,
   stale attempts, cancellation races, exact retries, changed retries and Any
   validation before semantic review.
4. Drive the installed coordinator from a scripted remote author: obtain context,
   discover/test the external service, accept, prepare, submit, observe independent
   review and let a separately authorized coordinator launch accepted work.
5. Preserve the broader Goal 5 gates: visible review retries, browser flow, worker
   process kill at the remote-effect/checkpoint boundary, Kafka submission,
   image-only Compose and native architecture/public download qualification.

## Review refinements

The bridge gives each call a new frame UUID. Serialize author mutations per
worker/task around inspection and forwarding. For a matching prior acceptance,
return the original successful identity without sending another frame. For a
matching attempt/revision and exact parsed candidate, return the persisted
submission identity without resubmitting or restarting review. A changed candidate
for a used revision is a conflict. Replay acknowledges submission; it does not
renew a lease or establish semantic acceptance. Test concurrent matching and
conflicting retries, response loss and coordinator restart.

The coordinator locks reducer checks and transcript publication together.
Prechecks bind to the exact attempt sent to that coordinator. If cancellation
commits first, a subsequent candidate fails before transcript persistence.
However, that failure closes the worker stream, and the bridge has advanced an
in-memory sequence. The adapter must reread durable state to classify the outcome
and support re-registration from persisted sequence counters. A wrapper lock
cannot serialize legacy coordinator operations. Tests must prove that denied
races preserve transcript validity and that stream recovery does not duplicate
submission or review.

Own-task status uses validated transcript entries for the requested attempt and
original assigned worker. The reducer's current task snapshot is insufficient
for attempt history. Return persisted submission/review identities and feedback
with a bounded cursor, filtering later attempts when the task has been reassigned.
A candidate with no persisted review is pending. The transient review-failure
field does not establish a durable outcome. Durable review failures and
invocation-bound retries remain required Goal 5 work.

Event cursors are scan watermarks and may advance over filtered entries on empty
pages. Scan at most 256 entries per call, return at most the requested batch and
do not advance beyond an authorized event deferred by the batch limit. Report
truncation if the bounded snapshot has unscanned entries. Filter using the
original worker and frame-derived attempt; exclude attempt-0 task messages.
Context reads allow only the current unexpired OFFERED or LEASED attempt;
historical and terminal outcomes use the event read.

## Schema coverage

Field annotations describe required task IDs, UUID formats, attempt/count/byte
bounds and cursor minima. Context CEL binds the task and attempt to the offer
and descriptor length to artifact metadata. Event CEL binds task identity and
cursor intervals. Generated JSON Schema/OpenAPI must retain these CEL rules as
runtime constraints; they do not replace checks in the runtime validator.
Hash verification, serialized response caps, principal ownership, ordered unique
cursors, attempt attribution, lifecycle and trusted policy remain handler checks.
Reused delegation response messages also require explicit handler checks for
success and echoed identity. Generator changes remain outside this contract work.
