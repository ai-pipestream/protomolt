# Durable candidate review status and retry

Status: reviewed design with standalone protobuf messages in
`transform/delegation/src/main/proto/ai/protomolt/proto/delegation/v1/review.proto`.
No status or retry API described here is available yet. This extends the delegation transcript and current reviewer;
it does not introduce another task lifecycle or evaluation provider interface.

## Current gap

`InProcessDelegationCoordinator.handleCandidate` runs the reviewer asynchronously.
An exception is assigned to private `TaskRuntime.reviewFailure` without a
transcript entry. Recovery restores the candidate but does not restart review.
The console and author event reader therefore cannot distinguish a running
review from a failed or interrupted one. `ReviewDecision.Pending` intentionally
waits for manual review and must remain distinguishable from infrastructure
failure and semantic rejection.

## Identity and persisted events

Use a shared review identity containing task UUID, worker ID, attempt, candidate
revision, invocation UUID, selected offer-entry digest, and candidate-entry
digest. Digests use the repository's deterministic protobuf serialization of
the complete TranscriptEntry. The selected offer binds the caller's contract and
policy references; the candidate binds the result and evidence. Hash strings
supplied by clients are comparisons, never authority. The coordinator derives
these values from the validated durable transcript.

Add three non-transitioning coordinator payloads to DelegateResponse:

- ReviewStarted: identity, a server-selected deadline, and optional paired
  previous-invocation/retry UUIDs. Both retry fields are absent for initial
  review and present for an explicit retry. Commit the candidate entry and its
  initial ReviewStarted in one transcript save before invoking a reviewer. The
  stored snapshot contains both entries or neither. A save exception schedules
  no review, but a missing remote acknowledgement does not prove no commit.
  The coordinator must stop publishing from its stale in-memory snapshot until
  reconciliation establishes the exact committed state; otherwise fail closed.
  Recovery cannot see a newly recorded candidate with only its start missing.
  The server owns a configurable whole-second review window, default 300 seconds,
  limited to 1..3600 seconds. CEL requires a positive interval no greater than
  one hour; handlers enforce the configured window and matching envelope time.
- ReviewFailed: exact identity and a bounded failure code. Codes distinguish
  reviewer infrastructure failure, deadline, and interrupted execution. Do not
  persist exception text, stack traces, credentials, or provider responses.
- ReviewDeferred: exact identity, indicating an explicit manual-review wait.
  This records ReviewDecision.Pending; it is not a failure or automatic retry.

The task remains CANDIDATE for all three events. CompletionAccepted and
RevisionRequested remain the semantic outcomes. Add an optional invocation UUID
to those messages for current automatic outcomes; old transcripts without
review events remain readable. A new automatic outcome must name the latest
started invocation. Existing manual review remains an explicit coordinator
decision and can supersede an in-flight automatic review under the same lock.
The manual outcome records the invocation only when it ends a still-started
review. After failure or deferral, a manual outcome carries no invocation UUID;
it decides the task without ending that already-closed invocation again. The
handler must prevent late automatic callbacks from applying another decision.

An invocation can end once: failure, deferral, acceptance, or revision. Matching
frame replay follows existing transcript rules. A different outcome for that
invocation, an event for another candidate, or a start while one is running is
invalid. Cancellation and task replacement continue to use existing lifecycle
rules and invalidate outstanding callbacks.

## Operations and authority

Reuse the author event reader for persisted review events; extend its attempt
filter and bounds when these payloads are implemented. The console derives
review status from the same validated history. Do not expose unrelated workers'
events or raw reviewer failures.

Add RetryCandidateReview with task, attempt, revision, expected invocation UUID,
and retry UUID. The response echoes that request identity and returns the
server-derived new review identity. Requests and successful responses require
runtime validation. Reuse the existing worker-coordinate authority for this
coordinator action; workflow-author alone cannot retry, decide, or launch.
The browser must use its explicitly configured coordinator principal, not an
operator token placed in client JavaScript.

Define messages first. Add the RPC to the existing DelegationService together
with its catalog action and handler; adding it alone would make the host reject
the existing partially bound service at startup. No new review service is needed.

Only the latest failed or interrupted invocation of the current CANDIDATE is
retryable. A deferred manual review is not automatically retryable. The handler
checks identity and state under the coordinator publication lock, persists one
new start carrying the retry UUID, then dispatches outside the lock. A matching
committed retry returns that recorded identity without another review, including
after completion; changed content under the same retry UUID conflicts. A reused
retry UUID cannot cross tasks, attempts, or candidates. Stale invocations,
concurrent distinct retries, cancellation, and newer revisions fail before
dispatch. Resolve the globally recorded retry key before checking current task
lifecycle: an exact replay still returns its original identity after acceptance
or cancellation, and a changed request still conflicts. Review can repeat fixture effects; targets retain responsibility for
stable operation keys and durable idempotency.

On recovery, a started invocation with no terminal review event is interrupted
only if it still belongs to the current CANDIDATE. Cancellation or replacement
already invalidates it and projects a superseded review; do not append a review
failure to a terminal or older attempt. Persist a current invocation's interrupted
outcome before accepting new retry operations. Never
silently execute fixtures during recovery. If recording the outcome fails,
startup fails rather than claiming a healthy review state. Server deadline
expiry records a failure and invalidates the callback; timing out or cancelling
review does not undo remote effects. The result callback also checks deadline
against the clock under the publication lock. A delayed timer cannot let a
post-deadline result win; the callback records timeout instead of a verdict.

Legacy candidates with no ReviewStarted are projected as legacy pending. They
remain manually reviewable but are not inferred failed or interrupted, and
cannot use invocation-bound retry without a recorded invocation. Recovery never
invents an invocation or executes a legacy candidate automatically.

The current repository-service transcript adapter replaces one blob with an
unconditional PutBlob. A timed-out request may still finish later. Stopping the
live writer is necessary but does not fence that request across restart; merely
reloading the latest snapshot is insufficient. Before automatic recovery and
retry can meet the no-stale-overwrite guarantee, the durable adapter needs
conditional/versioned writes or an equivalent fenced publication mechanism.
Until that capability is established, an uncertain write must remain fail-closed
and must not be described as recoverable by simply restarting the coordinator.
This is a storage prerequisite for the runtime implementation, not a restriction
that the new protobuf annotations can enforce.

## Validation and compatibility

Annotations cover required identities, UUID/digest/worker formats, attempts and
revisions in 1..1024, defined failure codes, timestamps, and exclusive paired
retry fields. CEL covers relationships within one message. Handler/reducer
checks cover digest recomputation, exact transcript selection, lifecycle,
deadline ordering, authority, global retry-key reuse, and current invocation.
Invalid candidates still cannot start semantic review.

JSON Schema/OpenAPI coverage must list scalar rules and retained runtime-only
CEL. Temporal, transcript, and ownership rules remain handler obligations.
Unsupported rules fail closed. Compile complete imports and run lint and
compatibility checks before implementation. Adding payloads also requires every
frame attempt selector and reducer to understand them before the host emits them.

The current JSON Schema fixture records UUID format and numeric bounds and
retains cross-field rules as x-protomolt-cel metadata. Digest fields are emitted
as strings without their full native digest-format restriction; native validator
fixtures cover that restriction. No OpenAPI RPC is exposed in this message-only
change, and no generator changes are included. Full service OpenAPI coverage
belongs with the later RPC binding; callers must not infer complete runtime
validation from the structural schema alone.

## Acceptance work

1. Contract fixtures exercise native generated and dynamic validation, required
   identities, bounds, failure-code enums, and paired retry alternatives.
2. A thrown reviewer produces a durable sanitized failure visible after restart
   in both console status and the assigned author's event stream.
3. Initial, deferred, failed, interrupted, accepted, and revised reviews have
   distinct projections. Deferred/manual review does not loop automatically.
4. Exact retry and lost-response replay dispatch once. Changed/global reused
   keys, stale identities, cancellation, and concurrent retries cannot dispatch.
5. Old callbacks after timeout, retry, cancellation, or a newer revision cannot
   change state, even when the timer is delayed. Recovery records interruption
   without fixture calls and preserves legacy pending/manual review behavior.
   Inject persistence failure at initial submission: the durable transcript
   contains both candidate and initial start, or neither. Restart after cancellation
   must not add a review event to the terminal attempt.
   Include a committed write with a lost acknowledgement: no review is dispatched
   and no subsequent write may overwrite it from stale in-memory state.
6. An installed-process test injects one infrastructure failure, observes it,
   retries through an authorized RPC, and reaches independent acceptance and
   launch. An author-only token is denied the same retry operation.

Implement in bounded changes after review: protobufs and validator fixtures;
transcript/reducer and recovery; coordinator dispatch/retry; catalog and console
projection; installed-process failure/recovery proof. Preserve the remaining
Goal 5 browser, worker-crash, Kafka, Compose, and release requirements.
