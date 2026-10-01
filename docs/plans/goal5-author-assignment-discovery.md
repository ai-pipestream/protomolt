# Author assignment discovery

Status: contract reviewed by Sol. Complete-import compilation, ten native
generated/dynamic contract tests, Buf lint and breaking comparison of complete
schema images against entry-mount commit 2eb1dd38 pass locally. The discovery
reader, actions and optional host binding are now implemented locally. Reader,
adapter and affected host tests pass, including generic filtering, duplicate
offers, reassignment, paging, corrupt evidence and storage failure. The installed
process test returns the same historical assignment over authenticated gRPC and
MCP, then an empty caught-up page. This is not yet idle-worker restart proof or
a deployment. The browser form is a separate change.

## Operation inventory

- Existing: RegisterWorkflowAuthor registers the authenticated worker.
- Existing: GetWorkflowAuthorContext verifies one known assigned task and attempt.
- Existing: AcceptWorkflowTask and SubmitWorkflowCandidate retain their lease,
  revision and independent-review boundaries.
- Existing: ReadWorkflowAuthorEvents follows one known attempt through review.
- New: ReadWorkflowAuthorAssignments discovers this caller's historical offers
  through a bounded cursor read. It adds no task lifecycle or authorization role.

## Contract and handler boundary

The request contains a nonnegative cursor and a required count from 1 through 64.
The response binds each assignment to a task, attempt, original offer-entry digest
and transcript cursor. Native annotations validate scalar bounds, required values
and the cursor interval. Validate requests and successful responses, reject unknown
fields and unsupported rules, and enforce a 64 KiB serialized response bound.

The handler requires workflow-author and derives ownership from Caller.name. It
validates the entire existing transcript (bounded to 8 MiB), then indexes prior
offers for duplicate suppression and scans at most 256 new cursor positions per
page. Return only coordinator offers addressed to that principal with the supported
WorkflowAuthoringDeliverable contract. Generic offers are filtered; malformed
authoring evidence fails closed. Original ownership survives reassignment. Hash
verification of the original entry does not require current policy equality or
artifact availability; policy rotation must not hide historical assignments. Hash
the deterministic original TranscriptEntry bytes using the existing context hash
convention. Return strictly increasing, unique cursors and echo the request cursor.

Advance the scan watermark over filtered entries, including empty pages. Stop
before an eligible offer excluded by the requested count or response-size bound.
Set truncated when the snapshot still contains unscanned entries. A cursor ahead
of the stored transcript is invalid. Reads do not mutate storage, register workers,
renew leases, invoke fixtures or require currently connected workers.

Historical assignments do not establish current eligibility. The consumer must
persist discovered identities before advancing its saved cursor, then fetch context
and compare its original offer digest. Accept only the current unexpired attempt;
an expired or reassigned attempt remains discoverable but cannot be accepted.
Retries and duplicate delivery must not repeat committed candidate submission.

## Errors and recovery

Invalid input is INVALID_ARGUMENT; missing scope is PERMISSION_DENIED; corrupt
evidence or unsupported rules are DATA_LOSS; storage failures are UNAVAILABLE or
DEADLINE_EXCEEDED. Cancellation has no durable effect. Retry the same cursor after
an uncertain read. Poll with bounded backoff after catching up. A long-poll service
and a new global task-list permission are unnecessary for the initial worker.

## Acceptance backlog

1. Compile complete imports and run native generated/dynamic fixtures. Record
   scalar JSON Schema/OpenAPI coverage; cross-field CEL, custody, hashes, ordering,
   scan/count limits and lifecycle remain runtime obligations. Generator changes
   stay outside this change.
2. Extend WorkflowAuthorTaskReader and actions with caller-owned discovery. Test
   mixed owners, generic filtering, expired and reassigned offers, empty pages,
   page boundaries, future cursors, corrupt evidence and storage failure. Assert
   no transcript writes and no coordinator authority is exposed.
3. Add generated gRPC, MCP and response-binding tests. Disabled hosts must omit
   the operation; enabled hosts retain workflow-author access checks.
4. Update the remote author to start idle, persist its discovered assignment queue
and cursor, reconnect without external task-ID injection, and finish one task
   through independent review. Prove restart and duplicate discovery recovery with
   installed processes before advertising automatic assignment discovery.

The sample currently requires a supplied task ID and uses a random UUID for its
fixture probe. The idle mode must persist a deterministic probe operation and exact
preparation/submission intent before external effects, inspect recorded outcomes
on restart, and wait for pending review without repeating committed work. Keep the
legacy invocation available while adding a locked, versioned durable state file.
