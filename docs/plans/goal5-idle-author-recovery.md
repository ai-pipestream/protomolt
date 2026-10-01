# Idle author recovery

Status: sample-private state contract reviewed, including the durable submission
observation needed after restart. Complete-import compilation, five generated and
dynamic validation tests, Buf lint and full schema-image compatibility comparison
with discovery-runtime commit 9ac73251 pass locally. The atomic state store is
implemented, with seven filesystem and transition tests passing alongside the
five contract tests. These cover locking, identity binding, corrupt files, failed
publication recovery and preservation of saved intent when an assignment moves.
The discovery loop and installed-worker restart proof remain unqualified; this
document does not claim a running idle mode.

Add `--discover <coordinator> <fixture> <worker-id> <state-dir>` while retaining
the explicit-task invocation. Use the existing discovery, context, acceptance,
preparation, submission and event RPCs. No additional coordinator service is needed.

## Durable boundary

The worker holds an exclusive lock for the state directory. Validate the protobuf
snapshot at load and before every commit, reject unknown fields and unsupported
versions, and require configured endpoint/worker identities to match. A corrupt
file must not reset discovery or regenerate network intents. Credentials do not
enter the file. Limit the complete snapshot to 16 MiB and the queue to 64 entries.
Use atomic replacement, force file contents and force the containing directory;
do not fall back to a non-atomic overwrite. The filesystem must support this mode.

Save each discovered assignment and the returned cursor together before fetching
the next page. Filter duplicate original identities without skipping a conflicting
digest. Bound the requested count by remaining queue capacity. The server performs
bounded page scans; an empty truncated page still advances discovery. Drain work
before fetching more pages when capacity is exhausted.

On recovery, inspect trusted events for queued attempts before external effects.
Accepted, cancelled and expired attempts can be retired after identity checks.
A revision request cannot be silently treated as completed work: report it and
retain the state for a supported revision path or operator action. Failed/deferred
review remains pending; it does not authorize another probe or candidate. A
context FAILED_PRECONDITION alone is insufficient evidence for dropping work.

For active work, verify the context's exact original offer digest, task, attempt
and authenticated worker. Accept the current offer through the existing RPC.
Persist a deterministic probe UUID and exact content before the fixture write.
Persist the exact source/preparation request before preparation, and the exact
candidate before submission. On uncertain replies, replay those bytes under the
same IDs. An observed candidate must equal the saved candidate; mismatch fails.
Save its submission_cursor with the consumed review_cursor so a restart preserves
that evidence before a later acceptance. A local saved intent alone is insufficient.
Process one queue item at a time, and check the complete serialized state size
before adding an intent or advancing discovery. State size failure leaves the
prior committed file and network intent unchanged.
The fixture enforces idempotency for its write only, not arbitrary external RPCs.

## Acceptance backlog

### Registration recovery prerequisite

Source review found that unary `RegisterWorkflowAuthor` creates a server-owned
`DelegationBridge` stream. Closing or killing its client process does not close
that stream. The current registration rejects an already connected worker, so
worker-only restart is not yet supported even when its local snapshot is valid.
Missing bridge sessions are reported as generic unavailable errors; that status
cannot safely identify registration loss after a coordinator restart.

Keep the existing registration's conflict semantics. Review an additive
`EnsureWorkflowAuthorRegistration` operation with distinct request/response
wrappers around the existing registration metadata and result. The catalog
requires a unique request/response pair; reusing both top-level shapes is
ambiguous. Both wrappers require their registration field, and the response
requires acknowledgement, worker identity and a session exactly when admitted.
The operation requires the same authenticated author identity. Under
the bridge's registration lock, it would return the current healthy bridge-owned
registration only when the complete validated hello metadata matches; changed
metadata conflicts. Otherwise it would use the existing registration/resumption
path. It must not replace an unrelated direct delegation stream or infer success
from a conflict. Reads of registration state and creation must be atomic with
respect to other registrations; a check followed by an unlocked register is not
sufficient. Unknown fields, invalid metadata and unsupported rules fail closed.

Review identified two coordinator prerequisites. A bridge stream must retain the
complete validated hello and prove that its response observer still identifies
the coordinator's current session. The coordinator must conditionally open a
bridge session under its own lock, refusing an already connected foreign stream;
the bridge lock alone cannot exclude direct delegation connections. Before any
non-hello worker frame is recorded, that same coordinator lock must verify that
the sending session is still current. Superseded streams must not append frames
or disconnect their replacements. Tests must exercise a direct-stream race and
a stale stream sending the next otherwise valid sequence number.

This operation would ensure one server-owned sequence writer, not assert exclusive
ownership of a remote client process. Existing author mutations authenticate the
principal rather than a client session. Distributed process fencing would need a
separate lease/generation contract enforced on every mutation; this proposal must
not advertise that guarantee. The sample retains its exclusive local state lock.

Before implementation, review this behavior and add contract/handler tests for
exact repeated calls, changed metadata, foreign principals, concurrent calls,
direct-stream conflicts, lost registration replies and coordinator restart.
The installed-process test must kill only the author at its recorded effect
boundary and restart against the still-running coordinator. Restarting the
coordinator to clear the registration would not prove the required recovery.

### Remaining implementation and proof

1. Compile and validate the sample-private state contract, including invalid
   version/queue/cursor/identity/order fixtures. Review scalar projection coverage
   and record runtime-only obligations without expanding the generators.
2. Implement atomic state persistence and single-process ownership. Test corrupt
   and oversized files, endpoint mismatch, unavailable atomic rename, and failed
   commits preventing dependent calls.
3. Add discovery mode and stable probe identity. Unit-test pagination, historical
   outcomes, duplicate pages, exact replay and state transitions around each call.
4. Start an installed worker before any task is assigned, offer later, and observe
   independent acceptance. Kill/restart around page persistence and uncertain
   probe/preparation/submission replies; check exact recorded intents and fixture
   record counts. Keep this separate from the workflow executor's remote-effect
   checkpoint kill test, which remains required by Goal 5.
