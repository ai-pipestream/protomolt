# Idle author recovery

Status: sample-private state contract reviewed, including the durable submission
observation needed after restart. Complete-import compilation, five generated and
dynamic validation tests, Buf lint and full schema-image compatibility comparison
with discovery-runtime commit 9ac73251 pass locally. The remote sample still needs
an explicit task ID; this document does not claim a running idle mode.

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
