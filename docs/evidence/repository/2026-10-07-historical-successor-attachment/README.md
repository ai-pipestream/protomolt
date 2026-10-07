# Historical successor attachment draft

Base: 9f83399cb51a76b12c61e0edda6bd4a15f5c3185. Uncommitted draft; hashes identify tested source.
The private local activation attempt can supply a live accepted Work child and
scope to the shared historical execution class. Its binding distinguishes current
preparation/modes from original retained roots; checks exact preparation bytes,
command/mode ownership, V94/V109/capture identity and active pins; and reauthorizes
through the shared physical fence before returning a handle. A cold receipt alone
is insufficient. Historical starts now select the owned predecessor generation.

The lifecycle test proves source/scope admission can close while an accepted handle
starts its own generation, capture drain waits for the handle, and budget/scope
ownership returns on close. Binding tests reject missing activation, wrong root
scope and a drained capture. Initial execution and ordinary start journal regressions
are included. SQL source publication has synthetic provider observations.

Command:

    ./gradlew :protomolt-repo-container:test --tests '*RepositoryHistoricalSuccessorPinsIT' --tests '*DocumentHistoricalExecutionIT' --tests '*DocumentAssessmentStartJournalIT' --max-workers=2 --console=plain

Result: 32 tests, 0 failures, 0 errors, 0 skips; 54 seconds.

Remaining before landing: final Sol attachment review; successor CREATE with real
provider reads and exact retained reconciliation; old-owner late work fencing;
revocation/corruption and uncertain attachment tests; a handle-confined active pin
fast path after full verification. Do not advertise public recovery or complete
successor execution from this draft. No source changes during its recorded run.

Follow-up revocation run: 19 tests, 0 failures, 0 errors, 0 skips; 46 seconds. Six cases revoke creation grants, credentials or source READ before attachment or after holding a handle; START refuses with no operation schema claims, assessment or publication. Failed attachment and closed handles return their scope/budget. Sol's attachment review found no blocking defect; CREATE/corruption/fault qualification remains pending.

Real-provider successor CREATE driver now running: session 40069, `/tmp/historical-successor-create.log`. Do not edit source during this run. It exercises an original START without CREATE, real lease expiry, new capture/provider rereads, old-owner late CREATE refusal, successor CREATE and retained reconciliation. Its terminal result is not yet known.

The first CREATE driver (session 40069) failed because the new probe omitted the
successor's `admitUploads` call. Reuse-only members also need per-generation
selection rows; `DocumentCommitParts.selections` correctly refused the incomplete
setup. The failure XML/Gradle/host log are in `create-missing-admission/`. No
production guard was relaxed. The probe now explicitly admits reuse selections,
asserts zero fresh upload attempts, and checks exactly two start rows and one
assessment for the operation. Rerun: session `2885`,
`/tmp/historical-successor-create-admitted.log`; terminal result pending.

Sol reviewed the proposed active-handle optimization on 2026-10-07. The full
verification flag must be set only after attachment's transaction and subsequent
Work authorization return successfully. Later mutations may skip immutable root
aggregation and V104 child-tuple comparison, but must retain live claim/owner,
current authorization, exact source/physical witnesses, capture drain/root release,
and all native pin checks under FOR SHARE. This relies on database-enforced
immutability (V81/V103/V104/V105/V109); it does not claim to detect privileged
trigger-bypass corruption after attachment. Fresh attachments must still perform
full verification. Implementation and regression qualification remain outstanding.

Session 2885 completed successfully: 8m 2s, one driver test, zero failures,
errors or skips. `create-admitted/` retains its compressed XML, build log and
source hashes. The successor CREATE marker was observed in the live child log;
successful JUnit cleanup removed that temporary log before archival. The test
requires that marker. This run predates the subsequent active-handle optimization.

Active-handle focused run: {"tests"=>34, "failures"=>0, "errors"=>0, "skipped"=>0}, 1m 7s. Sol found no blocking issue in
the optimization. This precedes the follow-up duplicate-attachment guard.

The duplicate-attachment guard run passed (8 tests, 25s). The subsequent
lost-activation-acknowledgment test initially failed on a test-only nonexistent
table name; `lost-ack-wrong-table/` retains that failure. The corrected test
compares actual document_revision_commits before/after retry, checks one activation,
two capture batches and one start, and proves failed-open scope/budget cleanup.
It retries through openExecution with source admission already closed, retains the
same capture object, and drains only after accepted Work closes. `lost-ack/`
archives the successful nine-test run and exact tested source hashes.

Fresh attachment corruption qualification passed: 10 tests, zero failures/errors/
skips, 30s (`corruption/`). A test-only PostgreSQL transaction temporarily bypasses
immutable-row triggers to change the captured child node identity. The private
factory refuses with DATA_LOSS and the exact pin-batch error; scope/budget return
and no assessment/start appears. The test restores the original row in a finally
block, then successfully attaches/starts and drains after Work closes. This tests
fresh attachment, not detection of privileged corruption after a handle is open.
Production sources match the preceding `lost-ack/source.sha256` entries.

Sol reviewed the lost-ACK case and duplicate-attachment guard with no blocker.
Recommended strengthening: explicitly confirm durable V109 receipt immediately
after the first lost acknowledgment, before retry, rather than relying only on
the afterCommit hook and post-retry counts. Query-trace proof of avoided scans,
additional preparation/mode corruption and mixed-upload/scoped CREATE coverage
remain open before the broader successor slice is considered fully qualified.

Trace and regression gate: {"tests"=>46, "failures"=>0, "errors"=>0, "skipped"=>0}, 1m 21s. `trace/` archives the run.
The JDBC statement inspector observes real PostgreSQL attachment root aggregation
and captured-child JSON comparison, then proves neither is issued by repeated
START while each START still takes the live native-pin FOR SHARE query. This is
query-shape evidence, not a latency benchmark. The lost-ACK test now also confirms
the durable V109 capture digest and execution owner before retry.

Final production-JAR rerun after active-handle and duplicate-attachment changes:
session 40817, `/tmp/historical-successor-final-storage.log`. Sources remain frozen
until the process completes. The earlier full-driver success does not qualify
these later production edits by itself.

Final Sol review of the full dirty successor slice found no blocker for a private
checkpoint, conditional on the packaged driver completing successfully. Review
covered claim/owner ordering, full binding before attachment, accepted Work/scope
and budget cleanup, exact handle-bound START/CREATE ownership, scoped revocation,
and immutable-data caching. The completed initial host log for session 40817 is
preserved in `final-storage/host.log.gz`; it ends with OBSERVED_SQL_HOST_OK and
contains CLAIMED_HISTORICAL_SUCCESSOR_CREATE_OK. Restart/recovery driver completion
is still pending; this log alone is not the full gate result.

Session 40817 completed successfully in 8m 1s: one packaged driver test, zero
failures, errors or skips. `final-storage/` contains the final XML, build log,
completed initial host log, and all changed source hashes. Hash verification after
completion confirmed no source changes during the run. This satisfies Sol's
packaged-driver condition for the private checkpoint. Mixed successor uploads,
publication, public recovery, pruning and other goal requirements remain open.
