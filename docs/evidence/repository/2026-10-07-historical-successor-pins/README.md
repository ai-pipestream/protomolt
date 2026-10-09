# Historical successor capture validation prerequisite

Base: 9f83399cb51a76b12c61e0edda6bd4a15f5c3185. Source hashes record the local draft.
The shared capture validator preserves initial-capture rules and adds full
successor validation against the original retention scope, exact activation
transaction and coordinator/claim tuple. It rejects closed, drained/released,
wrong-owner or stale captures and checks exact live historical pins. Its caller
must separately verify V94/V109 activation, current preparation/modes, authorization
and physical identity, holding the claim before origins/retention. This helper
alone grants no execution authority and is not wired to a successor handle yet.

Real PostgreSQL tests exercise valid, wrong transaction, old capture, drained,
old claim and wrong incarnation, alongside existing normal/scoped activation and
initial historical execution. Source publication uses synthetic provider observations;
these are SQL binding tests, not real-provider durability or public recovery proof.

Command:

    ./gradlew :protomolt-repo-container:test --tests '*RepositoryHistoricalSuccessorPinsIT' --tests '*RepositoryHistoricalSuccessorActivationIT' --tests '*ScopedHistoricalSuccessorActivationIT' --tests '*DocumentHistoricalExecutionIT' --max-workers=2 --console=plain

Result: 38 tests, 0 failures, 0 errors, 0 skips; 55 seconds.
Sol reviewed and approved the prerequisite conditional on this passing run.
An initial test compile failed due to an ambiguous static helper import; its log
is preserved, the import corrected, and the completed run is archived separately.

Active fast validation and executable successor attachment remain pending. The
fast path must be confined to a handle that already passed full validation.
