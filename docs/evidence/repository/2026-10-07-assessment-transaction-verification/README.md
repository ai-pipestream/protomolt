# Retained assessment verification within a caller transaction

Base: 6cbb78167a5d84f7960d418451c7a138d2e46bdd. Uncommitted draft.

DocumentAssessmentReconciliation now exposes a package-private verifier using the
caller's EntityManager and transaction. Existing observation and reader capture
reuse the same implementation. The helper requires an active transaction and
retains the assessment-owner row lock when it returns; no publication permission
is granted. Publication must call it after policy and full document authorization,
before physical origins, and repeat expiry validation immediately before success
if intervening locks can delay the transaction.

The real-provider successor CREATE probe compares the verified stage with CREATE,
then uses an independent PostgreSQL transaction with FOR UPDATE NOWAIT and requires
SQLState 55P03. This checks that the helper does not end the caller's lock lifetime.
An initial compile failed due to the overloaded Tx.inTransaction lambda; an explicit
return block resolved that Java ambiguity. The original compiler output is retained.

Command:

    ./gradlew :protomolt-repo-container:admissionStorageTest --max-workers=2 --console=plain

Session 47527, log `/tmp/historical-transaction-verifier.log`, is running. No final
result is claimed. Publication integration and expiry/release races remain open.

The initial host completed with OBSERVED_SQL_HOST_OK and the successor CREATE
marker after its exact PostgreSQL 55P03 assertion. Its complete host log and source
hashes are preserved here. Driver restart/cleanup checks remain in progress.
Sol found the extraction's SQL and lock lifetime sound, but requested restoring
pre-transaction control/caller checks in the observation wrapper. Those checks
currently run inside the new transaction; the correction follows this frozen run.

Session 47527 completed successfully in 8m 1s (one driver, zero failures/errors/
skips); XML and build log are archived beside the host log. This tested the
extraction before the following preflight correction.

Four real-PG preflight tests first failed because rejected requests opened one
session instead of zero. The wrapper now restores interruption, control and caller
checks before opening the transaction, while retaining checks inside it. A second
run caught a test fixture deadline using nanoseconds instead of required
microseconds; that output is retained in `preflight-deadline/`. Correcting the
fixture produced four passing tests with no failures/errors/skips in 11s.
Each invalid call opens zero sessions; a valid missing-stage call opens exactly
one. `preflight-final/` holds final source hashes and reports. The retention suite
report is archived separately. Sol reviewed the correction with no remaining
blocker. The packaged pass is not claimed against the later preflight source hash.
