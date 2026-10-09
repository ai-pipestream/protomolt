# Claimed cancellation lock order

Base: b86df18a0 with the coordinator's uncommitted historical-rejection draft
copied into this isolated checkout. The two additional files under test are listed
in source.sha256. The main checkout's storage gate continued without source edits.
After the first green run, the copied rejection draft was removed and the same
12 tests passed again against b86df18a0 plus only this cancellation fix and test.
The standalone reports are in `standalone/`; Gradle exited 0 with no failures,
errors or skips. This fix therefore does not depend on the pending rejection work.

The real PostgreSQL test holds the exact execution claim, starts cancellation on
another connection, and observes the blocker through pg_blocking_pids. The owner
must remain available through FOR UPDATE NOWAIT while cancellation waits. Before
the fix that statement failed with SQLSTATE 55P03 (red.xml; exit 1, 9 seconds).
The first fixture attempts lacked owner admission and refreshed statistics; those
were fixture errors and are not the archived regression failure.

The fix prelocks the claim before owner for claimed cancellation/precondition
decisions. It does not require a live claim before authorized terminal replay.
With that fix the focused regression and existing installed-cancellation and
retirement suites passed in 44 seconds, exit 0, with no failures or skips.

```sh
./gradlew :protomolt-repo-container:test --tests '*RepositoryHistoricalDecisionLockIT' --tests '*RepositoryHistoricalInstalledCancellationIT' --tests '*RepositoryHistoricalAttemptRetirementIT' --max-workers=2 --console=plain
```

Sol found no blockers. This demonstrates the corrected lock acquisition, not two
concurrent terminal decisions. The source fixture uses synthetic provider
observations; these tests establish SQL behavior, not provider durability.

## Expired-lease replay

At base 6243a7515, the second test asserts a terminal rejection exists, waits for
both owner and claim leases to expire using the PostgreSQL clock, checks their
expiry, and invokes cancellation again. The identical observation is returned.
No lease timestamps or protection guards are changed by the test.

The focused class passed in 15 seconds: 2 tests, zero failures, errors or skips,
Gradle exit 0. Reports and tested source hashes are in `expired-replay/`.
Sol reviewed the final assertions and found no blocker.

```sh
./gradlew :protomolt-repo-container:test --tests '*RepositoryHistoricalDecisionLockIT' --max-workers=2 --console=plain
```
