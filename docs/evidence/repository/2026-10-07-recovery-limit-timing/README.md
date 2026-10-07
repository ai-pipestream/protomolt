# Recovery-limit decision timing

Base `371cffa25e8e33be48d5a3cce412106154dbe42c` plus recorded source hashes.
Production SQL and Java are unchanged.

Three PostgreSQL 18 cases fill the capture limit using 16 real capture batches,
then reserve/install an unactivated successor with two-second leases. Each case
waits for actual database expiry and explicitly asserts that both claim and owner
leases are past before continuing. Lease rows and clocks are never rewritten.

- Expiry after sidecar insertion but before rejection insertion refuses the
  rejection and rolls back both rows.
- Expiry after both inserts but before deferred pairing causes commit to fail
  and rolls back both rows.
- Explicit early constraint firing validates the pair while live. A later commit
  after expiry succeeds. The receipt remains an authorized terminal read; no
  activation, lease renewal or capture drain is created. This is PostgreSQL
  guard-time semantics, not a commit-instant liveness guarantee.

The shared setup now waits for the actual greater claim/owner deadline rather
than assuming a fixed 1.1-second delay. The timing fixture allows ten seconds to
create the 16 initial batches. Sol reviewed the lifetime and transaction cases;
its explicit post-sleep expiry assertion and setup-margin suggestions are included.

The initial run passed 15 tests in 43s. Final run with those improvements exited 0
in 59s: 15 tests, zero failures/errors/skips.

```sh
./gradlew :protomolt-repo-container:test --tests '*RepositoryRecoveryLimitTimingIT' \
  --tests '*DocumentCaptureAdmissionClosureIT' --max-workers=2 --console=plain
```

`focused-results.tar.gz` retains both final XML reports. Source publication uses
synthetic provider observations. The production decision handler, corruption,
concurrency, lost acknowledgement and retained-root release remain unfinished.
