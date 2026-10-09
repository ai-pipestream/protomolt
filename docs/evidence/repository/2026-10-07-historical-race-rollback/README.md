# Historical activation rollback under contention

Base `6b99b5d20a2cc90296f6d2c151f4a9bd7427a4c3`, plus the test fingerprint
retained here. No production or protobuf source changed.

The concurrency test now runs both committing-first and aborting-first cases.
Both observe the exact blocking PostgreSQL backend before releasing the commit
gate. In the abort case, a controlled JDBC exception before commit rolls back
the first activation while the second waits. The second transaction then commits
its independently captured pin digest. There is exactly one V94/V109 activation
and one new durable capture batch. Leases do not change.

The first attempt retains its tentative local capture, but exact retry refuses
the second attempt's different capture identity. Closing the first source handles
releases their native pins. Explicitly attempting its drain receipt then fails
with `Capture drain requires its exact immutable owner`; confirmation shows no
receipt for that rolled-back capture. The committed second capture confirms and
drains normally. Sol reviewed the final assertions and idempotent cleanup with no
blocker.

Final command exited 0 in 43 seconds: 20 tests, zero failures, errors or skips.
The two-case race first passed separately, then the full focused set passed;
the final run below adds the explicit rolled-back drain refusal.

```sh
./gradlew :protomolt-repo-container:test \
  --tests '*RepositoryHistoricalActivationConcurrencyIT' \
  --tests '*RepositoryHistoricalActivationEvidenceIT' \
  --tests '*RepositoryHistoricalSuccessorActivationIT' \
  --tests '*ScopedHistoricalSuccessorActivationIT' \
  --max-workers=2 --console=plain
```

`focused-results.tar.gz` contains the four final JUnit XML reports. This uses
actual PostgreSQL 18 transactions and controlled JDBC faults. Source publication
still uses synthetic provider observations. Provider races, deployed process
recovery, ancestry bounds, retention release and public historical execution
are not established by this test.
