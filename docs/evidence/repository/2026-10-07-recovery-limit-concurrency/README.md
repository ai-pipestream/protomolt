# Concurrent recovery-limit decisions

Base: `3a54f36aa806317869b1dde22a1d8cd2f55f7f30`. This checkpoint adds a test
and documentation; production Java, protobuf contracts and SQL are unchanged.
The tested source fingerprints accompany this evidence.

Two callers use the same installed plan at a real capture bound of 16 retained
batches. The first transaction is held immediately before JDBC commit, after
inserting both sidecar and rejection with the same creation transaction ID. A
second transaction calls the actual handler. The test observes its active claim
`SELECT ... FOR UPDATE` blocked by the first backend using `pg_blocking_pids`.
An unfinished future alone is not considered evidence of SQL contention.

- When the first transaction commits, both calls return the same receipt.
- When the first transaction is deliberately aborted with SQLSTATE 40001, it
  reports that failure and the second creates the sole durable receipt.

Both cases then replay the receipt and assert one sidecar/rejection pair,
unchanged claim/owner leases, the same 16 batches, no execution, historical
activation or capture drain, and zero reserved payload budget. The shared budget
is 128 MiB to admit two simultaneous bounded serialization reservations; no
production bound is increased.

Command:

```sh
./gradlew :protomolt-repo-container:test \
  --tests '*RepositoryHistoricalLimitConcurrencyIT' \
  --max-workers=2 --console=plain
```

Both cases passed with zero failures, errors or skips. `results.tar.gz` holds
JUnit XML and `gradle.log` holds the build output. Sol reviewed the implementation
and assertions and found no blocker. These are local test and review results;
they do not imply hosted CI, merge or deployment.

PostgreSQL 18 and the transaction contention are real. Initial provider
observations are synthetic; this suite makes no provider durability or throughput
claim. Activation contention and V98 supersession remain separate gates: at the
exhausted bound a valid activation cannot win, while supersession requires expiry
and the new decision requires live ownership. Corruption, expired-lease replay,
source-root release and public historical execution remain unfinished.
