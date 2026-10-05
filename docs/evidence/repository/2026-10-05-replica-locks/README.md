# Archive statistics lock observations

The 32-worker archive diagnostic now samples PostgreSQL `pg_stat_activity` and
`pg_blocking_pids` during each mixed-traffic window. The sampler uses a dedicated
connection outside service pools, a five-second query timeout, and a ten-millisecond
pause between queries. Failed sampling or cleanup fails the test. Empty polls are
retained. Sampler SQL is excluded from the statement profile.

These are sampled observations, not lock-wait durations. Query families classify
SQL text, not the identity of the locked row. PostgreSQL can report both holders
and earlier waiters as blockers. One waiter can have several blocker rows, so the
summary counts distinct `(window, sample, pid)` observations. It does not count
requests, and the same request can appear in more than one sample. A missing
blocker does not invalidate a sampled wait; activity can change between views.

## Change under test

`ArchiveLedger.commitSave` previously acquired the shared statistics lock before
flushing and refreshing the entry/version records. It now flushes publication and
version removal first, refreshes the database-assigned revisions, and then updates
and flushes the counters at the end of the same transaction. The flush before
refresh is required because version triggers update the entry revision. Atomic
publication, exact counters and rollback semantics remain unchanged.

This change removes two refresh queries from the statistics critical section.
It does not remove the shared counter or qualify horizontal scaling. Destructive
mutations have a separate longer statistics critical section still to address.

## Results

Both benchmark runs passed all six configurations and 9,984 recorded requests
each. The baseline completed in 1m55s; the modified run completed in 1m58s.
The separate targeted run passed five publication-binding cases and 25 archive
service cases, with no failures, errors or skips. Revision assertions cover
initial publication and replacement with deletion of the previous version.

Across measured mixed windows, the baseline observed 1,841 distinct waiters over
1,982 polls; 1,840 involved the archive-statistics statement. The modified run
observed 1,400 waiters over 1,931 polls, all involving that statement. Every
configuration had fewer waiter observations, but throughput changed in both
directions: baseline 260–296 requests/second, modified 252–294. This establishes
an actual contention target and preserves correctness; it does not establish a
repeatable throughput gain. The shared counter remains a limit to investigate.

## Reproduction and artifacts

```sh
PROTOMOLT_REPLICA_BENCHMARK=true PROTOMOLT_REPLICA_WORKERS=32 ./gradlew :protomolt-repo-service:test --tests '*RepositoryScaleBenchmarkIT' --console=plain
./gradlew :protomolt-repo-container:test --tests '*ArchivePublicationBindingIT' :protomolt-repo-service:test --tests '*ArchiveServiceIT' --console=plain
```

`before` uses production commit `6dabbf5f` with the test sampler added. `after`
includes the statistics-tail change. Each directory retains its raw lock samples,
requests and windows as lossless gzip CSVs, environment, source hashes and original
build-report location. The after run also asserts final exact entry/version/byte
counters through every replica; these checks occur outside measured windows.

`summary.csv` excludes warmup sample -1 and aggregates samples 0–2 per topology.
Polls count distinct `(window, sample)` values. Waiters count distinct
`(window, sample, pid)` values with a nonempty PID; the archive-statistics subset
uses the waiting query family. Observations with blockers use the same distinct
key among rows with a nonempty blocker PID. Throughput divides summed mixed
operation counts by summed mixed window durations, including driver assertions.

These are sequential diagnostic runs on an unreserved host, with LocalStack and
additional observer traffic. Configuration order is fixed and retained history
grows. The aggregate SQL budget is controlled, but total CPU, memory and provider
connections are not. Differences are observations, not a controlled performance
improvement claim or production capacity measurement.
