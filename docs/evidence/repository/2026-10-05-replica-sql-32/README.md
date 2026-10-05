# Repository statement profile at 32 workers

Measured on `krick` on 2026-10-05, from `3d1b58ad` plus the two test-source
changes identified by `sources.sha256`. The test passed with zero failures,
errors or skips in 1m56s. Production repository behavior was unchanged.

```sh
PROTOMOLT_REPLICA_BENCHMARK=true PROTOMOLT_REPLICA_WORKERS=32 ./gradlew :protomolt-repo-service:test --tests '*RepositoryScaleBenchmarkIT' --console=plain
```

The [previous workload](../2026-10-05-replica-metrics/README.md) now runs with
32 workers. PostgreSQL 18.6 preloads `pg_stat_statements`, with tracking set to
`top` and verified before traffic. A dedicated diagnostic connection sits outside
service pool maxima. Statistics reset once after each topology's startup and seed;
subsequent captures are cumulative. Sampler queries carry an excluded marker.
The test requires positive request-path read-pin and version-reference-insert
call deltas after every mixed window; background activity alone cannot pass that
gate. Missing or decreasing query counters fail the test.

There are 9,984 recorded requests and 48 client windows, including warmup sample
-1. Measured samples 0–2 contain 7,488 attempts: 6,912 successful mixed operations,
and 576 contended attempts with 18 successes and 558 expected ABORTED results.
Byte equality, retained history, retry manifests and exactly one contention
winner remain mandatory. All six 1/2/4-process SQL-budget configurations passed.

`requests.csv.gz` preserves the raw request rows with lossless gzip compression.
`sql-captures.csv` records 54 captures, including six empty baselines after reset;
`sql-snapshots.csv` preserves rows from the 48 nonempty phase snapshots. Each row links to one of 53
normalized query texts by SHA-256; PostgreSQL query IDs are not used as content
hashes. `child-snapshots.csv` retains 126 provider/Hikari snapshots. Client summary
rates and quantiles follow the earlier diagnostic's formulas.

`sql-mixed.csv` subtracts the preceding contended snapshot from each measured
mixed snapshot and sums by topology/query key across samples 0–2. These intervals
include lifecycle activity and time between phases. Query execution times can
overlap across connections; their sum is not wall time. PostgreSQL's reported
`total_exec_time` does not separately identify CPU work and lock waits, and the
configured tracking scope does not provide a complete internal execution profile.

The locking select of the archive-statistics row has the largest cumulative
execution time in every configuration. For 288 measured mixed writes per
configuration, its accumulated time ranges from 634 ms (one process, pool maximum
four) to 10,704 ms (four processes, pool maximum four each). Mixed traffic ranges
from 242 to 292 successful requests/second. Extra replicas did not increase
throughput in this run.

Code inspection supplies a specific investigation target: `ArchiveLedger.commitSave`
locks the entry, publishes version/object references, then `applyDelta` locks the
shared archive-statistics row. Rendition-counter changes and the final ORM flush
and refresh calls occur before transaction commit releases that row lock. Thus
independent entries in one archive share a serialization point. The measurements
support investigating its lock duration; they do not yet prove how much of the
reported statement time is waiting or justify removing transactional counters.

Next, collect lock-wait evidence and test a shorter statistics-update critical
section while preserving exact atomic totals, conflict checks and failure recovery.
Do not infer a whole-repository rewrite from this shared counter.

Limitations remain: LocalStack, unreserved host, fixed configuration order,
growing retained history, fixed aggregate SQL maxima rather than fixed total
CPU/memory/provider capacity, and archive-only traffic. Enabling database profiling
also changes the measurement setup from the prior eight-worker run. These runs
are not a controlled before/after performance comparison and do not qualify typed
publication, production latency or replica scaling across every repository API.
