# Repository scaling qualification on RustFS

This guide runs the journaled repository across one, two and four independent
production-JAR JVM processes that share one PostgreSQL and one RustFS container,
and turns the raw output into a window-by-window resource and latency report.
Results are limited by the host they ran on; the harness records that host.
The retained measurements are in
[`docs/evidence/repository/external-rustfs-scaling/`](../evidence/repository/external-rustfs-scaling/README.md).

## What runs

`NativeReplicaRuntimeTest` compiles the probe sources under
`repo/container/src/test/resources/runtime-inventory/` against the observed
production artifact set, then starts `postgres:18-alpine` and the pinned
`rustfs/rustfs:1.0.0-beta.11-preview.1` through Testcontainers. Every worker is a
separate `java -Xmx… -cp <production jars>:<probe jar> NativeReplicaProbe` process
that receives only environment variables: JDBC URL, S3 endpoint and credentials.
Workers never share a JVM, a connection pool, a reader ledger or a payload budget.

The correctness gate always runs first and is identical for the test and the
benchmark task:

1. `seed` creates the versioned bucket, drive, backend binding and admission policy.
2. `write` windows with one, two and four processes publish typed documents through
   `DocumentPublicationRuntime.execute`, including one invalid schema per worker
   that must produce a retained admission rejection, then replay each command with
   no payload and require the identical terminal receipt.
3. A two-process `race` window updates the same revision from both processes with a
   validation-time barrier; exactly one wins and the loser receives
   `PRECONDITION_NOT_MET` without an assessment.
4. A fresh `read` process after every window observes each terminal result or
   rejection through `DocumentPublicationReplay`, reads the retained historical
   document through `DocumentHistoricalOperations.readValidated`, compares bytes,
   mutation revision, command hash, manifest checksums and ownership, and checks
   SQL revision counts for the race winner and the rejected creates.

The benchmark then runs `traffic` windows. Each worker process builds
`DocumentPublicationRuntime.journaled(...)` when `nativeBenchmarkJournaled=true`
(the ordinary constructor otherwise), a `DocumentPartReader` bound to the RustFS
store, a `DocumentHistoricalOperations` reader and a maintenance thread calling
`runtime.tick()` every 25 ms. Each client runs a fixed sequence per iteration:
even iterations are historical reads of a 64 KiB seed document with byte and
revision checks; every eighth iteration is an invalid-schema publication that must
return a retained `ADMISSION_REJECTED` receipt; the rest are typed publications
that must return one member. Every write is followed by an exact receipt replay.
Eight warmup iterations per client precede a start barrier; only the measured
phase is timed and only after every worker reported readiness.

Paths exercised: ordinary typed document publication through the private journaled
runtime, retained admission rejection, exact terminal replay, historical validated
reads, and the maintenance tick. Not exercised: the public gRPC transport,
authenticated callers, managed-host lifecycle, historical publication,
successor/recovery paths, pruning, and any live deployment. Numbers here do not
describe those paths.

## Properties and their meaning

Every `nativeBenchmark*` property is a Gradle project property (`-P`). The parent
test divides aggregate values across the replicas of each window; the worker
reports the value it actually applied in `<window>-<index>-config.txt`, and the
parent asserts it.

| Property | Scope | Default | Meaning |
|---|---|---|---|
| `nativeBenchmarkJournaled` | all workers | `false` | `true` builds the journaled runtime. Set it explicitly. |
| `nativeBenchmarkClients` | aggregate | 4 | Closed-loop clients in total: 4, 8, 16 or 32. Each window gives `clients / replicas` to every worker. 32 requires the `scaleout` plan because a worker admits at most 16 clients. |
| `nativeBenchmarkIterations` | per client | 32 | Measured iterations per client, a multiple of eight. Total measured operations per window is `clients × iterations` regardless of topology. |
| `nativeBenchmarkPayloadBytes` | per document | 0 | ASCII payload characters per published document; 0 is the original tiny fixture. |
| `nativeBenchmarkReadSlots` | aggregate | 0 | Total `DocumentPartReader` slots divided across replicas; 0 keeps 8 per worker (so aggregate grows with replicas). |
| `nativeBenchmarkReadHandles` | aggregate | 0 | Total `DocumentReadLedger` handles divided across replicas; 0 keeps 32 per worker. |
| `nativeBenchmarkBudgetBytes` | aggregate | 0 | Total `PayloadBudget` bytes divided across replicas; 0 keeps 128,000,000 per worker. Initial admission reserves about 18 MiB per in-flight call, so 16 clients on one worker need at least 384,000,000. A worker refuses more than 384,000,000. |
| `nativeBenchmarkHeapMiB` | aggregate | 0 | Total `-Xmx` across replicas, forwarded by the init script; 0 keeps 512 MiB per worker. |
| `nativeBenchmarkPlan` | run | `mirrored` | `mirrored` is the twelve-window `f1 a4 f2 a1 f4 a2 a2 f4 a1 f2 a4 f1` sequence; `scaleout` is `a2 a4 f2 f4 f4 f2 a4 a2`. |
| `nativeBenchmarkTrace` | all workers | `false` | Per-operation JDBC commit/execute attribution by completion site through a test-only proxy. It adds overhead; use it for attribution, not for the compared numbers. |

Window names encode the SQL pool: `fN` fixes eight connections in total
(`8 / N` per worker), `aN` gives eight connections to every worker. Every window
therefore has the same total clients, iterations, payload, aggregate reader slots,
read handles, payload budget and heap; the only differences between `f` and `a`
are the SQL connections.

Not controlled: CPU. Workers, PostgreSQL and RustFS share the host without cgroup
limits, so four JVMs have four times the GC and JIT threads. The report records
host, container and worker CPU per window so that this can be checked rather than
assumed. No window is a soak; each measured phase lasts a few seconds.

## Commands

Serialise heavy runs on a shared host with the common advisory lock. Gradle never
replays a cached benchmark (the task opts out of up-to-date checks and the build
cache). Each command below runs the correctness gate first.

Correctness gate only:

```sh
flock -w 600 /tmp/protomolt-repository-qualification.lock \
  ./gradlew :protomolt-repo-container:nativeReplicaTest --max-workers=2 --console=plain
```

Measured comparison (one repetition; run it three times):

```sh
flock -w 600 /tmp/protomolt-repository-qualification.lock \
  ./gradlew -I docs/evidence/repository/external-rustfs-scaling/RepositoryScalingBenchmark.init.gradle \
  :protomolt-repo-container:nativeReplicaBenchmark \
  -PnativeBenchmarkJournaled=true -PnativeBenchmarkPlan=mirrored \
  -PnativeBenchmarkClients=16 -PnativeBenchmarkIterations=32 -PnativeBenchmarkPayloadBytes=65536 \
  -PnativeBenchmarkReadSlots=16 -PnativeBenchmarkReadHandles=64 \
  -PnativeBenchmarkBudgetBytes=384000000 -PnativeBenchmarkHeapMiB=2048 \
  --max-workers=2 --console=plain
```

Load series: repeat with `-PnativeBenchmarkClients=4` and `8`, then
`-PnativeBenchmarkClients=32 -PnativeBenchmarkPlan=scaleout -PnativeBenchmarkBudgetBytes=768000000`.
With 16 aggregate reader slots the four-process window of that run refuses reads
(`DocumentPartReader` takes its slot with `tryAcquire` and answers
`RESOURCE_EXHAUSTED`; the workload treats that as fatal), so the retained
32-client measurement also passed `-PnativeBenchmarkReadSlots=32
-PnativeBenchmarkReadHandles=128`, which changes the aggregate reader budget and
is labeled as such. Attribution: repeat the comparison once with
`-PnativeBenchmarkTrace=true`.

The run prints `NATIVE_BENCHMARK_OUTPUT=<dir>` under
`repo/container/build/native-replica-benchmark/<uuid>/`. Archive that directory
whole; it contains `environment.txt` (settings, host, PostgreSQL settings),
`source-identity.txt` (SHA-256 of every production artifact, the compiled probe,
and both image IDs with registry digests), `windows.csv`, `processes.csv`
(PID, heap and exit code of every worker), per worker `-operations.csv` (every
measured request with start and elapsed nanoseconds), `-measure-metrics.csv`
(provider and pool callback counters), per window `-scaling.csv` (PostgreSQL,
host, container and child CPU deltas), `-sql.csv` plus `-query-*.sql`
(`pg_stat_statements` per window), `-activity.csv`/`-locks.csv` (sampled backend
states), `-rss.csv`, `-baseline.csv` and `-durable-delta.csv`.

Summaries:

```sh
java docs/evidence/repository/external-rustfs-scaling/RepositoryScalingReport.java <output-dir> <raw-dir>...
```

The analyzer recounts every window's operations, refuses recorded failures or
durable-count mismatches, and writes `windows.csv`, `configs.csv` and
`workers.csv`. Throughput is measured operations divided by inclusive window time
(which contains the receipt replays and parent polling); latency percentiles are
nearest-rank over individual measured operations, excluding the replay that
follows each write, which is reported separately.

## Reading the resource columns

- `pool_busy_fraction`: summed Hikari connection usage over window time and total
  connections. Near 1.0 means the pool is the admission limit.
- `sql_acquire_mean_ms`: mean wait to borrow a connection. Pool waiting, not SQL.
- `statement_exec_total_s` versus `sql_usage_total_s`: server execution versus the
  time connections were checked out. The difference is round trips, client work
  and idle-in-transaction time.
- `db_commits_per_iteration`: `pg_stat_database.xact_commit` delta minus the
  samplers' own marked statements, per primary operation. Maintenance ticks and
  pool validation also commit, so treat it as an upper bound; trace runs give the
  exact client-side `jdbc_commit` count per operation scope.
- `wal_fsync_time_s`, `wal_write_time_s`: `pg_stat_io` WAL object timings with
  `track_wal_io_timing=on`, summed over backends. Cumulative statistics flush at
  most once a second per backend; the parent waits 1.2 s after the last worker
  finishes and before releasing the workers, outside the inclusive window.
- `postgres_cores`, `rustfs_cores`: container cgroup CPU over window time.
- `worker_cores`: exact `ProcessHandle` CPU of the worker JVMs over window time.
- `host_busy_cores`, `host_iowait_cores`: whole-host `/proc/stat` deltas, which
  include unrelated processes; compare with the three component values above.
