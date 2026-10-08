# Journaled repository scaling on RustFS, 2026-10-08

One, two and four independent production-JAR JVM processes shared one PostgreSQL
18.6 and one pinned RustFS container under a fixed closed-loop workload. Every
measured window passed the same correctness checks. All numbers are limited by
this host and this fixture; they are not a capacity or deployment claim. How to
run and read the harness: [`docs/testing/repository-rustfs-scaling.md`](../../../testing/repository-rustfs-scaling.md).

A first series was reviewed by Sol (Codex, gpt-5.6-sol); the review found no
ownership or correctness-gate problem and listed eight measurement and wording
defects. The harness and analyzer were corrected (window versus settled
snapshots, deterministic probe JAR and per-source hashes, run provenance in the
analyzer, trace aggregation, PIDs in `workers.csv`, a recorded sampling interval
with a control run, a warmup trend table) and the whole series was rerun. This
document describes the rerun only.

## Identity

Source: branch `agent/repository-rustfs-scaling` from base
`4f4d66bffe0025783df178b82a35212424faabf6` plus the test-only changes in this
branch; no production source, SQL, provider or build file changed. Every raw
archive carries `source-identity.txt` with the SHA-256 of each production artifact
the workers loaded, each probe source file, the compiled probe JAR (identical
across all nine runs: `fef7b235…decfd`), the parent sampler class, and both image
identities:

- `postgres:18-alpine`, image `sha256:77f585114c32fbca283dc835b0596f4e52b51b4c6662d7810b2f4084f60a1873`
- `rustfs/rustfs:1.0.0-beta.11-preview.1`, image `sha256:ea50257bc5e281e83170f49b84a109432d930f93bf5051c77d09105c5a62746b`

Host: Linux 7.0.0-34-generic, 32 CPUs, 121 GiB, Java 25.0.3 (GraalVM CE), Docker
with cgroup v2. PostgreSQL ran its image defaults (`synchronous_commit=on`,
`fsync=on`, `wal_sync_method=fdatasync`, `shared_buffers=128MB`,
`max_connections=100`) plus `pg_stat_statements`, `track_io_timing` and
`track_wal_io_timing`, which are observation settings. RustFS ran with
versioning and conditional writes enabled through the existing provider options.
No container had CPU or memory limits. The host was shared with other agents'
builds: one-minute load averages at window start ranged from 7.4 to 20.3
(`summary/windows.csv`), and whole-host busy CPU exceeded the sum of this
benchmark's components by 2.5 to 10.7 cores depending on the window.

## Runs

| Directory | Plan | Clients | Reader slots / handles | Budget | Sampling | Result |
|---|---|---|---|---|---|---|
| `compare-1`, `compare-2`, `compare-3` | mirrored, 12 windows | 16 | 16 / 64 | 384,000,000 | 25 ms | 1 test, 0 failures each; 3m46s–3m52s |
| `control-sample250` | mirrored | 16 | 16 / 64 | 384,000,000 | 250 ms | passed; sampler-load control |
| `load-4` | mirrored | 4 | 16 / 64 | 384,000,000 | 25 ms | passed |
| `load-8` | mirrored | 8 | 16 / 64 | 384,000,000 | 25 ms | passed |
| `load-32-slots16` | scaleout | 32 | 16 / 64 | 768,000,000 | 25 ms | **failed** in the first four-process window: reader slots refused |
| `load-32-slots32` | scaleout | 32 | 32 / 128 | 768,000,000 | 25 ms | passed |
| `trace-16` | mirrored, JDBC tracing | 16 | 16 / 64 | 384,000,000 | 25 ms | passed; attribution only |

All runs: journaled runtime, 64 KiB payload characters, 32 measured iterations per
client after 8 warmup iterations per client, 2048 MiB aggregate worker heap
divided across replicas. Each directory holds `raw.tar.gz` (every per-request
timing, counter, SQL snapshot and sampler file), `junit.xml.gz` and
`gradle.log.gz`; `series.log` has start and end times and load averages. The
correctness gate (1/2/4-process typed publication, rejection, exact replay,
competing-revision race, fresh-process historical reads) passed at the start of
every run, including the failed one.

`summary/` is the analyzer output over the seven complete untraced runs:
`windows.csv` (80 windows), `configs.csv` (pooled per configuration, with the
number of distinct runs), `workers.csv` (per process) and `warmup.csv`.
`summary-trace/` is the same for the traced run plus `trace-sites.csv`, the
per-window, per-operation JDBC commit and execute counts by completion site.
Regenerate with:

```sh
java docs/evidence/repository/external-rustfs-scaling/RepositoryScalingReport.java <out> <label>=<extracted raw dir>...
```

Every process received measured traffic: `workers.csv` lists each window's
worker PIDs with `clients × 32` measured operations, `clients × 16` PUTs and
about 480 pool acquisitions per client. Every window's durable delta matched its
measured writes exactly (192 documents and revisions, 64 retained rejections at
16 clients).

## Results, 16 clients, three repetitions

Pooled over 36 windows with 25 ms sampling. Throughput is primary operations per
inclusive window second (the window contains each write's receipt replay, which
is not timed separately); latencies are per operation, nearest-rank.

| Processes | SQL pool | ops/s (min–max) | read p50/p95/p99 ms | publish p50/p95/p99 ms | reject p50/p95/p99 ms | pool busy | acquire wait |
|---|---|---|---|---|---|---|---|
| 1 | 8 | 75.5 (69.0–80.0) | 66 / 91 / 107 | 286 / 338 / 364 | 490 / 575 / 751 | 0.87 | 4.0 ms |
| 2 | 4 each | 69.6 (66.6–72.0) | 74 / 105 / 121 | 309 / 366 / 403 | 523 / 594 / 639 | 0.86 | 4.4 ms |
| 2 | 8 each | 90.9 (88.8–93.3) | 49 / 68 / 76 | 250 / 285 / 306 | 401 / 463 / 517 | 0.61 | 0.06 ms |
| 4 | 2 each | 63.9 (62.2–65.9) | 84 / 118 / 134 | 329 / 389 / 417 | 576 / 654 / 688 | 0.84 | 5.1 ms |
| 4 | 8 each | 79.4 (65.5–85.5) | 57 / 81 / 91 | 278 / 357 / 401 | 452 / 565 / 689 | 0.31 | 0.005 ms |

The two lowest windows of the series (`compare-2` t10 at 65.5 ops/s with four
processes and eight connections each, and `compare-2` t11 at 69.0 with one
process) started at one-minute host load averages of 17.9 and 19.2, the highest
of the series; the other windows of those configurations were 77.5 to 85.5 and
73.3 to 80.0. Excluding those two, every configuration's windows lie within 10%
of each other. Two processes with sixteen connections in total are 20% faster than one
process with eight. Four processes with thirty-two connections are about 5%
faster than one, within the spread. Dividing eight connections across processes
is slower than one process holding all eight.

The 250 ms sampling control reproduced every configuration within its spread
(76.8, 69.3, 92.2, 62.8 and 81.4 ops/s), so the parent's backend-state sampling
is not a material load at this scale.

Warmup trend (`summary/warmup.csv`, mean latency of the first eight versus the
last eight measured iterations of every client): publication latency in the first eight iterations is 4% lower than in the
last eight for one process (pool queueing builds up), 2% higher for two
processes with eight connections each, and 18% higher for four processes with
eight each (319 versus 271 ms); reads show no consistent trend. Four cold JVMs that each
saw only 32 warmup operations are penalised in the first quarter of their
window; the warm tail of the four-process configuration is still slower than
the two-process configuration's tail (271 versus 246 ms).

## Load series and saturation

| Clients | 1 proc / 8 | 2 procs / 8 each | 4 procs / 8 each | 2 procs / 4 each | 4 procs / 2 each |
|---|---|---|---|---|---|
| 4 | 32.6 | 29.5 | 24.7 | 28.0 | 25.3 |
| 8 | 55.9 | 51.0 | 46.3 | 49.5 | 46.0 |
| 16 | 75.5 | 90.9 | 79.4 | 69.6 | 63.9 |
| 32 | not runnable | 124.2 | 130.0 | 81.9 | 79.4 |

Operations per second; single runs except the 16-client row. The 32-client row
used 32 reader slots and 128 handles in total (the other rows used 16 and 64),
so its reader budget is not the same aggregate. One process cannot take 32
clients because a worker admits at most 16 clients and 384,000,000 budget bytes.

One process with eight connections flattens between 8 and 16 clients: its pool
is 87% busy and each publication waits 65 ms for connections. Two and four
processes with eight connections each are within 5% of each other at 32
clients (124 and 130 ops/s, publish p95 398 and 429 ms) while PostgreSQL uses
5.1 and 7.3 cores and its WAL flush occupancy is 97% and 91% of the window.
The 32-client run with 16 aggregate reader slots refused in the four-process
window (`load-32-slots16`, 4 slots per process, 8 clients per process):
`DocumentPartReader.readBounded` acquires its slot with `tryAcquire` and throws
`RESOURCE_EXHAUSTED` ("Concurrent document read capacity exhausted") rather than
queue. That is the designed explicit backpressure, reached here by the reader
budget, not by the database.

## Where the time goes

From the traced run, one process, 16 clients, window t00
(`summary-trace/trace-sites.csv`; the same counts recur in every topology):

| Operation | scoped JDBC commits | JDBC execute calls (query / update / batch) | commit call time per op | acquire wait per op | provider |
|---|---|---|---|---|---|
| publish | 16 | 177 (143 / 31 / 3) | 53 ms | 65 ms | 1 PUT (74 ms) + 1 read-back GET (2 ms) |
| reject | 35 | 300 (267 / 32 / 1) | 98 ms | 133 ms | 1 PUT + 1 GET |
| historical read | 6 | 26 (25 / 1 / 0) | 23 ms | 27 ms | 1 GET |
| replay | 1 | 7 (7 / 0 / 0) | 4 ms | 6 ms | none |

Outside the operation scopes the window also recorded 579 commits: one
`DocumentReadPins.finish` per read from the maintenance tick (256), one
`DocumentSelectedAttemptLedger.verifyBatch` per upload from the observation
flusher (256) and 67 `DocumentAssessmentReadProtection.finish`. Publication
commits by completion site: `DocumentOperationUploadAdmission.captureReads` ×2,
`DocumentPublicationModesJournal.requireObservedModes` ×2,
`DocumentPublicationReplay.observe` ×2, `DocumentSelectedAttemptLedger.renewOwnerAndSelections` ×2,
and one each for `recheckInitialSelections`, `stage` (upload admission),
`DocumentPublicationCommit.commitInternal`, `DocumentPublicationRegistration.admitInitialAccepted`,
`preflight`, `DocumentSchemaPolicies.read`, `DocumentSelectedAttemptLedger.renew` and
`RepositorySchemaArtifacts.stage`. The trace counts client-side `commit()` calls
and their duration; it cannot say which commits wrote WAL, and a read-only
commit still costs a round trip.

Across all 80 windows PostgreSQL executed statements for 19% to 30% of the time
connections were checked out (20% to 24% at 16 clients), at 149 execute calls and 15 checkouts per
iteration in every topology; the rest is round trips, client work and
idle-in-transaction time. `pg_stat_database` counted 13.7 to 15.5 commits per
iteration, 2.8 commits per WAL fsync.

WAL: PostgreSQL serialises WAL writes and flushes under `WALWriteLock`
(`XLogFlush` → `XLogWrite`), and in these runs client backends performed 99.7% of
the WAL fsyncs (`walio_client_backend_normal_*` rows in each `-scaling.csv`;
the walwriter and checkpointer are separate rows). Summed fsync call time over
the measured window therefore approximates flush occupancy; it is a sum of call
durations, not a lock-hold measurement. That occupancy rose with offered load
and not with process count: 0.55 to 0.64 at 4 clients, 0.73 to 0.84 at 8, 0.86
to 0.94 at 16, 0.91 to 0.97 at 32. Reading the counters again after a 1.2 s
settle changed the totals by less than 0.1%. Mean `commit()` call time in the
traced run was 3.3 ms with one process, 3.7 ms with two and 3.2 ms with four.
Sampled client-backend states agree: at one process 36% of client-backend samples were waiting on
`LWLock/WALWrite` and 11% on `IO/WalSync`; at four processes with 32 connections
69% of samples were idle and the active remainder split the same way
(`compare-1` t00 and t10 activity files).

Worker CPU for the same 16-client throughput was 2.8 cores with one process,
5.4 with two and 7.3 with four (window delta, exact per-process CPU). RustFS
used 0.1 to 0.4 cores; its PUT latency for one 64 KiB object grew from 52 to
63 ms at 4 clients to 75 to 83 ms at 32 and did not depend on process count.

## Bottleneck assessment

1. **Transaction and round-trip count per operation against a synchronous
   WAL** is the strongest candidate for the shared limit. Sixteen scoped commits
   plus one asynchronous verify commit and 177 execute calls per publication (35
   and 300 per rejection) cost 53 to 98 ms of commit calls and 65 to 133 ms of
   pool waiting per write at 16 clients, and keep WAL flush occupancy above 85%
   from 16 clients upward whatever the process count. Three independent signals
   point the same way (occupancy, `WALWrite` lock samples, commit time that does
   not fall when connections are added), but causality is not proven by this
   fixture: confirming it needs an intervention, namely fewer commits per
   operation, measured with the traced run before and after. Fix,
   coordinator-owned: fold the paired pre/post reads (`captureReads`,
   `requireObservedModes`, `Replay.observe`, `renewOwnerAndSelections`) and the
   separate `renew`, `recheckInitialSelections` and `preflight` transactions
   into the transactions that already write, and cut read-only round trips. Do
   not trade this for `synchronous_commit=off`.
2. **Per-process SQL pool admission.** With eight connections, one process is
   87% busy at 16 clients and spends 65 ms per publication waiting for a
   connection, because each operation checks a connection out 15 times and holds
   it about four times longer than the server executes. Raising the pool moved
   the limit (two processes with 16 connections, +20%) but not past item 1.
   Fewer checkouts per operation (item 1) is the code-side fix; the fixed-total
   configurations show that splitting a small pool across processes is strictly
   worse.
3. **RustFS PUT plus read-back, 52 to 83 ms per 64 KiB object,** is on the
   synchronous critical path of every write (`DocumentPartTransfer.upload`,
   awaited by `DocumentUploadCoordinator` before preparation), so in this
   closed-loop workload it reduces throughput as well as adding a quarter of
   publication latency. RustFS CPU stayed at 0.1 to 0.4 cores, which does not
   rule out storage, network or client serialisation. Not attributed further:
   the harness has no server-side RustFS timing and did not vary the HTTP
   client (`UrlConnectionHttpClient` with a SHA-256 checksum header in
   `S3BlobStoreProvider`) or the read-back GET. A controlled provider and client
   comparison is the next step before any provider-side change.
4. **Process overhead.** At 4 and 8 clients one process beats four, worker CPU
   per operation grows 2.6× from one to four processes, and four cold JVMs lose
   18% on publication in their first quarter window. More replicas are
   justified only when client concurrency exceeds one process's reader and
   client limits (16 clients, 384,000,000 budget bytes) and the pool is raised.
5. **Reader-slot refusal.** Under a fixed aggregate reader budget, splitting it
   four ways reached immediate `RESOURCE_EXHAUSTED` refusals at 8 clients per
   process. The workload probe treats a refusal as fatal, so throughput under
   partial refusal is unmeasured (gap below).

## Measurement gaps

- Lock waits are sampled states, not durations; `pg_stat_io` fsync call time is
  the only measured wait duration, and it is a sum over backends, not a
  lock-hold measurement.
- CPU was not isolated. Unrelated host load of roughly 2.5 to 11 busy cores was
  present and varied; two windows coincided with load spikes and are reported
  inside their configurations' spread rather than removed.
- Warmup is eight iterations per client, so warmup per JVM falls with process
  count; the warmup table quantifies the effect but the comparison is between
  short-lived processes. Changing the warmup volume needs the workload probe,
  which this branch does not own.
- No soak: measured phases are 4 to 13 s. No transport, authentication, managed
  host, historical publication, recovery, pruning or multi-part documents.
- The 32-client row changed the aggregate reader budget. The probe cannot count
  refusals as outcomes, so no point exists beyond the refusal boundary.
- Commits that write WAL are not distinguished from read-only commits in the
  trace; `db_commits_per_iteration` includes pool validation and maintenance.
- Replay latency is inside the throughput window but has no per-request row.

## Exit criteria

| Criterion | Evidence |
|---|---|
| Runnable checked-in harness and instructions | `NativeReplicaRuntimeTest`, `RepositoryScalingSampler`, the init script and analyzer here; `docs/testing/repository-rustfs-scaling.md` |
| 1/2/4-process correctness gate | passed at the start of all nine runs (`junit.xml.gz`) |
| Three controlled repetitions with raw evidence | `compare-1..3`, 12 windows each, `runs=3` in `summary/configs.csv`, raw archives retained |
| Load and resource fairness documented | fixed total clients, iterations, payload, reader slots, handles, budget and heap per run; SQL pool as the single varied resource; CPU and per-JVM warmup not controlled (stated) |
| Immutable image and source identity | `source-identity.txt` in every archive; digests above; identical probe JAR hash across runs |
| No ignored unexpected errors | `load-32-slots16` failed and is retained as a refusal finding; every other run had zero failures, zero recorded provider or pool failures, zero SQL timeouts |
| Source-backed bottleneck attribution | trace completion sites, `DocumentPartReader.readBounded`, `DocumentPartTransfer.upload`, `DocumentUploadCoordinator`, PostgreSQL WAL statistics and `WALWriteLock` serialisation |
| Numbers labeled as host-limited | this document |

Performance qualification is complete for this fixture and host at the stated
limits; it is not a deployment-capacity qualification.
