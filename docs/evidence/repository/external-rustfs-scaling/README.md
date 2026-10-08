# Journaled repository scaling on RustFS, 2026-10-08

One, two and four independent production-JAR JVM processes shared one PostgreSQL
18.6 and one pinned RustFS container under a fixed closed-loop workload. Every
measured window passed the same correctness checks. All numbers are limited by
this host and this fixture; they are not a capacity or deployment claim. How to
run and read the harness: [`docs/testing/repository-rustfs-scaling.md`](../../../testing/repository-rustfs-scaling.md).

## Identity

Source: branch `agent/repository-rustfs-scaling` from base
`4f4d66bffe0025783df178b82a35212424faabf6` plus the test-only changes in this
branch; no production source, SQL, provider or build file changed. Every raw
archive carries `source-identity.txt` with the SHA-256 of each production artifact
the workers loaded, the compiled probe JAR, and both image identities:

- `postgres:18-alpine`, image `sha256:77f585114c32fbca283dc835b0596f4e52b51b4c6662d7810b2f4084f60a1873`
- `rustfs/rustfs:1.0.0-beta.11-preview.1`, image `sha256:ea50257bc5e281e83170f49b84a109432d930f93bf5051c77d09105c5a62746b`

Host: Linux 7.0.0-34-generic, 32 CPUs, 121 GiB, Java 25.0.3 (GraalVM CE), Docker
with cgroup v2. PostgreSQL ran its image defaults (`synchronous_commit=on`,
`fsync=on`, `wal_sync_method=fdatasync`, `shared_buffers=128MB`,
`max_connections=100`) plus `pg_stat_statements`, `track_io_timing` and
`track_wal_io_timing`, which are observation settings. RustFS ran with
versioning and conditional writes enabled through the existing provider options.
No container had CPU or memory limits. The host was shared with other agents'
builds: one-minute load averages were 8 to 20 across the series (`series.log`),
and whole-host busy CPU exceeded the sum of this benchmark's components by about
5 to 8 cores in every window (`summary/windows.csv`).

## Runs

| Directory | Plan | Clients | Reader slots / handles | Budget | Result |
|---|---|---|---|---|---|
| `compare-1`, `compare-2`, `compare-3` | mirrored, 12 windows | 16 | 16 / 64 | 384,000,000 | 1 test, 0 failures each; 3m50s–3m54s |
| `load-4` | mirrored | 4 | 16 / 64 | 384,000,000 | passed |
| `load-8` | mirrored | 8 | 16 / 64 | 384,000,000 | passed |
| `load-32` | scaleout | 32 | 16 / 64 | 768,000,000 | **failed** in the first four-process window: reader slots refused |
| `load-32-slots32` | scaleout | 32 | 32 / 128 | 768,000,000 | passed |
| `trace-16` | mirrored, JDBC tracing | 16 | 16 / 64 | 384,000,000 | passed; attribution only |

All runs: journaled runtime, 64 KiB payload characters, 32 measured iterations per
client after 8 warmup iterations, 2048 MiB aggregate worker heap divided across
replicas. Each directory holds `raw.tar.gz` (every per-request timing, counter,
SQL snapshot and sampler file), `junit.xml.gz` and `gradle.log.gz`. The
correctness gate (1/2/4-process typed publication, rejection, exact replay,
competing-revision race, fresh-process historical reads) passed at the start of
every run, including the failed one.

`summary/` is the analyzer output over the six complete untraced runs:
`windows.csv` (68 windows), `configs.csv` (pooled per configuration) and
`workers.csv` (per process). `summary-trace/` is the same for the traced run plus
`trace-sites.csv`, the per-operation JDBC commit and execute counts by completion
site. Regenerate with:

```sh
java docs/evidence/repository/external-rustfs-scaling/RepositoryScalingReport.java <out> <extracted raw dirs...>
```

Every process received measured traffic: `workers.csv` lists each window's
PIDs with `clients × 32` measured operations, `clients × 16` PUTs and about 480
pool acquisitions per client. Every window's durable delta matched its measured
writes exactly (192 documents and revisions, 64 retained rejections at 16 clients).

## Results, 16 clients, three repetitions

Pooled over 36 windows. Throughput is primary operations per inclusive window
second (the window contains each write's receipt replay); latencies are per
operation, nearest-rank, replay excluded.

| Processes | SQL pool | ops/s (min–max) | read p50/p95/p99 ms | publish p50/p95/p99 ms | reject p50/p95/p99 ms | pool busy | acquire wait |
|---|---|---|---|---|---|---|---|
| 1 | 8 | 76.7 (74.4–78.3) | 64 / 90 / 101 | 283 / 338 / 371 | 479 / 548 / 575 | 0.87 | 3.8 ms |
| 2 | 4 each | 66.8 (64.8–70.1) | 80 / 114 / 128 | 323 / 391 / 420 | 541 / 618 / 653 | 0.86 | 4.8 ms |
| 2 | 8 each | 89.0 (86.9–91.4) | 49 / 68 / 76 | 257 / 294 / 313 | 414 / 459 / 471 | 0.62 | 0.06 ms |
| 4 | 2 each | 62.4 (60.8–64.7) | 85 / 120 / 135 | 339 / 401 / 440 | 590 / 687 / 747 | 0.85 | 5.2 ms |
| 4 | 8 each | 77.7 (74.1–80.5) | 59 / 79 / 95 | 283 / 354 / 393 | 469 / 536 / 587 | 0.32 | 0.005 ms |

Run-to-run spread is within 5% for every configuration. Two processes with
sixteen connections in total are 16% faster than one process with eight. Four
processes with thirty-two connections are no faster than one. Dividing eight
connections across processes is slower than one process holding all eight.

## Load series and saturation

| Clients | 1 proc / 8 | 2 procs / 8 each | 4 procs / 8 each | 2 procs / 4 each | 4 procs / 2 each |
|---|---|---|---|---|---|
| 4 | 29.3 | 28.0 | 23.2 | 27.4 | 24.1 |
| 8 | 55.3 | 50.0 | 45.6 | 48.7 | 44.3 |
| 16 | 76.7 | 89.0 | 77.7 | 66.8 | 62.4 |
| 32 | not runnable | 120.4 | 121.9 | 80.7 | 78.9 |

Operations per second; single runs except the 16-client row. The 32-client row
used 32 reader slots and 128 handles in total (the 16-client rows used 16 and
64), so its reader budget is not the same aggregate. One process cannot take 32
clients because a worker admits at most 16 clients and 384,000,000 budget bytes.

Saturation observed: one process with eight connections flattens between 8 and
16 clients (its pool is 87% busy and each publication waits 59 ms for
connections). Two and four processes are equal at 32 clients, at about 121 ops/s
mixed, with publish p95 at 407 and 453 ms; the shared WAL pipeline is busy for
84% and 79% of the sampled span and PostgreSQL uses 4.5 and 6.1 cores. The
32-client run with 16 aggregate reader slots refused in the four-process window
(`load-32`, 4 slots per process, 8 clients per process):
`DocumentPartReader.readBounded` acquires its slot with `tryAcquire` and throws
`RESOURCE_EXHAUSTED` ("Concurrent document read capacity exhausted") rather than
queue. That is the designed explicit backpressure, reached here by the reader
budget, not by the database.

## Where the time goes

From the traced run, one process, 16 clients (`summary-trace/trace-sites.csv`,
window t00; the same counts recur in every topology):

| Operation | JDBC commits | queries | updates | commit wait per op | acquire wait per op | provider |
|---|---|---|---|---|---|---|
| publish | 16 | 143 | 31 | 50 ms | 59 ms | 1 PUT (74 ms) + 1 read-back GET (2 ms) |
| reject | 35 | 267 | 32 | 100 ms | 128 ms | 1 PUT + 1 GET |
| historical read | 6 (+1 pin release in the maintenance tick) | 25 | 1 | 22 ms | 23 ms | 1 GET |
| replay | 1 | 7 | 0 | 4 ms | 5 ms | none |

Publication commits by completion site: `DocumentOperationUploadAdmission.captureReads` ×2,
`DocumentPublicationModesJournal.requireObservedModes` ×2,
`DocumentPublicationReplay.observe` ×2, `DocumentSelectedAttemptLedger.renewOwnerAndSelections` ×2,
and one each for `recheckInitialSelections`, `stage` (upload admission),
`DocumentPublicationCommit.commitInternal`, `DocumentPublicationRegistration.admitInitialAccepted`,
`preflight`, `DocumentSchemaPolicies.read`, `DocumentSelectedAttemptLedger.renew` and
`RepositorySchemaArtifacts.stage`; plus one `DocumentSelectedAttemptLedger.verifyBatch`
commit per upload from the observation flusher. The trace records client-side
commit calls; it cannot say which of them wrote WAL. The fsync count bounds that:
2,100 to 3,000 WAL fsyncs per 512-iteration window at 16 clients, so at least
4 to 6 WAL flushes per iteration after group commit.

Across all 68 windows, PostgreSQL executed statements for 21% to 26% of the time
connections were checked out; the rest is round trips (about 149 statements per
iteration), client work and idle-in-transaction time. WAL fsync time as a share
of the sampled span rose with offered load and not with topology: 0.45 to 0.50 at
4 clients, 0.61 to 0.64 at 8, 0.70 to 0.79 at 16, 0.79 to 0.84 at 32. Mean commit
wait in the traced run rose from 3.2 ms with one process to 3.5 ms with two and
3.9 ms with four at the same offered load. Sampled client-backend states agree:
at one process, 34% of samples were waiting on `LWLock/WALWrite` and 10% on
`IO/WalSync`; at four processes with 32 connections, 69% of samples were idle
and the remainder split the same way.

Worker CPU for the same 16-client throughput was 2.5 cores with one process,
4.5 with two and 6.3 with four. RustFS used 0.2 cores everywhere; PUT latency
for one 64 KiB object grew from 60 ms at 4 clients to 82 ms at 32 and did not
depend on process count.

## Bottleneck assessment

1. **Per-operation transaction and statement count against a synchronous WAL.**
   Sixteen commits and 174 statements per publication (35 and 299 per rejection)
   turn into 50 to 100 ms of commit waiting per write and keep the single WAL
   writer busy for 70% to 84% of the time at 16 to 32 clients. The WAL is shared
   by every process, so added processes cannot add commit capacity; they only
   raise the mean commit wait. This is the limiter that makes two and four
   processes equal at 32 clients. Fix, coordinator-owned: fold the paired
   pre/post reads (`captureReads`, `requireObservedModes`, `Replay.observe`,
   `renewOwnerAndSelections`) and the separate `renew`, `recheckInitialSelections`
   and `preflight` transactions into the transactions that already write, and
   cut read-only round trips; measure commits per publication before and after
   with the traced run. Do not trade this for `synchronous_commit=off`.
2. **Per-process SQL pool admission.** With eight connections, one process is
   87% busy at 16 clients and spends 59 ms per publication waiting for a
   connection, because each operation checks a connection out about 15 times and
   holds it 4.2 times longer than the server executes. Raising the pool is a
   deployment knob that moved the limit (two processes with 16 connections,
   +16%) but not past item 1. Fewer checkouts per operation (item 1) is the
   code-side fix; the fixed-total configurations show that splitting a small
   pool across processes is strictly worse.
3. **RustFS PUT latency, 60 to 82 ms per 64 KiB object.** It is a quarter of
   publication latency and sits outside SQL transactions, so it bounds latency
   rather than throughput at these concurrencies. RustFS CPU stayed at 0.2 cores.
   Not attributed further: the harness has no server-side RustFS timing and did
   not vary the HTTP client (`UrlConnectionHttpClient` with a SHA-256 checksum
   header, `S3BlobStoreProvider`) or the read-back GET. A targeted comparison is
   the next step before any provider-side change.
4. **Process overhead.** At 4 and 8 clients one process beats four, and worker
   CPU per operation grows 2.5× from one to four processes. More replicas are
   justified only when client concurrency exceeds one process's reader and
   client limits (16 clients, 384,000,000 budget bytes) and the pool is raised.
5. **Reader-slot refusal.** Under a fixed aggregate reader budget, splitting it
   four ways reached immediate `RESOURCE_EXHAUSTED` refusals at 8 clients per
   process. The workload probe treats a refusal as fatal, so throughput under
   partial refusal is unmeasured (gap below).

## Measurement gaps

- Lock waits are sampled states, not durations; `pg_stat_io` fsync time is the
  only measured wait duration. Cumulative PostgreSQL counters flush at most once
  per second, so each window's database deltas were read after a 1.2 s settle
  that is inside the sampled span but outside the inclusive window.
- CPU was not isolated. Unrelated host load of roughly 5 to 8 busy cores was
  present and varied; the 32-client windows ran at one-minute load averages of
  12 to 20. Repeated runs agree within 5%, which bounds but does not remove the
  effect.
- No soak: measured phases are 4 to 13 s. No transport, authentication, managed
  host, historical publication, recovery, pruning or multi-part documents.
- The 32-client row changed the aggregate reader budget. The probe cannot count
  refusals as outcomes, so no point exists beyond the refusal boundary.
- Commits that write WAL are not distinguished from read-only commits in the
  trace; the fsync count gives only a lower bound.
- The `db_commits_per_iteration` column is approximate (it includes pool
  validation and the maintenance tick); the traced counts are exact and agree
  within one commit per iteration.

## Exit criteria

| Criterion | Evidence |
|---|---|
| Runnable checked-in harness and instructions | `NativeReplicaRuntimeTest`, `RepositoryScalingSampler`, the init script and analyzer here; `docs/testing/repository-rustfs-scaling.md` |
| 1/2/4-process correctness gate | passed at the start of all eight runs (`junit.xml.gz`) |
| Three controlled repetitions with raw evidence | `compare-1..3`, 12 windows each, raw archives retained |
| Load and resource fairness documented | fixed total clients, iterations, payload, reader slots, handles, budget and heap per run; SQL pool as the single varied resource; CPU not controlled (stated) |
| Immutable image and source identity | `source-identity.txt` in every archive; digests above |
| No ignored unexpected errors | `load-32` failed and is retained as a refusal finding; every other run had zero failures, zero recorded provider or pool failures, zero SQL timeouts |
| Source-backed bottleneck attribution | trace completion sites, `DocumentPartReader.readBounded`, `DocumentPartTransfer.upload`, PostgreSQL WAL statistics |
| Numbers labeled as host-limited | this document |

Performance qualification is complete for this fixture and host at the stated
limits; it is not a deployment-capacity qualification. Sol's review is recorded
below once received.
