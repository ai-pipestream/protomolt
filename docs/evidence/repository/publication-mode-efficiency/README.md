# Publication-mode validation: one fenced transaction

Branch `agent/publication-mode-efficiency` from base
`5f8acfb413febd0a3ce1233d06d9a143d1220f86`. The change is in
`DocumentPublicationModesJournal.requireObservedModes` and the new
`DocumentPublicationModeValidation`; the design, statement map and test mapping
are in [`docs/testing/publication-mode-efficiency.md`](../../../testing/publication-mode-efficiency.md).
Every number here is limited to this host and this fixture; none is a capacity or
deployment claim. The RustFS harness was run unchanged.

## What was measured

Two series of the pinned journaled workload from
[`docs/testing/repository-rustfs-scaling.md`](../../../testing/repository-rustfs-scaling.md):
`mirrored` plan (one, two and four processes, twelve windows), 16 clients, 32
measured iterations per client after 8 warmup iterations, 64 KiB payloads, 16
reader slots, 64 handles, 384,000,000 budget bytes, 2048 MiB aggregate heap,
25 ms sampling. Three untraced repetitions and one traced repetition per series.

| Series | Production source | Container jar SHA-256 (prefix) | Runs |
|---|---|---|---|
| baseline | `5f8acfb413febd0a3ce1233d06d9a143d1220f86`, tree clean | `5454a883fb21` | `baseline-1`, `baseline-2`, `baseline-3`, `baseline-trace` |
| candidate | `1c9faac60852818ea023c0ef6e67327fa0731929`, tree clean | `db802153c133` | `candidate-1`, `candidate-2`, `candidate-3`, `candidate-trace` |

Across all eight runs `source-identity.txt` records the same compiled probe JAR
(`71bda593ce00…`), the same probe sources and parent sampler class, and the same
images: `postgres:18-alpine`
`sha256:77f585114c32fbca283dc835b0596f4e52b51b4c6662d7810b2f4084f60a1873` and
`rustfs/rustfs:1.0.0-beta.11-preview.1`
`sha256:ea50257bc5e281e83170f49b84a109432d930f93bf5051c77d09105c5a62746b`. The
container jar is the only artifact that differs. Host: Linux 7.0.0-34-generic, 32
CPUs, 121 GiB, Java 25.0.3 (GraalVM CE), Gradle 9.6.1, Docker with cgroup v2,
no CPU or memory limits. PostgreSQL ran its image defaults (`synchronous_commit=on`,
`fsync=on`, `shared_buffers=128MB`, `max_connections=100`) plus the observation
settings `pg_stat_statements`, `track_io_timing` and `track_wal_io_timing`.

Each run directory holds `raw.tar.gz` (every per-request timing, counter, SQL
snapshot and sampler file), `junit.xml.gz` (the correctness gate and the
benchmark, 1 test, 0 failures in every run), `gradle.log.gz` and `exit-code.txt`.
`series.log` records start and end times, HEAD, tracked-file cleanliness, lock
wait and load averages. `summary-baseline`, `summary-candidate`,
`summary-baseline-trace` and `summary-candidate-trace` are the unchanged
analyzer's output; regenerate with
`java docs/evidence/repository/external-rustfs-scaling/RepositoryScalingReport.java <out> <label>=<raw dir>...`.

## Round trips per operation (traced run, window t00, one process, 16 clients)

Client-side JDBC counts by completion site, 192 publications and 64 rejections
in the window (`summary-*-trace/trace-sites.csv`):

| Per operation | baseline publish | candidate publish | baseline reject | candidate reject |
|---|---|---|---|---|
| scoped `commit()` | 16 | **15** | 35 | **33** |
| pool acquisitions | 16 | **15** | 35 | **33** |
| `executeQuery` | 143 | **138** | 267 | **257** |
| `executeUpdate` | 31 | 31 | 32 | 32 |
| `executeBatch` | 3 | 3 | 1 | 1 |
| commit call time | 53.7 ms | 48.2 ms | | |
| acquisition wait | 62.4 ms | 59.5 ms | | |

A rejection validates modes twice (`DocumentPublicationExecution.executeAssessed`
and `DocumentAssessmentCreation`), so it loses two commits and ten statements. At the site itself, per window of 192
publications: `requireObservedModes` commits 384 → 192, commit call time
1584 ms → 753 ms, acquisition wait 1604 ms → 856 ms, connection usage
1980 ms → 1078 ms. The reads moved from
`DocumentPublicationPreparationJournal.capture` (384 queries, 35.5 ms) plus
`DocumentPublicationModesJournal.readModes` (192, 18.3 ms) to
`DocumentPublicationModeValidation.capture` (384, 51.4 ms): one fewer statement
and the same server time. The same deltas recur in every window of both traced
runs. `pg_stat_database` agrees: 14.72 → 14.41 commits per iteration at one
process, 14.61 → 13.77 at four processes with eight connections each.

Statement-level counts for one call on real PostgreSQL come from
`PublicationModeEfficiencyIT` (`it-baseline/` against the base tree,
`it-candidate/` against the change; both JUnit XML files carry the printed
`PUBLICATION_MODE_EFFICIENCY` lines):

| One call | acquisitions | commits | rollbacks | statements |
|---|---|---|---|---|
| baseline, first transaction (measured by the corruption cases) | 1 | 1 | 0 | 7 |
| baseline, second transaction (trace: 2 acquisitions and 2 commits per call) | 1 | 1 | 0 | 3 |
| candidate, valid | 1 | 1 | 0 | 5 |
| candidate, any refusal | 1 | 0 | 1 | 5 |
| candidate, claim-only | 1 | 1 | 0 | 4 |

Against the base tree 17 of the 20 new cases fail, as expected; the three that
pass are the authorization, bound-modes and takeover-before-fence cases, whose
behaviour did not change. The baseline valid case fails on
`CapacityExceededException` before its count assertion, because the exact
two-row budget it uses is smaller than the old flat 1 MiB modes reservation.

## Throughput and latency, three repetitions each

Pooled over the windows of each configuration (`summary-*/configs.csv`),
operations per inclusive window second, publication latency nearest-rank per
operation:

| Processes × pool | baseline ops/s mean (min–max) | candidate ops/s mean (min–max) | baseline publish p50/p95/p99 ms | candidate publish p50/p95/p99 ms |
|---|---|---|---|---|
| 1 × 8 | 76.5 (69.1–82.1) | 77.9 (74.7–80.4) | 284 / 337 / 363 | 276 / 331 / 365 |
| 2 × 4 | 69.9 (67.1–72.4) | 72.5 (70.1–76.8) | 309 / 360 / 380 | 296 / 349 / 369 |
| 2 × 8 | 92.9 (89.4–94.4) | 93.2 (90.0–97.6) | 247 / 280 / 292 | 241 / 276 / 289 |
| 4 × 2 | 64.1 (60.5–66.1) | 64.8 (62.9–67.2) | 328 / 386 / 446 | 321 / 384 / 413 |
| 4 × 8 | 81.2 (78.0–86.3) | 82.4 (80.6–84.7) | 273 / 326 / 357 | 266 / 331 / 360 |

Reads and rejections (`read` p50/p95/p99, `reject` p50/p95/p99, ms): one
process 66.7/91.3/104.4 and 477/562/625 baseline against 65.3/91.0/105.4 and
466/557/616 candidate; two processes with eight each 48.0/66.2/76.4 and
387/431/454 against 49.0/67.4/74.0 and 397/433/446.

Every configuration mean moved in the direction of the round-trip reduction
(+0.4% to +3.7% throughput, publication p50 lower by 6 to 13 ms), but every
candidate range overlaps the baseline range, so the latency and throughput
change is not distinguishable from run-to-run variation in this fixture. The
expected size of the effect is consistent with that: one commit call (3.3 to
4.1 ms here) plus one acquisition wait (4 ms at one process, under 0.1 ms with
sixteen or more connections) out of a 240 to 330 ms publication. The verified
claim is the round-trip reduction above, not a latency improvement.

Warmup (`summary-*/warmup.csv`, first eight versus last eight measured
iterations per client, ratio of first to last) shows the same pattern in both
series: publication within 4% at one process and at two processes with four
connections each, 8% slower in the first eight iterations at two processes with
eight each (1.086 and 1.076), 2% to 5% at four processes with two each, and 8%
(baseline) to 22% (candidate) at four processes with eight each, where each
cold JVM saw only 32 warmup operations; reads show no consistent trend.

## Host interference

The host was shared with other agents' builds and test runs. One-minute load
averages at window start: baseline 5.8 to 16.4, candidate 7.4 to 16.9
(`loadavg_begin` in `summary-*/windows.csv`). Whole-host busy CPU exceeded the
sum of worker, PostgreSQL and RustFS cores by 2.4 to 6.2 cores in the baseline
windows and 2.4 to 7.7 in the candidate windows, above 4.5 cores in three
windows of each series; `candidate-1` t00 started under a 13.7-core host load (the highest of either
series) and is the lowest one-process candidate window (75.9 ops/s). Lock waits
on `/tmp/protomolt-repository-qualification.lock` were 0 s for every baseline
run and 34, 38 and 52 s for `candidate-2`, `candidate-3` and `candidate-trace`
(another agent held the lock between runs); the wait is outside the measured
window and is recorded separately from the results. No run was isolated and no
window was excluded.

## Decode inside the fence

The candidate decodes the preparation and modes while the claim and owner rows
are locked. `decodeWindowInsideTheFenceIsMeasured` recorded the time from the
completion of the rows statement to the commit call, five consecutive calls on
the fixture (2,011-byte preparation, 43-byte modes): 3.6, 2.9, 2.2, 1.9 and
2.1 ms, which includes Hibernate's pre-commit work and the test proxy. The modes
column bound is 1 MiB; parsing a 1,011,391-byte, 9,000-entry modes object took
9.5 ms. The preparation codec bound is 16 MiB; a decode at that size was not
measured and is listed as a gap. The lock-hold extension at the fixture size is
therefore a few milliseconds against the 4 ms commit and up to 65 ms
acquisition wait that the second transaction cost in the earlier scaling
evidence.

## Suites

| Run | Task | Result |
|---|---|---|
| `it-baseline` | `:protomolt-repo-container:test --tests PublicationModeEfficiencyIT` on the base tree | 20 tests, 17 failures, 0 skipped (expected: the base path has 2 commits and 10 statements) |
| `it-candidate` | `PublicationModeEfficiencyIT` and `DocumentPublicationModesJournalIT` | 20 + 22 tests, 0 failures, 0 skipped |
| `regression-publication-replay` | `test --tests '*Publication*' '*Replay*' '*Journal*' '*Successor*' '*ReservedPreparation*' '*ScopedRegistration*'` | 93 classes, 625 tests, 0 failures, 0 errors, 0 skipped (lock wait 203 s) |
| `admission-storage` | `:protomolt-repo-container:admissionStorageTest` | `DocumentAssessmentStorageRuntimeTest` 5 tests, 0 failures, 0 errors, 0 skipped, 1171 s (lock wait 172 s on the second attempt) |
| `native-replica-gate` | `:protomolt-repo-container:nativeReplicaTest` | `NativeReplicaRuntimeTest` 1 test, 0 failures, 0 errors, 0 skipped, 34 s (lock wait 104 s on the second attempt) |
| `baseline-*`, `candidate-*` | `:protomolt-repo-container:nativeReplicaBenchmark`, journaled, gate first | 1 test, 0 failures in all eight runs |

Lock timeouts, which are not test outcomes: the first `admission-storage`
attempt (18:55:12Z to 19:05:12Z) and the first `native-replica-gate` attempt
(19:05:12Z to 19:15:12Z) never started Gradle because `flock -w 600` expired
while another agent's run held `/tmp/protomolt-repository-qualification.lock`;
`series.log` records them with the runner's `exit=98` sentinel (no exit-code
file) and the reruns that followed. The benchmark windows run the same
correctness gate first, but the `nativeReplicaTest` row above is the named
task's own run.

## Real, synthetic and injected

- Real: every SQL statement, lock wait, commit, rollback, trigger and constraint;
  PostgreSQL and RustFS in containers; the production jar in separate JVMs.
- Synthetic: the fixture publications and the 9,000-entry modes object used for
  the parse timing; the workload is the harness's closed loop, not production
  traffic.
- Injected: a `SQLException` on the rows statement and on commit, a barrier
  after the rows statement, deliberately damaged journal rows with their guards
  disabled (labeled as such in the test), expired leases through short lease
  durations and `pg_sleep`.

## Limitations

- CPU was not isolated; the throughput comparison is within noise and is not a
  latency claim.
- Warmup is eight iterations per client, so four cold JVMs are penalised in
  both series equally.
- The traced run adds proxy overhead; it is used for attribution only.
- The 16 MiB preparation decode bound was not timed under the lock.
- LocalStack was not used; nothing here is a LocalStack number.
