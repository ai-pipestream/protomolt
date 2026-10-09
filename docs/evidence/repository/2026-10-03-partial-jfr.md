# Partial-save JVM diagnostic

Source: `35e46dfb` plus the uncommitted V25 reference-table experiment. Publication
bookkeeping therefore differs from the earlier latency baseline. No implementation
was enabled from this diagnostic.

The temporary Gradle init script sets `maxParallelForks=1` after project evaluation,
checks it before execution, and adds
`-XX:StartFlightRecording=filename=/tmp/protomolt-partial-qualified-profile.jfr,settings=profile,dumponexit=true`.
Command: `PROTOMOLT_PARTIAL_BENCHMARK=true ./gradlew --init-script /tmp/protomolt-profile.init.gradle :protomolt-repo-service:test --tests '*DocumentPartialBenchmarkIT'`.
Application worker concurrency stays enabled. The log confirms one test fork and
success in 55 seconds. Earlier multi-worker recordings are excluded.

The accepted recording is `/tmp/protomolt-partial-qualified-profile.jfr`, with log
`/tmp/protomolt-partial-qualified-profile.log`. These temporary artifacts are not
published or guaranteed durable. The recording has one 53-second chunk starting
2026-10-03 19:04:44 UTC. It spans setup, seeding, warmups, saves and verification.
There are no save-phase markers. Sampling, event thresholds, host load and profiler
overhead limit interpretation; this is not production latency evidence.

## Observations

- 1,188 execution samples: MessageDigest delegate update 5.13%, DigestBase update
  2.02%, SDK checksum update 1.60%. These are CPU samples, not latency proportions.
- SocketRead grouping by stack/port: HTTP/S3 on port 40367 records 8,450 events
  and 89.019 cumulative thread-seconds; PostgreSQL on port 40368 records 2,644
  and 5.918 thread-seconds. Docker Unix sockets add 5.998 thread-seconds.
  Concurrent durations overlap. Socket events are not repository GET counts.
- 371 GC pause events total 0.676 seconds, with maximum 9.42 ms, across 296 GC
  cycles. Pause time cannot account for most of this test's duration.
- Allocation sample weights attribute about 6.15 GB to byte arrays across the
  test. This is estimated allocation volume, not live heap or per-save memory.
- The Java monitor-contention view has no events. It does not exclude database
  locks, connection waits or other contention outside the recorded event scope.

## Interpretation

Reduce repeated byte transfer and processing first. This profile does not prove
SQL contention, incorrect thread scheduling or a particular redesign speedup.
Quantifying per-part SQL costs needs statement timing and database wait data,
correlated with save phases. Representative-provider qualification still requires
latency, throughput and resource thresholds.

Sol reviewed the recording and conclusions. Analysis used `jfr summary`,
`jfr view hot-methods`, `jfr view contention-by-site`, and JSON exports of
SocketRead, GCPhasePause and ObjectAllocationSample. Preserve scope and sampling
limits in subsequent comparisons.
