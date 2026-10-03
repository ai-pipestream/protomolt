# Local reader quiescence and recovery evidence

Measured on `krick` on 2026-10-03, baseline `2db38f56` plus V32 local lifecycle
accounting, attestation, bounded recovery and host wiring. This evidence covers
local shutdown, not remote crash termination or the full repository redesign.

The affected regression command passed 65 container and 113 service tests,
with one opt-in benchmark skipped:

```sh
./gradlew :protomolt-repo-container:test --tests '*Archive*' --tests '*Repository*' :protomolt-repo-service:test --tests '*Archive*' --tests '*LifecycleShutdownTest' --tests '*UploadHttpShutdownTest'
```

The first run exposed two migration fixture errors: reusing a Flyway configuration
preserved the old target instead of applying the later migration. Fresh
configurations corrected those fixtures; the final container rerun passed all
65 cases, and the unchanged service suite remained green. No assertion was
weakened to accept a missing migration.

Seven new real PostgreSQL tests cover ledger-wide pending/live lifetimes, empty
and SQL-failed admission, failed pin release, partial batch recovery failure and
retry, ordinary release racing recovery, immutable evidence, repeated operations,
protected UNKNOWN/FENCED owners and populated V31 state migration. Existing SQL
race and retention tests remain green. Five host shutdown cases use actual S3
adapter reads with controlled delay/failure injection; they include failed fence
and quiescence SQL, timeout/interruption, and recovery of a leftover pin from a
fresh database connection after the original host closes.

The unchanged real-provider benchmark also passed:

```sh
PROTOMOLT_ARCHIVE_READ_BENCHMARK=true ./gradlew :protomolt-repo-service:test --tests '*ArchiveReadBenchmarkIT'
```

Raw files retain all 5,376 operations and 192 batches including warmups; 4,032
operations and 144 batches are measured samples. The 24-connection-pool harness
checks bounded reads, byte equality, exact GET/byte counts and zero pins after
completed batches. Each pinned read still used two Hibernate-visible client SQL
statements and two transactions. This excludes internal SQL and commit traffic.

With 16 readers of one 4 KiB object, p50/p95 were 13.66/25.65 ms and throughput
1,002 reads/s. At 256 KiB, p50/p95 were 19.29/26.04 ms and throughput 776 reads/s.
The preceding V31 diagnostic measured 15.12/31.16 ms and 848 reads/s, and
22.30/28.32 ms and 708 reads/s. Do not interpret the difference as a speedup:
revisions were not interleaved on a quiet host, and order/load were uncontrolled.
One-minute host load ranged from 26.72 to 28.72 on 32 logical processors; it is
not instantaneous CPU utilization. No background applications were stopped.

Sol reviewed the lifecycle and recovery implementation with no safety blocker
within the trusted `Pin.close()` boundary: callers close only after actual
provider completion. Latency qualification, recovery query cost/fairness at
large scale, and remote termination authority remain outstanding. There is no
TTL-based pin release, no automatic promotion of UNKNOWN/FENCED to QUIESCED,
and no wire-contract change.
