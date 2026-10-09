# Managed archive read coordination baseline

Measured 2026-10-03 on `krick`, against production code at
`c23dd54e0b7b4a1d9dbe0b83c166388cdff6f0e2` plus the new opt-in
`ArchiveReadBenchmarkIT` harness. This is a shared-host diagnostic, not a
production latency target or a release performance qualification.

Run:

```sh
PROTOMOLT_ARCHIVE_READ_BENCHMARK=true ./gradlew :protomolt-repo-service:test --tests '*ArchiveReadBenchmarkIT'
```

The run passed: one test, zero failures, errors or skips. The retained files
contain all 192 batches and 5,376 operations, including warmups. Excluding the
negative sample numbers leaves 144 batches and 4,032 measured operations.
`environment.txt` records Java, container versions, processor count and pool size.
The host had 32 logical processors. Its one-minute system load average ranged
from 12.84 to 14.41 during measured batches; this is not batch CPU utilization.
No applications or containers were stopped to produce the measurement.

## Workload and interpretation

Synthetic deterministic bytes are stored through the actual managed archive
writer in real PostgreSQL 18 and LocalStack 3.8 containers. Payloads are 4 KiB
and 256 KiB. Each scenario runs one, four or sixteen virtual-thread readers,
either sharing one object or reading distinct objects. The database pool is 24,
above the maximum reader concurrency, rather than the default pool of 10.
No explicit container CPU or memory limits are applied by the harness.

Each path receives two warmup batches and six measured batches, with four reads
per worker in a batch. Workers start behind a barrier. Pinned and unpinned order
alternates between batches. Scenario order is fixed, so comparisons between
sharing/concurrency scenarios can still include JIT, provider and host-load drift.

The unsafe unpinned baseline recreates the earlier metadata lookup followed by
a bounded provider read and size/SHA-256 verification. It is test-only and cannot
replace reader pins: deletion could otherwise reclaim bytes during its read.
Both paths use the exact stored coordinates and provider version. Every result
is compared with the complete expected payload. Each batch asserts one GET per
operation, exact read-byte counts and no remaining read pins.

`elapsed_nanos` includes metadata lookup, provider read and integrity verification;
the pinned path also includes acquisition and release. `provider_nanos` is a
nested interval, not an additional latency. Full-payload test equality is outside
the per-operation timer but inside batch wall time. Throughput should be computed
as total measured operations divided by the sum of measured batch wall times.

Hibernate counters are batch-level, collected after all workers finish. They count
Hibernate-visible JDBC statements and transactions, not SQL executed inside
PostgreSQL triggers. Statistics and measurement overhead are enabled for both
paths. The run does not sample database lock waits or isolate network round trips.

## Findings

Across single-reader scenarios, pinned medians were 3.31–4.31 ms; the unsafe
baseline medians were 1.69–2.60 ms. The pinned path consistently issued seven
client SQL statements and two transactions per read. The baseline issued one
statement and no explicit Hibernate transaction.

For sixteen readers of the same 4 KiB object, pinned p50/p95 were
27.98/34.24 ms and batch throughput was 546 reads/second. Provider p50 was
2.61 ms. For sixteen readers of distinct 4 KiB objects, pinned p50/p95 were
18.46/38.68 ms and throughput was 685 reads/second; provider p50 was 15.15 ms.
These independently computed quantiles must not be subtracted as a decomposition
of one representative request.

For sixteen readers of the same 256 KiB object, pinned p50/p95 were
33.62/58.39 ms and throughput was 419 reads/second. Provider p50 was 4.96 ms.
With distinct objects, pinned p50/p95 were 25.61/39.59 ms and throughput was
578 reads/second; provider p50 was 21.90 ms.

The data identifies read coordination as worth investigating, especially for
shared objects. It does not establish that a particular SQL lock is the dominant
wait. Code inspection confirms that both pin acquisition and release currently
request exclusive source-owner and retention row locks, with multiple client
statements while those locks are held.

The next design review should examine shared locks for read-only admission and
release, retaining exclusive locks for retirement and reclamation. Mirror triggers
must follow the same lock mode and order; an upgrade from shared to exclusive
would defeat the change and could deadlock. A separate option is reducing client
round trips without weakening atomic admission. Rerun the same diagnostic and
all lifetime/race/failure tests after any change. No optimization is qualified by
this baseline alone; crash-pin recovery remains outstanding as well.
