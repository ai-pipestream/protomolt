# Managed archive reads with shared reader locks

Measured 2026-10-03 on `krick`, using the unchanged `ArchiveReadBenchmarkIT`
harness from `d672b090` with V29, shared Java reader locks and explicit
READ COMMITTED enforcement in the working tree. The baseline is retained in
[`2026-10-03-archive-read`](../2026-10-03-archive-read/README.md).

The command and workload are unchanged:

```sh
PROTOMOLT_ARCHIVE_READ_BENCHMARK=true ./gradlew :protomolt-repo-service:test --tests '*ArchiveReadBenchmarkIT'
```

The benchmark passed, with one test and zero failures/errors/skips. The retained
files contain 192 batches and 5,376 operations, including warmups; measured samples
number 144 batches and 4,032 operations. Every read used the actual provider,
bounded GET and checksum/length validation, and passed full expected-byte equality.
Each batch confirmed exact GET/byte counts and zero remaining reader pins.

Both paths still execute the same number of client SQL statements as before:
seven statements and two transactions per pinned read; one statement and no
explicit Hibernate transaction in the unsafe unpinned diagnostic. Counts exclude
SQL inside PostgreSQL triggers. This change reduces reader lock exclusivity, not
round trips, validation, durability, or provider I/O.

## Observations

At sixteen readers sharing one 4 KiB object, pinned p50/p95 changed from
27.98/34.24 ms to 14.86/27.46 ms. Measured batch throughput changed from
546 to 897 reads/second. With a shared 256 KiB object, p50/p95 changed from
33.62/58.39 ms to 21.57/25.91 ms; throughput changed from 419 to 733 reads/second.

In the new run, same-object pinned provider medians were 13.28 ms for 4 KiB and
19.06 ms for 256 KiB. Provider time rose relative to the old same-object run as
reads reached storage concurrently. These independent medians must not be
subtracted to estimate one request's phase costs.

Not every scenario improved: the disjoint single-reader 4 KiB pinned median rose
from 4.31 to 5.76 ms, and its p95 rose from 7.83 to 9.01 ms. The benchmark does not
establish a universal speedup or prove how much of a difference comes from locks.

The shared host's one-minute load average was 31.28–34.52, compared with
12.84–14.41 in the earlier run, on 32 logical processors. Those are moving load
averages, not CPU utilization. Runs were sequential, not interleaved revisions on
a quiet host. Scenario order is fixed and only path order alternates. JIT,
provider and background-load drift remain possible. These results justify more
qualification, not a production SLA or release-wide performance claim.

## Safety evidence

Before the change, both variants of
`independentReadersOfOneObjectDoNotSerializeTheirAdmissionOrRelease` failed with
TimeoutException while another reader's admission transaction remained open.
They pass with V29: another pin can acquire and release on the same object without
waiting for the first transaction to commit. This proves removal of that specific
serialization independently of the benchmark timings.

The final affected suite passed 88 container and 123 service tests. Its one
additional skipped case was this opt-in benchmark, subsequently run successfully.
Coverage includes logical mutation/read races, release/cleanup commit and rollback
with observed PostgreSQL lock waits, delayed real provider reads after cancellation,
failed pin release, populated migration preservation, and explicit rejection of
REPEATABLE READ at pin and reclamation boundaries. Sol reviewed the implementation
and these safety boundaries.

READ COMMITTED is now explicit in the Java connection pool and enforced in SQL
retention guards. Reader native/common triggers use shared locks consistently;
other reference owners, retirement and cleanup retain exclusive source-before-
retention locking. Old readers must be drained before migration. This does not
implement crash-pin recovery or remove the two transactions required by each read.
