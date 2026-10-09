# Managed archive reads with one call per phase

Measured 2026-10-03 on `krick`, using the unchanged `ArchiveReadBenchmarkIT` from
`d672b090`, production baseline `36728406`, and the V30 SQL functions/Java ledger
changes in the working tree. Compare the preceding
[shared-lock diagnostic](../2026-10-03-archive-read-shared-locks/README.md).

The benchmark command, fixtures, containers, 24-connection pool, warmups and
concurrency matrix are unchanged:

```sh
PROTOMOLT_ARCHIVE_READ_BENCHMARK=true ./gradlew :protomolt-repo-service:test --tests '*ArchiveReadBenchmarkIT'
```

The benchmark passed: one test, zero failures/errors/skips. All 192 batches and
5,376 operations are retained, including warmups. Excluding negative samples
leaves 144 batches and 4,032 measured operations. Every read used actual provider
bytes, bounded reads, integrity verification and full expected-payload equality;
every batch checked exact GET/byte counts and zero remaining reader pins.

## Verified statement reduction

Every pinned scenario measured two Hibernate-visible client statements and two
transactions per read, compared with seven statements/two transactions before
V30. The unsafe unpinned diagnostic still used one statement and no explicit
Hibernate transaction. Internal function and trigger SQL is excluded from these
counts. Transaction commit traffic is not counted as a prepared statement.

`ArchiveReadCallIT.ledgerUsesOneStatementPerAdmissionAndRelease` independently
makes this a regression gate. It also checks that closing an already-closed
handle performs no further statement. This verifies fewer client statements;
it does not establish that fewer internal checks or database operations execute.

## Latency remains unqualified

This run does not show a consistent latency improvement. For sixteen readers of
one 4 KiB object, pinned p50/p95 were 18.03/33.40 ms, compared with 14.86/27.46 ms
in the prior run. Throughput was 738 reads/second, previously 897. For one shared
256 KiB object with sixteen readers, p50/p95 were 23.50/31.80 ms and throughput
657 reads/second, compared with 21.57/25.91 ms and 733 reads/second before V30.

Some cases were faster, but the first single-reader disjoint 4 KiB scenario was
much slower in both paths: pinned p50 was 17.96 ms and unsafe unpinned p50 was
14.06 ms, versus 5.76 and 2.20 ms in the prior run. The harness cannot isolate
whether this arose from host scheduling, provider startup, JIT or other variation.

The shared host's one-minute load average ranged from 30.78 to 34.03 on 32 logical
processors. It is not instantaneous CPU utilization. Revisions were not interleaved
on a quiet host, and scenario order remains fixed. Raw results must not be used
to claim a production speedup. A quieter paired run remains necessary for latency
qualification; no background apps or containers were stopped for these runs.

## Safety acceptance

The affected suite passed 92 container and 123 service tests. Its one skipped
case was this opt-in benchmark, subsequently run successfully. Coverage retains
reader-reader concurrency, read/mutation and release/cleanup races with actual
lock waits, cancellation while a real provider call continues, failed release,
populated migration protection and READ COMMITTED enforcement. New direct-call
cases cover returned original identity, admission rollback, unavailable/invalid
identities, wrong-owner/object release and idempotent release.

Sol reviewed V30 and the Java changes with no blocker. SQL functions are VOLATILE
PL/pgSQL with separate statements after lock acquisition. The exact readable
snapshot is resolved before native pin insertion; existing native/common guards
remain in force. Functions run with caller privileges. This does not change wire
contracts, provider verification or the rule that I/O is outside SQL transactions.
Crash-pin recovery is still outstanding.
