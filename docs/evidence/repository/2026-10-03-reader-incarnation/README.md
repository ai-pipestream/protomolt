# Reader incarnation fencing diagnostic

Measured on `krick` on 2026-10-03, with baseline `2fac5b37` plus the V31
incarnation registry, ledger registration/fence and reader lifecycle changes.
The unchanged `ArchiveReadBenchmarkIT` uses PostgreSQL 18 and LocalStack 3.8,
a 24-connection pool, real stored payloads and full returned-byte comparison.

```sh
PROTOMOLT_ARCHIVE_READ_BENCHMARK=true ./gradlew :protomolt-repo-service:test --tests '*ArchiveReadBenchmarkIT'
```

One benchmark passed with no failures or skips. Raw files retain 5,376 operations
and 192 batches including warmups; 4,032 operations across 144 batches are
measured samples. Every batch asserts GET counts, bytes and zero remaining pins.
Every pinned scenario retains two client SQL statements and two transactions per
read. Registration is startup work outside measured reads. SQL inside functions
and triggers, and transaction commit traffic, are not counted as statements.

For 16 readers of the same 4 KiB object, p50/p95 were 15.12/31.16 ms and throughput
848 reads/second. At 256 KiB, p50/p95 were 22.30/28.32 ms and throughput 708 reads
per second. The preceding V30 diagnostic measured 18.03/33.40 ms and 738 reads/s,
and 23.50/31.80 ms and 657 reads/s respectively. These differences do not establish
a speedup: host load and revision order were not controlled or interleaved.

The one-minute host load ranged from 20.05 to 21.18 on 32 logical processors,
compared with 30.78 to 34.03 for the V30 run. Load average is not instantaneous
CPU utilization. This diagnostic checks actual behavior and operation counts;
it does not qualify production latency or rule out incarnation-row contention.
No applications or containers were stopped to improve the result. Quiet paired
measurements remain required before a performance claim.

The affected regression run passed 58 container and 112 service tests; its one
skipped case was this benchmark, subsequently run successfully. Tests cover
populated legacy-pin migration, admission/fencing lock races, permanent state,
ordinary release after fencing, SQL fence failure, blocked fence versus local
drain, shutdown interruption, actual delayed provider work and pin release
failure. The final incarnation race rerun additionally checks another incarnation
can read the same object while a fence holds its exclusive incarnation lock.

Sol reviewed the migration, lock ordering and lifecycle boundary. V31 implements
registration and admission fencing only; attested quiescence and recovery of
pins left by crashed processes remain unimplemented. No wire contracts change.
