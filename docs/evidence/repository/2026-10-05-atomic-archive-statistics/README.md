# Atomic archive counter updates

`ArchiveStatistics` replaces counter entity reads and dirty writes with one
additive PostgreSQL UPSERT for the archive and one for each affected rendition.
The operation still runs in the caller's publication or destructive transaction;
the shared archive row remains locked until commit. Pending domain changes are
explicitly flushed before counter DML, including version-revision triggers.
Current callers do not load managed counter entities in that EntityManager;
public counter reads use a fresh transaction scope. No provider or wire contract
changes are part of this checkpoint.

Rendition names are the sorted union of object-count and byte-delta keys. Missing
deltas default to zero. This fixes a previous silent omission when only the byte
map contained a name. PostgreSQL BIGINT arithmetic also rejects overflow instead
of Java arithmetic wrapping and committing incorrect counters.

## Correctness evidence

ArchiveStatisticsIT initially compiled after resolving the Tx Consumer/Function
lambda overload, then failed two real PostgreSQL cases: a dropped byte-only delta
and silently wrapped aggregate overflow. Both pass after the change. The final
five cases cover concurrent first/existing updates, multi-rendition increments and
valid decrements, new/existing byte-only rows, failure after writes, aggregate
overflow, and rendition overflow after the archive update. Checks confirm rollback
of earlier writes and unchanged totals after refusal. Publication-binding,
mutation-ledger and ArchiveServiceIT suites also passed with the new helper.

The existing schema does not enforce nonnegative counters. An invalid negative
delta against a missing row can still create a negative counter; this change does
not add a migration or qualify that invariant. Destructive commands also still
hold the statistics lock through subsequent target verification and receipt
creation. Neither limitation is resolved by replacing the arithmetic mechanism.

```sh
./gradlew :protomolt-repo-container:test --tests '*ArchiveStatisticsIT' --tests '*ArchiveMutationLedgerIT' --tests '*ArchivePublicationBindingIT' :protomolt-repo-service:test --tests '*ArchiveServiceIT' --console=plain
PROTOMOLT_REPLICA_BENCHMARK=true PROTOMOLT_REPLICA_WORKERS=32 ./gradlew :protomolt-repo-service:test --tests '*RepositoryScaleBenchmarkIT' --console=plain
```

The replica workload remains RustFS, 32 workers, 4 KiB payloads, one/two/four child
JVMs, and fixed/added SQL budgets. Raw requests, windows and lock observations are
preserved as gzip CSVs; image identity and mounts are in environment.txt. This is
an instrumented run on an unreserved host with fixed configuration order, not a
controlled performance comparison or complete horizontal-scaling qualification.

The run passed all six configurations and 9,984 recorded requests in 1m59s.
Measured mixed throughput was 233–284 requests/second, versus 225–276 in the
preceding RustFS run; individual configurations moved in both directions.
Sampled waiter observations fell from 1,976 to 1,430, all involving the statistics
statement. Poll counts and host conditions differ. No repeatable throughput
improvement is established, and this is not evidence that the remaining latency
is entirely counter-lock waiting. Longer steady-state warmup and service CPU/GC
measurements remain needed before choosing another architectural optimization.

`run/summary.csv` excludes sample -1, sums mixed operations/window durations, and
counts distinct `(window,sample,pid)` wait observations. `sql-snapshots.csv.gz`
retains query text and cumulative statement counters; `sql-captures.csv` includes
empty baselines. SQL execution times include overlapping calls and do not isolate
lock waiting. Source hashes, RustFS image identity and raw-run location are saved.
