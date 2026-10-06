# JDBC publication diagnostic

The journaled RustFS benchmark passed twelve windows across one, two and four
worker JVMs with test-only JDBC tracing enabled. The build took 2 minutes 1 second;
the JUnit test took 118.886 seconds, with zero failures, errors or skips. Base
production source is `0473478ea`; this checkpoint changes test instrumentation only.

```
./gradlew :protomolt-repo-container:nativeReplicaBenchmark -PnativeBenchmarkJournaled=true -PnativeBenchmarkTrace=true -PnativeBenchmarkClients=4 -PnativeBenchmarkReadSlots=8 -PnativeBenchmarkReadHandles=32 -PnativeBenchmarkIterations=8 --console=plain
```

The same command with `nativeBenchmarkTrace=false` passed in 1 minute 55 seconds.
Its metrics contain no JDBC trace entries. Disabled tracing uses the original Tx
and does not create an extra persistence context. Both runs retain outcome,
history, replay and resource-release checks. The disabled run is a behavior check,
not an interleaved speed comparison.

## Findings

Across 144 successful foreground publications the trace recorded 2,592 commits,
21,312 query executions, 4,032 update executions and 432 batch executions. That is
18 commits, 148 queries, 28 updates and three batches per publication in this
fixture. Foreground commit calls total 6,114.06 ms, or 42.46 ms per publication.
Asynchronous provider and cleanup work appears separately as unscoped activity.

Selected-attempt renewal accounts for 432 foreground commits; owner renewal for
288. Coordinator inspection shows two paired owner/selected renewals per normal
publication, plus one separate selected renewal that verifies returned attempt
states. Combining each pair is a candidate for removing two commits per operation.
No such production change is included here, and no latency improvement is claimed.

## Trace limits and integrity

A second test-only EntityManagerFactory wraps the same real Hikari datasource,
using the production persistence unit, schema validation and PostgreSQL dialect.
It is constructed before timed windows and closed after runtime/reader drain. All
traffic components use its Tx; the original factory/pool owner remains unchanged.
JDBC calls delegate to PostgreSQL, including failures. No SQL text or parameter
values are added to the trace.

Commit/rollback and statement execution timing covers method duration, including
network and lock waits. These values overlap Hikari connection-use duration and
must not be added to it. Tracing adds proxy, stack-walk and accounting overhead.
Vendor unwraps and Statement.getConnection can expose underlying objects; coverage
is for calls through the wrapped standard interfaces, not every possible driver
escape. Positive per-operation commit/statement assertions and exact aggregate/
detail counter conservation passed. No trace failures were reported.

Four total clients, eight iterations per client/window, eight read slots and 32
read handles were used. Payload configuration zero selects the original small
fixture. Java 25.0.3, Linux amd64, PostgreSQL and RustFS ran on an unisolated host;
startup load averages were 6.61, 4.06 and 3.74. There were no container CPU/memory
limits. This does not establish saturation, maximum capacity, transport overhead
or horizontal scaling.

Raw traced directory `fcc2abe9-ddf6-4096-9405-71fcc0d8cbde` and disabled directory
`dc98aeae-d400-4a24-b8de-bfc401570fbf` are retained in their archives. `totals.csv`
pools only measure-phase traces by operation, metric and completion site; columns
are those keys, count, summed nanoseconds and failures. Sol reviewed instrumentation
and the proposed renewal composition constraints.
