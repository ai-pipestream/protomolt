# Child-process repository timing

This diagnostic extends the [first replica run](../2026-10-05-replica-scale/README.md)
with measurements inside the real service processes. The workload, 1/2/4 process
matrix, SQL pool maxima, payloads and correctness assertions are unchanged.
The baseline is `ddd36cdb`; `sources.sha256` identifies the changed sources used.

```sh
PROTOMOLT_REPLICA_BENCHMARK=true ./gradlew :protomolt-repo-service:test --tests '*RepositoryScaleBenchmarkIT' --console=plain
```

`ReplicaMetrics` wraps the installed provider factories without changing backend
identity, options, capabilities, provisioning, reclamation or ownership of client
resources. Provider calls use the real Java store and preserve thrown exceptions.
Hikari's tracker attaches after service construction and before traffic starts.
No SQL driver proxy or alternate repository implementation is involved. The only
production seam added is a package-private accessor for the existing datasource;
Hikari instrumentation is a test dependency.

The parent requests identified snapshots after seeding and after each phase. The
child consumes each request before atomically publishing its matching response.
Missing, stale or late replies fail the test. Counter rows must be unique,
nonnegative and monotonic; every mixed window must increase put, bounded-read,
connection-acquisition and connection-usage counts on every child. Pool gauges
can decrease. A nonzero acquisition-timeout count fails this diagnostic.

`snapshots.csv` preserves all child snapshot rows and request IDs. `mixed-metrics.csv`
subtracts the preceding contended snapshot (or the warmup predecessor for sample
zero) from each measured mixed snapshot, then sums across children and samples
0–2. Counter snapshots are approximate whole-child observations, not atomic
window boundaries. Background lifecycle work and time between phases are included.

Metric meanings:

- `provider_s3_put` and `provider_s3_getBounded`: completed Java API calls and wall
  time, including client overhead/retries. Namespace and reclamation calls use
  separate interfaces and are not timed here. These are not HTTP-only durations.
- `sql_acquire`: Hikari connection acquisition count/time, including ordinary
  borrow overhead. This is not pure queue wait or SQL lock-wait duration.
- `sql_usage`: connection checkout count/time, reported by Hikari in milliseconds
  and converted to nanoseconds. This includes work while holding the connection;
  it is not a measurement of database execution time.
- `pool_*`: instantaneous runtime pool gauges at the snapshot, not maxima over
  the traffic window. Flyway startup connections are outside this pool.

Means are aggregate elapsed time divided by completed-call count. Concurrent
elapsed times overlap: do not add them to request latency or treat their sum as
a wall-clock percentage. Averages do not establish acquisition tail latency.
The client summary uses attempts divided by summed window wall time and
nearest-rank latency quantiles, excluding warmup sample -1.

The final run passed one test with zero failures, errors or skips: 2,496 recorded
requests, 48 windows and 126 child snapshots. Each configuration's measured mixed
intervals recorded exactly 72 puts and 144 bounded reads, with no provider errors
or pool acquisition timeouts. Across configurations, mean acquisition time ranged
from 0.004 to 0.416 ms, mean bounded-read time from 5.64 to 7.28 ms, and mean put
time from 6.08 to 9.81 ms. Those averages do not show persistent pool starvation
at this offered load, but do not rule out long individual waits. SQL checkout
counts can differ from acquisitions when background work spans a snapshot boundary.

The run is diagnostic: LocalStack, fixed topology order, growing retained history,
an unreserved host and eight load workers. CPU saturation, provider connection
counts and SQL lock-wait durations remain unmeasured. It does not qualify typed
publication scale-out or establish production latency. Increase offered load,
measure those remaining components and repeat interleaved runs on representative
storage before attributing a scaling limit to the service architecture.
