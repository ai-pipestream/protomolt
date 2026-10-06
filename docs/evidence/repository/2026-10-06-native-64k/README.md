# Larger-payload native repository qualification

Twelve interleaved PostgreSQL 18/RustFS windows passed locally in 4m 48s. Each
window completed 1,024 measured operations from sixteen clients: 512 historical
reads, 384 accepted publications and 128 retained admission rejections. All 12,288
measured operations retain the historical document/revision checks, exact terminal
replay, provider/SQL failure checks and durable count assertions. No required
provider or SQL failure counter or SQL timeout was observed.

```sh
./gradlew :protomolt-repo-container:nativeReplicaBenchmark \
  -PnativeBenchmarkClients=16 -PnativeBenchmarkReadSlots=16 \
  -PnativeBenchmarkReadHandles=64 -PnativeBenchmarkPayloadBytes=65536 \
  -PnativeBenchmarkIterations=64 --console=plain
```

Payload values contain exactly 65,536 ASCII bytes from a deterministic varied
text template created once per worker before measurements. Protobuf framing is
additional; every larger retained read seed records its serialized document and
part size in worker configuration (the first is 65,634 bytes). There are sixteen
read seeds per window, one per client, regardless of replica count. Seed creation
and eight warmup iterations/client precede the measurement baseline. The original
zero-payload/32-iteration settings retain the older small shared-seed workload.
This run is not a controlled comparison with that different workload.

Inclusive throughput, combining repeated windows:

| Replicas | SQL connections per replica | Completed operations/second |
| --- | --- | --- |
| 1 | 8 | 80.745 |
| 2 | 4 | 75.654 |
| 2 | 8 | 95.979 |
| 4 | 2 | 67.938 |
| 4 | 8 | 87.879 |

Two workers with more total SQL connections improve inclusive throughput about
19 percent over one worker. Four workers do not improve on two. Splitting the
same eight connections among workers is slower. This is modest scale-out benefit
under additional resources, not linear scaling or a saturation ceiling. The first
one-worker sample records 59.3 aggregate seconds of SQL connection acquisition
across 14,593 acquisitions during its 13.85-second window; an eight-connection
worker in the two-worker profile records about 0.25 seconds across 7,301 acquisitions.
Concurrent duration sums can exceed wall time. These observations identify pool
waiting worth profiling; they do not justify removing SQL fences or prove the
complete latency cause.

Read slots (16) and outstanding handles (64) are fixed totals. Each worker still
gets a 512 MiB heap and 128 MB payload allowance, so aggregate memory grows with
replicas. The same database grows across windows. Host load is uncontrolled and
CPU/container resources are not isolated. Windows last roughly 11–15 seconds;
doubling the measured iterations is not a long soak. Inclusive throughput includes
command construction, replay and maintenance; per-operation p50/p95 in summary.csv
exclude command construction and terminal replay. Neither measures public RPC or
network-client performance.

Raw per-operation, provider/SQL, durable-count, RSS, lock and query statistics,
worker configuration and environment are in qualified-samples.tar.gz. windows.csv
preserves ordering and timing, and summary.csv uses the existing Java summary tool
from ../2026-10-05-snapshot-budget/NativeTrafficSummary.java.

Sol reviewed the harness. Two subsequent corrections do not change the measured
64 KiB path: the payload ceiling is now 786,432 bytes, below the fixture's 1,000,000
byte decoded limit, and default-size seeds also report numeric part sizes. Separate
measured-sources.sha256 and reviewed-sources.sha256 preserve that distinction.
Production limits and implementation are unchanged. Hosted CI, merge, deployment,
longer sustained load and full horizontal recovery remain separate requirements.

Post-review validation: `./gradlew :protomolt-repo-container:nativeReplicaTest
--console=plain` passed in 38 seconds. It compiled the reviewed probe sources and
ran the real PostgreSQL/RustFS native replica correctness matrix. It did not rerun
the twelve measured windows or exercise a maximum-sized payload.
