# Native repository capacity qualification

Local Java 25, PostgreSQL 18 and RustFS qualification, not hosted CI or public RPC
performance. Sixteen concurrent clients completed twelve interleaved windows across
one, two and four JVMs. Each window completed 512 measured operations: 256 reads,
192 accepted publications and 64 contract rejections. Exact terminal replay,
historical bytes and retained rejection checks remain enabled. Required provider
and SQL failure counters and SQL timeouts were zero. The Gradle task passed in
3m 12s.

Run:

```sh
./gradlew :protomolt-repo-container:nativeReplicaBenchmark \
  -PnativeBenchmarkClients=16 -PnativeBenchmarkReadSlots=16 \
  -PnativeBenchmarkReadHandles=64 --console=plain
```

The explicit limits are totals divided by replica count: read slots 16/8/4 and
outstanding read handles 64/32/16. Omitted settings preserve eight reader slots
and 32 handles per worker. Production defaults were not changed. Each worker still
has a 128 MB payload allowance and 512 MiB heap; aggregate memory therefore grows
with replicas. SQL profiles use either eight connections total or eight per worker.

Inclusive completed throughput was 85.143 ops/s for one worker, 76.898 for two
workers sharing eight SQL connections, and 65.831 for four sharing eight. With
eight connections per worker, two reached 95.764 and four 83.010 ops/s. The
per-operation p50/p95 values are in `summary.csv`. This is a modest two-worker
benefit with more SQL connections, not linear scaling or a saturation ceiling.
Inclusive timing contains replay/checks and maintenance; operation latency does
not include terminal replay. Small StringValue payloads, warmup, a growing shared
database and a non-isolated host limit generalization. These runs are not a
controlled comparison against earlier four/eight-client measurements.

Two preceding runs failed and are preserved in `refused-samples.tar.gz`:

- `47769da4-a476-4904-b76e-7fe9ca3d43ee`: sixteen clients with eight reader slots
  per worker refused concurrent read capacity during the first warmup.
- `8c9fe86c-b2b8-47f1-9455-77758642bee4`: sixteen total reader slots, original
  32 handles per worker; first measured window refused outstanding read capacity.
  Closed handles remain counted until durable SQL release. Concurrent capture and
  release can fill this bound; this observation does not prove a leak.

Neither refusal is a completed throughput sample. The second failure also exposed
a sampler defect: a zombie process has no VmRSS, masking the child's actual error.
The sampler now records exited RSS as missing, not zero; malformed live status
still fails. Two focused parser tests passed before this qualified run. No timed
operation retry or production capacity increase was added to hide refusals.

`qualified-samples.tar.gz` contains all twelve windows, per-worker operation and
provider/SQL metrics, worker configurations, SQL text/statistics, locks and RSS.
`sources.sha256` binds the changed harness sources. Sol reviewed the diagnostic
fix and fixed-total capacity configuration with no blocker. Hosted CI, merge,
deployment, larger payloads and sustained load remain separate qualification.
