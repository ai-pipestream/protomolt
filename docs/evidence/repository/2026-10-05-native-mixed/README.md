# Native mixed traffic on RustFS

Local production-JAR diagnostic run on 2026-10-05, based on branch commit
`3ad316341b011df7b4af5dd50a5516bc0f83f748` plus the source hashes recorded here.
The final run passed in 2m38s with one wrapper test, no errors, failures or skips.

```sh
./gradlew :protomolt-repo-container:nativeReplicaBenchmark --console=plain
```

The accepted output directory was
`repo/container/build/native-replica-benchmark/e334d79b-009d-4f7c-bd26-e711836eaa9f`.
`samples.tar.gz` retains its configuration, operation/metric samples, SQL query
texts/statistics, sampled locks/RSS, durable row deltas and child logs.
`result.xml.gz` retains the wrapper result. Earlier attempts are not included.

## Workload and accounting

The fixture first runs the independent-create and competing-update correctness
workload. It then interleaves twelve windows with one, two or four worker JVMs,
four total concurrent clients, tiny StringValue payloads, 32 warmup operations
and 128 measured operations per window. Each measured window must retain exactly
48 new documents, 48 revision commits and 16 admission rejections; its other 64
operations read an earlier revision. Every write verifies exact terminal replay
without another schema resolution. Reads verify original content and revision.

Fixed SQL capacity is eight connections divided across workers; the added-capacity
series gives each worker eight. Each child checks its actual Hikari maximum.
The diagnostic sampler has an additional dedicated connection. Identical
one-process configurations are combined in the summary (four windows); other
configurations have two windows each. Runtime maintenance runs throughout traffic
and finishes before the measurement-complete barrier. Workers park until SQL
capture finishes, excluding runtime shutdown from that capture.

Read, accepted-publication and rejected-publication latencies stay separate.
Nearest-rank p50/p95 use direct measured operations. Write latency excludes the
subsequent replay check; read latency includes result checks and closing the read.
Inclusive operations/second divides initial mixed operations by the parent window,
which also includes replay, checks, coordination and final maintenance. SQL/provider
metrics include replay and maintenance. SQL acquisitions per initial operation are
therefore not the cost of one publication RPC. Counter snapshots are not atomic
across distinct metrics. Sampled lock presence is not lock-wait duration.

## Observations and limits

One worker achieved 32.33 inclusive operations/second. Two workers achieved 30.76
with fixed total SQL capacity and 30.80 with added capacity. Four workers achieved
26.50 and 24.14 respectively. Read p95 ranged from 53.86 to 81.71 ms, accepted-write
p95 from 222.59 to 284.48 ms, and rejected-write p95 from 315.90 to 435.03 ms.
All recorded metric failure counts were zero. The harness requires positive
PUT, bounded GET, SQL acquisition and usage measurements and zero failures for
those counters, plus no SQL acquisition timeouts.

This run shows no speedup from replicas for this workload. Four clients do not
saturate an eight-connection budget, and these short windows do not establish
steady-state capacity. Database history grows between windows; baseline counts
are retained. Warmup is small and each topology receives different per-worker
warmup volume. No isolated-host, CPU-saturation, large-payload or public-transport
claim follows. Internal callers have trusted process authority.

Environment: krick/Linux amd64, Java 25, PostgreSQL 18.6 (`postgres:18-alpine`),
RustFS `rustfs/rustfs:1.0.0-beta.11-preview.1`, worker maximum heap 512 MiB.
RustFS local image ID was
`sha256:ea50257bc5e281e83170f49b84a109432d930f93bf5051c77d09105c5a62746b`;
this is a local image ID, not a registry manifest digest. There were no dedicated
container CPU/memory limits or host isolation. Environment and load snapshots are
in the archive. These are local results, not hosted CI or deployment evidence.
