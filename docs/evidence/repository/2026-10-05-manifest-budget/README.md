# Exact retained assessment manifest reservations

Retained assessment reconciliation formerly reserved three copies of the 4 MiB
manifest maximum before any database lookup. The new production-JAR regression
acknowledges a real retained stage using a 256 KiB budget; it failed at that old
reservation (`red.xml`). The fixture uses PostgreSQL, LocalStack and observed
runtime/schema artifacts, not a synthetic successful storage backend.

The handler now locks and checks the existing owner metadata, including actual
manifest byte length, before reserving three times that length. A second query
returns bytea only at the checked length. The owner lock spans both reads and
normal SQL mutation cannot replace the sealed manifest. The reservation covers
JDBC payload, returned array and immutable copy through retained evidence checks.
Snapshot and decoder reservations remain separate. Absent or wrong-identity rows
require no manifest byte reservation; the wrong-digest fixture uses a one-byte
budget and must still report the identity conflict.

Owner fencing, current authorization, original command/assessment identity,
manifest digest and contents, retained associations, expiry, cancellation and
reader-session registration remain enforced. Capacity refusal remains immediate;
no waiting for memory is introduced while holding SQL locks. This does not bound
PostgreSQL server-side detoast memory. It adds one SQL read, so it is not a claim of
lower latency. There are no protobuf or migration changes.

```sh
./gradlew :protomolt-repo-container:admissionStorageTest --console=plain
```

The real-provider production-JAR gate passed in 2m35s after the change, including
the 256 KiB original-stage acknowledgement and one-byte wrong-digest refusal.
`provider-green.xml` records the full gate, including its existing publication,
rejection, recovery, historical access and cancellation paths. Sol reviewed the
implementation and reservation lifetime. This is local validation, not hosted CI,
merge, deployment or automatic recovery activation.

## Eight-client RustFS observation

The unchanged twelve-window workload passed in 2m42s, including all 3,072 initial
measured operations and exact terminal replays. Every window verified 96 new
documents/revisions, 32 retained rejections and 128 historical reads. Required
provider/SQL metric failures and acquisition timeouts were zero. The payload
budget stayed at 128,000,000 bytes and each worker's heap limit at 512 MiB.

```sh
./gradlew :protomolt-repo-container:nativeReplicaBenchmark \
  -PnativeBenchmarkClients=8 --console=plain
```

Raw output was
`repo/container/build/native-replica-benchmark/efccda53-c531-41f8-be40-ebedd4500cb8`.
`samples.tar.gz` preserves all operation timings, provider/SQL metrics, SQL text,
lock/RSS samples, configuration, durable deltas and worker logs. `summary.csv` was
produced by the [existing Java summarizer](../2026-10-05-snapshot-budget/NativeTrafficSummary.java).
`benchmark-green.xml` records the wrapper result; `sources.sha256` records the
changed production and regression sources on base `c67cd7a7`.

Inclusive rates were 59.57 operations/second with one worker; 54.13 and 53.01 with
two; and 46.97 and 46.73 with four. For multiworker pairs, the first rate has eight
SQL connections total and the second eight per worker. Read p95 ranged from 58.32
to 75.20 ms, accepted publication p95 from 222.63 to 286.75 ms, and retained rejection
p95 from 339.90 to 424.90 ms. The same operation accounting and nearest-rank
percentiles as the previous run apply: inclusive time contains replay/checks and
maintenance, but individual publication latency excludes the replay afterward.

These short, non-isolated, closed-loop windows do not establish steady-state
capacity or saturation. They still show no replica speedup. The previous run was
not interleaved with this code version, so a causal latency/throughput delta cannot
be inferred from their difference. This observation qualifies correctness at the
exercised load and records the cost with the extra query present; it is not a
latency improvement claim. Public transport and larger payloads remain unqualified.
