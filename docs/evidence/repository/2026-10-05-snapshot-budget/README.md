# Exact retained slot snapshot reservations

An eight-client mixed RustFS run exposed payload-budget exhaustion at retained
assessment slot verification. The failing run completed three measured windows
before a later one-worker window failed under the unchanged 128,000,000-byte shared
budget. `benchmark-red.xml` and `benchmark-red-samples.tar.gz` preserve the result.
The workload uses small typed protobuf documents, real PostgreSQL and RustFS,
production-JAR JVMs, and current authorization/assessment/replay checks.

`DocumentAssessmentRetainedSlots` reserved twice the 4 MiB maximum for every
snapshot read. It now encodes the exact expected immutable snapshot first, reserves
twice that actual size for JDBC payload/copy ownership, and projects stored bytea
only when its length equals that expected size. Byte equality, SHA, identity,
association and current authorization checks remain unchanged. The caller still
holds the retained owner lock. This does not change the outer manifest reservation
or bound PostgreSQL server-side detoast memory.

The new 20 KB PostgreSQL regression failed with capacity exhaustion before the fix
(`budget-red.xml`). The focused run passed in 18s after the fix:

```sh
./gradlew :protomolt-repo-container:test \
  --tests '*DocumentAssessmentSlotsIT' \
  --tests '*DocumentAssessmentSlotSnapshotTest' --console=plain
```

The corruption fixture deliberately bypasses the SQL update guard inside a
transaction, maintains a valid SQL checksum, and verifies that a 1-byte snapshot,
a 200 KB snapshot, and same-length wrong bytes all fail integrity checks under the
20 KB budget. It restores the guard in that transaction; these are not normal
repository mutations. The attached green XMLs include existing association,
authorization, encoding and cancellation checks. Sol reviewed the change and tests.

The benchmark can now select four or eight total clients with
`-PnativeBenchmarkClients=8`; four remains the default. Per-client operation counts
and durable row expectations scale together. An initial harness attempt observed
96 documents, 96 revisions and 32 rejections but still asserted the old four-client
counts; that harness assertion was corrected before the retained-capacity failure
above. No workload failure is converted to success or retried silently.

## Eight-client result

The complete repeat passed in 2m40s with the same 128,000,000-byte payload budget,
512 MiB worker heap limit, SQL pool configurations and provider settings. All twelve
interleaved windows completed, for 3,072 initial measured operations plus their
replay checks. Each window verified 96 new documents/revisions, 32 retained
rejections and 128 historical reads. Required provider and SQL metric failures
and SQL acquisition timeouts remained zero.

```sh
./gradlew :protomolt-repo-container:nativeReplicaBenchmark \
  -PnativeBenchmarkClients=8 --console=plain
```

Output: `repo/container/build/native-replica-benchmark/c0f317b7-c9a3-4cc7-8621-73e3b20960c5`.
`benchmark-green-samples.tar.gz` retains every child log, operation/metric sample,
SQL query/statistic, sampled lock/RSS, durable delta and configuration. The wrapper
result is `benchmark-green.xml`; the source hashes and Java summary tool are saved.

One process averaged 61.72 inclusive operations/second. Two averaged 53.15 with
fixed total SQL capacity and 53.30 with eight connections per worker. Four averaged
47.16 and 47.08 respectively. Read p95 ranged from 52.78 to 71.23 ms, accepted
publication p95 from 223.79 to 298.32 ms, and rejected publication p95 from 315.41
to 427.74 ms. `summary.csv` preserves per-configuration distributions; percentiles
use nearest rank over actual operation samples. Inclusive throughput includes
replay, correctness checks, final maintenance and coordination, while individual
publication latency excludes the subsequent terminal replay check.

This supports the exact-size fix under the exercised workload; it does not prove
that every capacity bottleneck is resolved. The separate maximum-sized manifest
reservation remains. More replicas did not increase throughput here. These short,
closed-loop windows with tiny payloads, a growing database and no host isolation
are not saturation, steady-state, public-RPC or scale-out qualification. The earlier
four-client run has different total work and is not a controlled before/after
performance comparison for this production fix.

Environment: Linux amd64, Java 25.0.3, PostgreSQL 18.6, RustFS
`rustfs/rustfs:1.0.0-beta.11-preview.1`. RustFS local image ID:
`sha256:ea50257bc5e281e83170f49b84a109432d930f93bf5051c77d09105c5a62746b`
(not a registry manifest digest). Starting host load average was 12.50/10.51/8.83;
no apps or containers were stopped for isolation. Source base was
`5b4ef11ae0c8482ebdf87f8dc8a0c88b2e55932a` plus the recorded change hashes.

The production-JAR storage gate also passed in 2m34s after this change:

```sh
./gradlew :protomolt-repo-container:admissionStorageTest --console=plain
```

`provider-green.xml` records the separate real-provider correctness and recovery
run. This is local verification, not hosted CI, merge or deployment evidence.
