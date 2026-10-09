# First selected-read transport qualification

Parent: `8456589c9590f2839102cf2d035fce6ded71b23b`.

```sh
./gradlew :protomolt-repo-container:admissionStorageTest --console=plain
./gradlew :protomolt-repo-container:admissionRustFsTest --console=plain
```

Passed in 2m 12s. One JUnit test launches production-JAR PostgreSQL/LocalStack
host and restart/expiry probes, now including a real in-process selected-read
gRPC adapter. The harness requires `NATIVE_HISTORICAL_MATERIALIZATION_TRANSPORT_OK`.

Transport cases use the real provider-backed repository: successful archived
payload, unauthenticated request, wrong account, missing selection, wrong captured
revision, malformed canonical metadata, revocation after an initial view, and a
lower client fragment limit. Invalid output is injected only after a real read.
The server/channel terminate and the response byte budget returns to zero.

The same correctness harness also passes against pinned RustFS (rustfs.log/XML).
Both correctness tasks use the service graph and its matching inventory; the
separate native replica workloads retain their original lean host graph. These
fault/expiry tests are not benchmark samples or evidence of a speed improvement.

An initial attempt to combine separately resolved service and admission host JARs
failed the existing class-shadow check on Gson (`class-shadow-red.xml`). The fix
resolves one transport graph and inventories its admission dependency subtree.
No verifier rule was relaxed. Separate performance host configurations and the
published runtime dependency graph were not changed.

The response verifier checks exact request/retained evidence/descriptor bindings,
with explicit serialized-byte and descriptor limits plus borrowed-input ownership.
It does not claim source retention or compiler trust from metadata alone. Current
authorization and storage retention are still repository/host responsibilities.

This is not complete transport conformance: held-send cancellation races,
deadline/aggregate-size tests, broader invalid-output cases, managed-host wiring
and the remote client remain open. Nothing is mounted automatically or deployed.
