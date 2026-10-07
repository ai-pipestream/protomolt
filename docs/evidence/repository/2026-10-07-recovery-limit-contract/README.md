# Recovery-limit rejection contract

Base `914689653b2e68dafa6f2ff200461105326f2e78`, plus retained source fingerprints.

Added enum reason 4 without changing existing names, numbers, receipt fields,
codec versions, imports or Any URLs. ProtoMolt's runtime validator checks both
generated and DynamicMessage fixtures: the reason is valid with REJECTED, invalid
with ABORTED or an assessment binding. Codec fixtures round-trip the new reason
and retain identity checks. Existing precondition, admission and cancellation
fixtures remain green. The JSON Schema-facing projection contains the new enum;
cross-field CEL rules remain runtime annotations, not newly translated OpenAPI
constraints. No generator changes were made.

Both new fixtures failed against the old enum. After the addition, all 9 focused
tests passed with zero failures/errors/skips; final Gradle invocation took 3s.

```sh
./gradlew :protomolt-repo-proto:test --tests '*DocumentPublicationRejectionContractTest' \
  :protomolt-repo-spi:test --tests '*DocumentPublicationRejectionCodecTest' \
  --max-workers=2 --console=plain
buf lint --path repo/proto/src/main/proto/ai/protomolt/proto/repo/v1/document_publication_rejection.proto
scripts/check-proto-compatibility.sh 914689653b2e68dafa6f2ff200461105326f2e78
```

All commands exited 0. The compatibility script builds complete descriptor sets
with imports for current and baseline trees and applies FILE breaking checks.
Sol reviewed the contract and tests; its fence-comment clarification is included.

This is contract preparation only. Existing SQL reason checks still refuse 4;
the private decision sidecar, handler, migration and their acceptance tests remain
unfinished. Upgrade/qualify receipt readers before enabling the writer: older
generated validators can reject the unknown enum despite wire compatibility.
