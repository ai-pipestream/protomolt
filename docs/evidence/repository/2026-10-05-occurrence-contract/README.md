# Selected occurrence wire contract

Parent: `c9fde2872c36a8db01bdad60b76076f4253b2897`.
Sol reviewed the contract and complete-path ownership changes. This is an
unmounted contract; no handler, remote client or public endpoint is enabled.

```sh
./gradlew :protomolt-repo-proto:test \
  --tests '*HistoricalOccurrenceContractTest' --tests '*DocumentHistoryContractTest' \
  :protomolt-repo-admission:test --tests '*DocumentSchemaMaterializationTest' \
  :protomolt-repo-engine:compileJava --console=plain
buf lint --path repo/proto/src/main/proto/ai/protomolt/proto/repo/v1/document_history_materialization_service.proto
scripts/check-proto-compatibility.sh c9fde287
```

The focused gate passes 22 cases: six selected-read contract, six existing history
contract and ten owned materialization tests. Generated and DynamicMessage fixtures
use ProtoMolt's runtime validator. Fixtures include missing fields, invalid revision,
unsigned overflow/limits, artifact bounds, unsupported metadata version and a nested
path whose final boundary must match the delivered definition. Synthetic malformed
artifact bytes intentionally satisfy shape rules; byte integrity and retained
decoding remain handler obligations.

Complete descriptor compilation, targeted STANDARD lint and FILE compatibility
pass against the parent. Existing protobuf messages/tags/import paths are unchanged.
The JSON Schema fixture confirms CEL extensions and runtime-only byte annotations;
it does not claim standard JSON Schema can enforce hashes, traversal, authorization
or an aggregate serialized response bound.

The owned result carries the actual previously hash-matched path from retained
evidence, rather than reconstructing a lookalike from a prefix. The admission test
checks its canonical digest and closed-result refusal. The separate production-JAR
storage gate checks exact path equality through the optional SPI using real
PostgreSQL/LocalStack archived content. Its result is recorded in storage.log/XML.

Transport implementation and its in-process conformance, callback lifetime,
authorization, error sanitization and aggregate response gates remain required.
