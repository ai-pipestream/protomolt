# Historical selector contract

Local verification on 2026-10-05:

```
./gradlew :protomolt-repo-proto:test :protomolt-repo-spi:test --console=plain
buf lint --path repo/proto/src/main/proto/ai/protomolt/proto/repo/v1/document_publication.proto
scripts/check-proto-compatibility.sh 0b4c4eeab4463ceba7edfbcca99c0a72ea06fb57
```

All commands exited zero. Gradle completed in 6 seconds: 89 proto and 32 SPI tests,
121 total, no failures or skips. The Buf script compiles both complete descriptor
inventories including imports and applies the repository's breaking-change policy.
Existing command v1 golden bytes/hash fixtures remain unchanged and pass.

Generated and DynamicMessage fixtures exercise exact UUID shape, ordinal bounds,
required nested messages, nonblank coordinates, account and slot cross-field rules,
oneof selection and a valid but nonexistent revision/object claim. Such a claim is
shape-valid; only handlers can establish authorization, existence and membership.
The canonical command rejects this new arm explicitly until execution is implemented;
nested unknown fields are still rejected first. No restore operation is advertised.

JSON Schema exposes the ordinal interval and required source/slot/object fields.
Address, same-account and same-slot CEL rules remain runtime constraints, represented
as x-protomolt-cel; standard JSON Schema consumers do not execute them. Existing
OpenAPI translation limits remain, and no generator code changed. The first fixture
used snake_case instead of the generator's JSON camelCase property; it was corrected,
including an explicit presence check to prevent a missing minimum looking like zero.

This is contract and command-boundary evidence only: no historical publication,
provider operation, pruning, process-restart, hosted CI or deployment proof.
