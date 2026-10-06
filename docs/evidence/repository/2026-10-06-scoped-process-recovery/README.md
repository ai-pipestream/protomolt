# Scoped publication after writer process death

Base: `9210c4c05884712d5f0771425188651d89f3ddbe`.

```sh
./gradlew :protomolt-repo-container:test --tests '*DocumentPublicationProcessRecoveryIT' --console=plain
```

Six cases passed without skips: the five existing administrative process-recovery
cases and a new scoped caller case. PostgreSQL 18 and versioned LocalStack storage
perform the real operations. Before first admission, the parent registers the test
credential and an exact command/placement creation grant. The writer process
uploads real bytes and is killed while its PUT acknowledgement is held. The parent
reaps it before starting a separate recovery JVM with the public command/payload.

Recovery waits for actual lease expiry, discovers and reserves the predecessor,
loads its preparation, activates a successor and publishes. Preparation delivery,
activation, execution and replay use the same scoped caller; coordinator operations
use process authority. Assertions verify distinct successor attempts and object
keys, provider version and byte readback, revision binding, no predecessor object
selection, unchanged predecessor evidence and reader incarnations, and receipt
replay with no additional provider calls.

Sol found no administrative execution bypass. The test deterministically derives
a credential identity from its operation ID; this is fixture identity, not API-key
authentication or a production credential resolver. Payloads are opaque. Scoped
typed recovery, revoked credentials/grants during process recovery, recovery host
policy refusal, automatic scheduling and pin reclamation remain separate work.
This is local correctness evidence, not a performance measurement. The archive
contains XML and binary test results.
