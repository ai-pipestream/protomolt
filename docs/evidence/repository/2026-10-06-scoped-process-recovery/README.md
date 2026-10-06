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

## Revoked authority after writer death

Base: `ae8a9dfad6d92da7d6c02d9c3d743420ac9d9de3`.

```sh
./gradlew :protomolt-repo-container:test --tests '*DocumentPublicationProcessRecoveryIT.killedWriterIsRecoveredByFreshJvm' --console=plain
```

Six cases passed without skips: the administrative recovery/supersession cases,
live scoped recovery, and two new scoped refusals. After killing and reaping the
writer, the parent revokes either its exact credential or its exact creation grant
through the production authority operation. The fresh JVM refuses delivery in
`RepositoryReservedPreparation.load`, with the specific credential/grant error.
No success, successor execution, revision commit, extra attempt or destination is
created. The predecessor's real bytes and ACTIVE reader evidence remain intact.
The coordinator may reserve recovery before delivery is refused; this test does
not assert zero recovery metadata changes or cleanup of retained bytes.

Sol reviewed the negative assertions and found no blocker. `revoked-green.tar.gz`
contains this six-case result. These cases cover revocation before the fresh
process loads preparation; revocation after preparation delivery/activation and
typed recovery remain separate qualification.

## Revocation after successor activation

Base: `943cdb1dcc738219a9c24fbdd17cbe1bb9104332`. The same targeted command
now passes nine cases, with no skips. Three added cases pause the fresh JVM after
the manager has committed successor activation and before publication execution.
The parent observes the activation marker and one durable execution row, revokes
either the credential or grant, then releases the child through a test-only file
barrier. Execution reports the exact authorization failure. The durable activation
remains, but there is no new attempt, success, revision commit or destination.
The live-authority control crosses the same barrier and publishes normally.

Sol found no blocker. `activated-green.tar.gz` records these nine results.
The barrier has a bounded child wait and the parent reaps the process on failure.
This qualifies revocation between activation and execution; it does not imply
automatic cleanup, typed recovery, or revocation at every later provider phase.
