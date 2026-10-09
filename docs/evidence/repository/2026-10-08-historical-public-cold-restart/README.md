# Historical publication through gRPC after a process crash

Base: `d0c886a5d61837ccb4fd78a6754cd0a0a4bf9d49`, with the test and documentation
changes in this commit. Production Java, protobuf contracts and migrations are
unchanged. The historical runtime factory remains package-private.

```sh
./gradlew :protomolt-repo-container:admissionHistoricalRuntimeTest \
  --tests '*HistoricalRuntimeQualificationTest.coldPublicProcessRestart' \
  --tests '*HistoricalRuntimeQualificationTest.coldProcessRestart' \
  --max-workers=2 --console=plain
```

Passed in 3m13s: six cases, zero failures, errors or skips; XML suite time 189.777
seconds. Each path covers a crash after initial ownership, successor reservation,
and successor installation. The log and JUnit XML are archived beside this file.
Sol reviewed the fixture changes and found no blocking issue.

The writer calls `Runtime.halt(23)`. The parent verifies the exact child process
exit and the closure of its uniquely named SQL sessions, then records a durable
host termination receipt. Recovery starts in a fresh JVM with caller request
bytes and resubmitted upload data. It receives no predecessor execution handle,
plan or historical content bytes.

The new path calls the shared repository through `RemoteDocumentPublicationRepository`
and an authenticated in-process gRPC server. Its host selector supplies no
placements: the dispatcher must recover the original placement from its journal.
The resolver rejects requests for historical definitions; only the resubmitted
upload resolves a fresh schema. Authentication tokens and bindings are synthetic
test fixtures. This does not qualify a network deployment or an external identity
provider.

Assertions cover:

- The exact receipt on gRPC retry, followed by library replay and comparison with
  the durable operation result, without another selection or schema resolution.
- One revision commit, one assessment and the expected START, reservation,
  installation, supersession and activation counts for each crash boundary.
- The current committed revision, real provider-version readback with checksums
  and upload-byte equality, and preserved historical object, backend, namespace,
  key and provider-version identities.
- Independent recapture of the original historical revision after publication.
- Fresh runtime generation retirement, reader cleanup and zero remaining byte
  reservations.
- The original writer's pins remaining protected until proven host termination
  and bounded recovery, followed by capture/root release and receipt replay.

The original private recovery cases still run separately, retaining their own
publication, readback and cleanup assertions. The public path does not call
`prepareCold` or manually preload historical fragments before publication.

Remaining dispatcher qualification includes concurrent same-process takeover,
revocation/cancellation and cleanup-error behavior at the public boundary, plus
broader regression gates before host API exposure. This is correctness evidence
using PostgreSQL and LocalStack; RustFS performance and scale-out are separate.
