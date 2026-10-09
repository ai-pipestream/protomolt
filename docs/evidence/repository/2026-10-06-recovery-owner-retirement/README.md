# Retire permanently fenced recovery-owner identities

Base: `3d2e512badeb61c2fd3cca0cd50955788980fa77`.

The private recovery owner can release its retained bytes after an exact locked
claim proves every proposed successor identity obsolete, including any pending
supersession. V78's immutable digest, monotone epoch and fixed token per epoch make
the proof permanent. This check does not depend on the winning lease being live.
It does not renew a claim or write a fence.

The real-PostgreSQL cases exercise foreign winners with and without a pending
proposal. They reject retirement before reservation and after expiry of the same
claim. Cancellation after the proof transaction commits leaves the old identity,
byte budget and unresolved count intact. Retrying retirement frees the owner budget
and operation slot; an open accepted handle still contributes to active calls.
The durable claim, owner tokens and leases are unchanged by retirement.

When an old activation session remains in the manager, it is deliberately retained.
The winner has changed only the claim, so owner-based session retirement stays false.
An empty recovery-owner map therefore does not prove whole-host drain. No provider
I/O, reader-pin release or physical cleanup is part of these tests.

The initial 14 cases (12 owner, 2 target) passed without skips and are retained in
`initial-green.tar.gz`. Later assertions cover owner closure before retirement retry
and an absent SQL claim: a supplied observation cannot make absence proof of fencing.

```sh
./gradlew :protomolt-repo-container:test --tests '*RepositoryRecoveryAttemptsIT' --tests '*DocumentSuccessorTargetIT' --console=plain
```

Sol reviewed the locked proof, pending identity comparison, idempotence and ownership
boundaries without a blocker. The exact stored-command check adds a database round
trip and bounded payload read under the claim lock; no latency or throughput claim
is made. Terminal/graceful reconciliation, separately retained session handling and
complete shutdown integration remain unfinished.

The final run passed 13 owner cases, 14 local recovery cases and the packaged
production-JAR storage-runtime case, with no skips (`final-green.tar.gz`):

```sh
./gradlew :protomolt-repo-container:test --tests '*RepositoryRecoveryAttemptsIT' --tests '*DocumentPublicationRecoveryIT' :protomolt-repo-container:admissionStorageTest --console=plain
```

Local verification is separate from hosted CI, merge and deployment.
