# Shutdown after rolled-back successor activation

Base: `b5796bced072bfd2585ba304eb2357a193aa2ce7`.

`DocumentSuccessorTargetIT.rolledBackActivationCanStopWithoutWaitingForAnotherCoordinator`
creates a real installed successor with a live five-minute lease. A JDBC
before-commit gate rolls back actual V94 activation. The manager retains that
successor, and SQL confirms the original live reserved claim with no successor
execution. Before the fix, shutdown failed when it tried to fence the unactivated claim
for V90. The historical predecessor V91 guard reports that execution is closed.

The test expects shutdown to dispose of the closed local context without an
external takeover, while preserving its durable reservation and adding neither
V90 nor V91. `red.tar.gz` preserves the original failing case with no skips.
The manager now classifies this as a local detached outcome and revalidates it
before final shutdown. This does not attest remote quiescence or authorize cleanup.

```sh
./gradlew :protomolt-repo-container:test --tests '*DocumentSuccessorTargetIT.rolledBackActivationCanStopWithoutWaitingForAnotherCoordinator' --console=plain
```

`initial-green.tar.gz` has the original four successor-target cases passing.
Review added a strict-chain check for retained successors on the first shutdown
pass. `attestation-red.tar.gz` preserves a second failure: direct final attestation
accepted an unreviewed transfer when the preceding drain remained unresolved.
The final attestation now uses the same strict successor check.

`final-green.tar.gz` records 71 cases, zero failures/errors/skips:

- 11 successor-target cases: rollback, exact activation after detachment,
  changed fingerprints, private authority, raw versus reviewed takeover both
  before and after classification, and shared byte-budget exhaustion/release.
- 27 journaled session cases.
- 18 expired-coordinator cases.
- 15 retained recovery-attempt cases.

```sh
./gradlew :protomolt-repo-container:test --tests '*DocumentSuccessorTargetIT' --tests '*DocumentJournaledSessionsIT' --tests '*RepositoryCoordinatorExpirationIT' --tests '*RepositoryRecoveryAttemptsIT' --console=plain
```

The late-activation case deliberately commits V94 through the real activation
implementation after the first detached observation. It covers that SQL-state
interleaving, not a concurrent lost-acknowledgment race. The capacity case holds
a real budget lease; it does not impersonate provider work. These tests are
correctness evidence, not latency, throughput or horizontal scaling qualification.

Sol reviewed the implementation and the additional transfer/capacity cases.
Recovery-owner disposal and automatic managed-host retry integration are still
outstanding. No public contract changed.

`packaged-green.tar.gz` records the separate packaged storage runtime check:
one case, zero failures/errors/skips, 188.633 seconds. Its isolated runtime
inventory contains 38 artifacts. This is regression coverage for the existing
provider/schema/recovery composition, not a new detached-successor worker race.

```sh
./gradlew :protomolt-repo-container:admissionStorageTest --console=plain
```
