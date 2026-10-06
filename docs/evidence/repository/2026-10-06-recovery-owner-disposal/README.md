# Closed recovery-owner disposal

Base: `4cb5bf2619535c8dee3333193963e8e8bc436612`.

The private recovery owner can now stop admission, wait for accepted handles and
release its retained command/preparation copies without discarding a possible
activation. The manager records an exact fingerprint before V94 SQL and captures
its shutdown ownership without SQL or additional byte leases. Missing snapshot
ownership requires permanent claim fencing or an exact successor terminal outcome.
Client receipt replay and pre-owner abandonment do not substitute for that proof.

`focused-green.tar.gz` records 74 cases, zero failures/errors/skips:

- 26 recovery-owner cases, including metadata-only disposal, a held handle and
  waiter notification, rolled-back V94, committed V97/V93/V94 with cancelled replies,
  and pending V98 with an uncertain reply and earlier submitted activation.
- 11 successor-target cases.
- 10 successor-manager cases.
- 27 journaled-session cases.

```sh
./gradlew :protomolt-repo-container:test --tests '*RepositoryRecoveryAttemptsIT' --tests '*DocumentSuccessorTargetIT' --tests '*DocumentSuccessorManagerIT' --tests '*DocumentJournaledSessionsIT' --console=plain
```

`partial-green.tar.gz` records two additional passing cases. Disposal releases one
entry, then encounters either a missing private authority or cancellation for the
next. The remaining entry and its bytes survive; a corrected disposal retry
finishes without changing either durable reservation. No execution retry is
possible after admission closes.

```sh
./gradlew :protomolt-repo-container:test --tests '*RepositoryRecoveryAttemptsIT.partialDisposalCanRetryAfterLaterAuthorityFailureOrCancellation' --console=plain
```

`packaged-green.tar.gz` records the first passing composed storage run: one case,
196.874 seconds, zero failures/errors/skips. In `runOwned`, the predecessor
performs real provider work before a schema interruption. Its lease expires
naturally. A recovery owner reserves and installs a successor, then observes
cancellation only after actual V94 commit. The successor publishes through real
provider uploads and reads and evicts its terminal session. With all remaining
owner budget reserved by an explicit pressure lease, `detachClosed` releases its
duplicate preparation bytes and verifies the exact terminal generation; only the
pressure lease remains. Closing that lease leaves the owner budget at zero.

The independent graceful-publication probe also checks that activation alone is
not terminal proof and a changed fingerprint is rejected after real publication.
These are PostgreSQL/LocalStack correctness checks, not performance measurements.

`packaged-marker-green.tar.gz` records the final rerun after requiring the
`RECOVERY_OWNER_TERMINAL_DISPOSAL_OK` marker in the outer test harness: one case,
197.535 seconds, zero failures/errors/skips.

```sh
./gradlew :protomolt-repo-container:admissionStorageTest --console=plain
```

Sol reviewed the callback timing, monitor order, terminal identity, shared-budget
behavior and pending supersession. Owner disposal does not drain provider or
schema workers. Managed-host recovery routing and shutdown integration remain
outstanding; no public protobuf or transport contract changed.
