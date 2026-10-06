# Recovery after an unactivated replacement dies

V98 records an immutable supersession with exact prior reservation, owner,
preparation and installation-phase identity. Claim-before-owner locks protect the
transition. The new claim does not grant execution, renew the owner, or release
reader pins. Existing V93 installation and V94 activation remain required.

Focused regression command:

```sh
./gradlew :protomolt-repo-container:test \
  --tests '*RepositoryCoordinator*IT' --tests '*RepositorySuccessor*IT' \
  --tests '*RepositoryReservedPreparationIT' --tests '*DocumentSuccessor*IT' \
  --console=plain
```

Result: 142 tests across 14 suites, zero failures/errors/skips; build successful
in 1 minute 22 seconds. This includes 15 supersession tests covering both original
reservation kinds, both unactivated phases, repeated replacement deaths, exact
retry and lost acknowledgment, concurrent proposals, immutable evidence, migration
with existing rows, and atomic rollback when publishing the parent fails. Held
V93/V94 transactions expire before commit and roll back; a waiting supersession
then succeeds. This is not proof of every possible ordering of those operations.

Real process qualification:

```sh
./gradlew :protomolt-repo-container:test \
  --tests '*DocumentPublicationProcessRecoveryIT' --console=plain
```

All three cases passed with zero failures/errors/skips (71.813 seconds in the
test suite). Each begins with a real versioned LocalStack PUT and SIGKILL of the
writer before verification. Two cases start a replacement JVM, kill and reap it
after committed reservation or installation, and then recover in a third JVM.
Only the public command and payload files are passed between invocations. Recovery
uses production discovery, reservation, bounded preparation loading, installation
and activation. Assertions check the V98 phase, final epoch/binding, original
unverified/unselected object, retained ACTIVE readers and storage-free receipt
replay. PostgreSQL 18 and LocalStack 3.8 supply actual SQL and provider behavior.

Sol reviewed the SQL/Java transition and process-test changes and found no blocking
issue. The final phase/epoch assertions incorporate its process-review feedback.

Scope: admin/opaque recovery building blocks, not an automatic recovery host,
scoped typed process recovery, reader-pin reclamation, or a performance result.
RustFS performance qualification remains separate. Public protobuf contracts did
not change.

The packaged runtime regression also passed:

```sh
./gradlew :protomolt-repo-container:admissionStorageTest --console=plain
```

Build successful in 2 minutes 58 seconds, with zero test failures/errors/skips.
The runtime inventory contains 38 production artifacts. Its compressed XML is
retained beside the focused and process-test reports.

Follow-up qualification adds pre-call and post-commit cancellation, plus the
opposite activation ordering: V94 commits while live, leases expire, and an exact
installed-phase V98 proposal is refused without changing the claim or adding a
reservation. Discovery then reports the activated epoch as expired bound recovery.
Cancellation after a real commit remains CANCELLED; a fresh exact retry confirms
the committed reservation without lease renewal. Sol reviewed these cases.

The focused `RepositoryCoordinatorSupersessionIT` rerun passed all 18 cases with
zero failures/errors/skips (68.269 seconds; build 1 minute 12 seconds). Its separate
`supersession-cancellation-activation.xml.gz` preserves the follow-up evidence.
This follow-up changes tests only; the production-JAR result above applies to the
unchanged production implementation.
