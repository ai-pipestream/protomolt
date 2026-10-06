# Non-executing reserved preparation reads

Focused command:

```sh
./gradlew :protomolt-repo-container:test \
  --tests '*RepositoryReservedPreparationIT' \
  --tests '*DocumentPublicationPreparationJournalIT' \
  --tests '*DocumentPublicationPreparationCodecIT' \
  --tests '*DocumentSuccessor*IT' \
  --tests '*RepositoryCoordinatorRecoveryDiscoveryIT' \
  --tests '*RepositoryCoordinatorReservationIT' \
  --tests '*DocumentPublicationProcessRecoveryIT' --console=plain
```

Result: 73 tests, zero failures/errors/skips; BUILD SUCCESSFUL in 52 seconds.
Compressed XML retains results from PostgreSQL and the real LocalStack fixtures.

The new loader confirms the live reservation and expired owner with claim-before-
owner locks, reserves bytes before fetching preparation, decodes outside SQL locks,
and checks the exact state and current execution caller's read access before
returning borrowed data. It never stamps a write fence or renews leases. The
ordinary preparation journal uses the same extracted integrity decoder and retains
its original authority path.

Eight new cases cover both reservation kinds, the unchanged V91 general-loader
refusal, exact claim/owner/fence/lease state, missing private authority, wrong
principal/owner/kind, insufficient budget, current-policy revocation between read
and delivery, installation/cancellation during that interval, expired reservation,
and explicit absent preparation for a legitimate unjournaled operation. Failures
release budget. Both real delayed-PUT variants reload through the new path.

The first broader run found one stale existing test expectation: the transferred
bound claim was correctly rejected by `requireUnbound`, but the assertion expected
an older error message. `stale-assertion-red.xml.gz` retains that failure. Only the
expected message changed; the production guard was not weakened.

Scope: this is private reserved read authority, not execution permission,
automatic host recovery, unactivated replacement supersession, pin release, or a
fresh-process graceful recovery proof. The provider fixtures retain their original
preparation for predecessor assertions even though successor installation now uses
the independently reloaded record.

Packaged runtime command:

```sh
./gradlew :protomolt-repo-container:admissionStorageTest --console=plain
```

The production-JAR storage qualification passed with zero failures/errors/skips.
Its scoped typed graceful-successor probe now loads preparation through the new
reserved read path before installing and activating the successor, then validates
real provider publication, schema retention, and receipt replay. The compressed
runtime XML contains the timing and full qualification output.
