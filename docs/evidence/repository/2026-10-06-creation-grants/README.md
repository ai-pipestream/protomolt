# Exact-operation creation grant qualification

Local working-tree qualification, based on `33ad9d09be25dde2fe908fe3b6674ad41cd3fc69`.
This is an internal durable grant primitive, not an available publication API.
Publication admission does not yet consume these grants.

Command:

```sh
./gradlew :protomolt-repo-container:test --tests '*RepositoryCreationGrantsIT' --tests '*DocumentPublicationPreparationCodecIT' --tests '*RepositoryExecutionClaimLedgerIT' --tests '*DocumentInitialAdmissionIT' --tests '*DocumentPublicationReplayIT' --console=plain
```

All 48 affected tests passed without skips. `affected-green.tar.gz` contains the test result XML and binary results.
The same invocation also ran `:protomolt-repo-container:admissionStorageTest`;
its packaged runtime test passed without skips. Results are in `packaged-green.tar.gz`.
The grant tests use PostgreSQL 18, actual execution-scope arbitration and database
lock observations. They cover both admission-first scope winners, grant-first
claim admission, exact retry, expired installation, expiry during a row-lock wait,
credential rotation and revocation, placement failure rollback, transaction-stamp
replacement, and direct SQL late-insertion, mutation, deletion and revival refusal.
These tests do not execute a storage provider or establish performance capacity.

Sol reviewed the primitive and tests without finding a production blocker. Its
test scheduling concern was addressed by widening the expiry window to ten seconds.
Missing backend generation now produces a typed repository conflict.

Migration from V99 preserves existing scopes with null transaction stamps, creates
no grants, and preserves pending replay. Digest tests cover normalized map ordering,
changed snapshots, malformed/oversized text and the aggregate encoding bound.
The design and operation inventory record the internal-only boundary.

Admission integration remains unfinished and must derive the digest from its actual selected
placements checked under domain locks, never from the grant or request alone.
