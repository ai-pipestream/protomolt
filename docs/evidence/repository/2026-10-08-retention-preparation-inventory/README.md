# Retained preparation root inventory

The internal revision inventory now counts preparation roots by exact source
node and revision in its existing SQL statement. The V103 index supports this
lookup. This exposes existing retention protection; it grants no pruning
permission. Legacy journal coverage remains explicitly unresolved.

```sh
./gradlew :protomolt-repo-container:test --tests '*DocumentRevisionRetentionInventoryIT' --tests '*DocumentPreparationMultiRootReleaseIT' --tests '*DocumentPreparationRootReleaseIT' --max-workers=2 --console=plain
```

Passed: 13 tests, zero failures, errors or skips; 17-second build. Tests use real
PostgreSQL. Shared publication fixtures supply synthetic provider observations.
Two revisions retain their preparation roots after read pins drain. Partial root
release rolls back; complete release changes counts from two to one and one to
zero while keeping another preparation's root and the permanent release receipt.
The V87 migration test verifies the root lookup index and unresolved coverage.

The first run exposed an older migration fixture using current reader registration
against V110. Its repair uses explicit test-only legacy registration SQL, rejects
host-bound registration, and disables the adapter before real V110-to-latest
migration. Production registration has no old-schema fallback. Review also caught
a pre-upgrade assertion against the later release table; that assertion now checks
only existing roots. The final archived run includes both corrections.

Sol reviewed the inventory change with no blocker; the coordinating agent reviewed
the migration fixture repair. Public historical START reconciliation is documented
as planned work and remains separate from this change. No protobuf changed.
