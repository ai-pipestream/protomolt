# Versioned assessment slot snapshots

New snapshots use v2 with explicit content tags and historical source node,
revision and full ordinal. Version 1 remains readable using its unchanged encoding.
V87 expands the allowed version set without rewriting retained rows. Historical
slots cannot be encoded as v1; unsupported versions fail before reservation.

Retained verification reads immutable format metadata, computes the exact expected
encoding under the retained-owner lock and reserves that size before JDBC reads
bytes. Both metadata and stored byte version, exact bytes and digest must agree.
Historical slot checks bind source node/revision/ordinal and full physical identity;
retained root checks recognize historical size and hash.

Evidence:

- Unit tests inspect the complete v1 layout, v2 historical tag/source layout,
  source-node digest changes, unsupported/legacy refusal and reservation release.
- A real PostgreSQL fixture writes v1 at schema V86, migrates to V87, and verifies
  it through current authorization and owner locks. The stored version stays 1.
- Existing assessment creation, replay, corruption, retention and journal cases
  exercise the new default v2 encoding.
- Sol reviewed compatibility, bounds, migration and tests with no material blocker.

```sh
./gradlew :protomolt-repo-container:test --tests '*DocumentAssessmentSlotSnapshotTest' --tests '*DocumentAssessment*IT' --tests '*DocumentPublicationPreparationJournalIT' --tests '*DocumentJournaledPublicationSessionsIT' --console=plain
```

Result: 99 tests, zero failures/errors/skips, 37 seconds. Local log:
`/tmp/protomolt-assessment-snapshot-v2-qualified.log`. `git diff --check` passed.

Historical CREATE remains gated. This does not establish an end-to-end historical
stage or restore: the pinned-source binder, command-to-slot proof, atomic reference
publication and recovery remain incomplete. Add a SQL historical-slot/v1-snapshot
mismatch case before activation; SQL permits both versions and Java refuses the
incompatible interpretation. No hosted CI, merge or deployment is claimed.
