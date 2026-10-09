# Preparation coverage certification checkpoint

Base: `da34a816f37e9b8c616472d467f8f4f111f08cf0`. This checkpoint adds V118 and
canonical coverage certification for new journal preparations. It does not enable
pruning, reconcile old entries, or qualify performance or deployment.

## Behavior and review

New journal preparations certify their sealed root projection against the canonical
protobuf command in the same transaction. SQL checks hashes, identity and atomicity;
the trusted Java handler interprets protobuf. Existing preparations remain indexed
as unresolved. An internally consistent empty SQL header for a historical command
is rejected by canonical verification. Exact retries do not certify legacy rows.

The first combined run found an incorrect blanket requirement: recovery successors
are executable preparations referring to older retained roots. V118 now permits
only the exact same-transaction V93 successor install, with its live claim and
existing deferred modes/owner checks. That branch requires an unresolved entry and
no history header or certificate. Installation grants no capture or activation.
Sol reviewed the classification, SQL guards and migration fixtures with no remaining
production blocker for this checkpoint.

## Validation

All runs use real PostgreSQL 18 test containers and `--max-workers=2 --console=plain`.
Provider observations in the existing publication fixtures remain synthetic; these
are SQL, canonical-command and lifecycle checks, not provider performance evidence.

```
./gradlew :protomolt-repo-container:test \
 --tests '*DocumentPreparationCoverageCertificatesIT' \
 --tests '*DocumentCaptureAdmissionClosureIT' \
 --tests '*RepositorySuccessorInstallIT' \
 --tests '*RepositoryHistoricalSuccessorActivationIT' \
 --max-workers=2 --console=plain
```

40 tests, zero failures/errors/skips; 1m13s. Includes false-empty rejection,
uncertified-write rollback, certificate immutability, ordinary and historical
successors, exact retries, failed installs and historical activation.

```
./gradlew :protomolt-repo-container:test \
 --tests '*DocumentPreparationPinOwnerMigrationIT' \
 --tests '*DocumentPreparationCaptureDrainIT' \
 --tests '*DocumentPreparationRootReleaseIT' \
 --tests '*DocumentPreparationHistoryRootsIT' \
 --tests '*DocumentPreparationSourceFenceIT' \
 --max-workers=2 --console=plain
```

46 tests, zero failures/errors/skips; 59s. V104 and V106 fixtures use actual older
writer SQL and the existing pre-V112 reader-registration adapter, without disabling
SQL guards. Root release and capture drain checks remain intact.

The V106 migration case was subsequently strengthened to retain its admitted owner
and publication modes. Its exact test was rerun successfully, one test with zero
failures/errors/skips. That final XML is under `migration-owner/`; the parent XMLs
preserve the two earlier complete focused runs.

## Remaining work

- Bounded reconciliation of existing rows, including verified live roots and exact
  terminal release/capture-drain receipts.
- Separate bounded successor lineage verification through immutable install edges;
  no successor can clear unresolved merely because it was installed or activated.
- Additional adversarial certification/lineage and concurrency cases before pruning
  authorization, including future-writer and source-acquisition races.
- Pruned state, atomic reference release, physical cleanup, schema reclamation,
  backup/restore, RustFS performance and horizontal scaling qualification.
- This is not a current full-storage-suite result, a merge to main, or a deployment.
