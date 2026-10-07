# Historical capture ownership

Validated working-tree changes based on
`ed593eef55c044b4fb3514611a0bc235ebb0e53e`; this receipt is committed with V105,
ownership checks and tests. Sol reviewed both implementation and the migration
fixture correction without a blocker.

```sh
./gradlew :protomolt-repo-container:test --tests '*DocumentHistoricalMultiRevisionPublicationIT' --tests '*DocumentPreparationPinOwnerMigrationIT' --tests '*DocumentHistoricalRegistrationAuthorizationIT' --tests '*RepositoryCoordinatorDrainIT' --tests '*RepositoryCoordinatorLocalDrainIT' --max-workers=2 --console=plain
```

Exit 0, 41 seconds, 36 tests: historical multi-revision (2), ownership migration
(1), registration authorization (2), coordinator drain (17), local drain (14).
No failures, errors or skips. XML trailing whitespace was normalized.

Actual PostgreSQL migrations and constraints verify exact stored epoch/token/
coordinator identity, wrong-identity refusal, immutable ownership, and deferred
owner completeness. Existing registration rollback and lost-acknowledgment cases
now include the ownership table. Repeated capture batches and released-pin
confirmation retain their original ownership. The new V104-to-V105 test creates
a historical batch with the actual V104 SQL protocol, upgrades it, and verifies
that both automatic retry and direct SQL cannot invent ownership afterward.
Provider observations used to prepare source revisions are synthetic fixtures.

The first run failed compilation because expression lambdas were ambiguous
between the transaction helper's Consumer and Function overloads. Explicit void
blocks fixed the test code. The next run exposed six older migration-test cases
calling the current journal writer against V89/V90/V94, which lack V103's history
set table. A narrowly version-checked fixture now writes the real V81 preparation
format before upgrade. Modern-schema cases still use the production writer.
No production compatibility fallback or SQL guard was weakened. Both failed-run
logs are retained.

This checkpoint does not prove batch-local worker drain, historical successor
execution, terminal cleanup or retention release. The production-JAR storage gate
passed at the immediately preceding V104 checkpoint; it was not rerun here, and
that earlier result is not claimed as V105 runtime qualification. These focused
suites apply V105 to fresh and populated databases. No protobuf contracts changed.
