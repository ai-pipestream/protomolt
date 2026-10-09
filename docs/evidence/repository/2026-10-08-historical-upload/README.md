# Historical uploads through the shared coordinator

Base: ec653a96c. Private initial-owner staging now uses DocumentUploadCoordinator
with the complete historical SQL authorization checks. Child Work, registration
and metadata reservation last through actual worker drainage. The child uses the
coordinator's configured SQL lock and statement timeouts.

The production-JAR probe passed with real PostgreSQL and LocalStack: one aggregate
test, zero failures/errors/skips, Gradle exit 0 in 30 seconds. It verifies initial
publication and lost CREATE acknowledgement reconciliation after coordinator
staging, plus exact verified selection replay. It does not count provider PUTs.

```sh
./gradlew :protomolt-repo-container:admissionHistoricalRuntimeTest --tests '*HistoricalRuntimeQualificationTest.initialOwner' --max-workers=2 --console=plain
```

Sol reviewed the change. Review identified a missing bounded Tx view in the first
implementation; that was corrected before the provider test.

Still required: successor coordinator qualification; barrier tests for revocation,
expiry, close and late observations; managed public routing and transport parity.
The request Attempt monitor still serializes its operations. Public historical
publication remains disabled. This evidence does not complete the repository goal.

Regression gate: 66 tests (40 uploader, 15 historical execution, 11 successor
pins), zero failures/errors/skips. Gradle exited 0 in 51 seconds; XML is adjacent.

```sh
./gradlew :protomolt-repo-container:test --tests '*DocumentUploadCoordinatorIT' --tests '*DocumentHistoricalExecutionIT' --tests '*RepositoryHistoricalSuccessorPinsIT' --max-workers=2 --console=plain
```
