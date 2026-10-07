# Initial historical assessment start

Base: `13e95cc25110f15b9061fa44a4d959588631ff59` on
`refactor/repository-composition`. Local PostgreSQL 18 container tests.

The private initial historical execution handle now starts or retrieves a sticky
V83 assessment identity. Each call rechecks current authority, claim/owner,
immutable journal bindings, placement, physical references and live capture pins.
The previously verified immutable capture avoids rebuilding its full JSON tuple
set on each start. Concurrent handles converge on one assessment ID and deadline.
Changed retention and expired starts fail; rollback and lost acknowledgement have
separate real JDBC fault cases. Ordinary explicit proposed-ID behavior is preserved.

Sol reviewed the implementation and identified two corrections made here:

- The retry checks database time after acquiring the V83 row lock. The dedicated
  expiry test observes a real PostgreSQL blocking PID, crosses the stored database
  deadline while holding the lock, then verifies refusal and unchanged coordinates.
- Shared admission now checks a supplied credential's durable current generation
  for existing destinations. Existing READ/WRITE policy is insufficient after key
  revocation. Absent-target creation retains its existing credential/grant checks.

`review-red` records the real revoked-key regression: 4 tests, 1 failure because
assessment start incorrectly succeeded. The expiry fix was already present in that
run; its passing test is not a recorded red/green reproduction of the old query.
`initial` and `pre-review` preserve earlier implementation runs, not final coverage.

Final command:

```sh
./gradlew :protomolt-repo-container:test \
  --tests '*DocumentHistoricalExecutionIT' \
  --tests '*DocumentHistoricalRegistrationAuthorizationIT' \
  --tests '*DocumentHistoricalStartExpiryWaitIT' \
  --tests '*DocumentAssessmentStartJournalIT' \
  --tests '*DocumentInitialCaptureConfirmationIT' \
  --tests '*RepositoryCredentialAuthoritiesIT' \
  --tests '*DocumentPublicationCommitIT' \
  --max-workers=2 --console=plain
```

Result: **77 tests, zero failures/errors/skips**, 1m31s. Final XML and Gradle log
are in `final/`; source hashes identify the tested Java files. The concurrent
first-start case launches two handles together; only the separate expiry case
asserts an observed database lock wait. Provider observations in the historical
source fixture are supplied by the fixture; these are not provider performance tests.

This does not enable claimed historical upload admission, assessment CREATE,
publication, successor execution or public restoration. Those remain subsequent
implementation and qualification work. No protobuf contract, migration, deployment
or hosted CI result is claimed here.
