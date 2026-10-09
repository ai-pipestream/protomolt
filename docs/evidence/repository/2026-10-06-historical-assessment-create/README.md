# Historical assessment CREATE

The internal historical owner can prepare a physical plan from its own exact source
references and retain an observed assessment through CREATE. It returns only the
created identity. The owner remains busy and holds all source Uses while artifacts
are staged and the assessment transaction runs. The ordinary CREATE entry retains
its execution guard; claimed historical sessions and public restoration remain off.

Historical roots now bind their declared object size and hash in retained evidence.
CREATE uses the complete historical authorization set, historical slot preparation,
historical physical binding and its exact transaction-local origin/retention lock
proof for slot binding. The shared owner, command, policy, drive, schema and manifest
checks remain in place. No source-authorization transaction is opened from evidence
callbacks while the CREATE transaction holds locks.

`HistoricalAssessmentCreationProbe` runs inside the production-JAR storage harness,
without the test framework or a fabricated runtime observation. It reads original
provider versions, assesses an older retained revision after the current revision
has advanced, refuses any current-registry lookup, and creates retained assessment
rows. SQL checks establish the exact historical revision/node provenance for every
slot and that neither a publication nor a destination revision advance occurred.
Closing the owner releases the source pin and all accounted payload bytes.

```sh
./gradlew :protomolt-repo-container:admissionStorageTest --console=plain
```

The full harness passed in 2 minutes 38 seconds, including its existing ordinary
assessment, restart and forced-crash cases. The run emitted
`HISTORICAL_ASSESSMENT_CREATE_OK`; the outer test requires that marker. Log:
`/tmp/protomolt-historical-create-runtime.log`. Storage used real PostgreSQL and
LocalStack's versioned S3 path. This is correctness evidence, not a RustFS latency
or throughput measurement.

The affected ordinary and historical assessment suites also passed: 58 tests,
zero failures/errors/skips, 37 seconds. Log:
`/tmp/protomolt-historical-create-regression.log`. The requested
`DocumentHistoricalAssessmentBindingIT` filter matched no class; only
`DocumentHistoricalRestoreAssessmentIT` and `DocumentPublicationAssessmentTest`
contribute to that count.

Sol reviewed ownership and transaction boundaries with no blocker. The review noted
that final READ authorization can fail after CREATE commits: callers must reconcile
the fixed assessment ID, not assume rollback or blindly retry. Historical-specific
lost acknowledgment, revocation during CREATE, conflicting policy, failed physical
binding and closed borrowed-plan cases still need explicit acceptance coverage.
Existing ordinary recovery coverage does not prove those historical cases. Atomic
reference publication, public activation, hosted CI and deployment are not established.

## Historical lost acknowledgment and borrowed-plan follow-up

The production-JAR probe now repeats historical CREATE with an injected JDBC
acknowledgment loss after the real PostgreSQL commit. Only CREATE receives the
wrapped datasource; artifact staging uses the original transaction service. The
injector is armed after persistence initialization and requires SQLState 08006
after its real commit. Normal return and pre-commit failure fail the test.

Discovery receives no stage coordinates. It recovers the original assessment ID
and deadline, and retained reconciliation verifies the full manifest, historical
slots and roots. Historical-only commands have no upload selections. The test
again checks exact source provenance and no publication or destination advance.
After closing the Historical owner, attempting to admit its borrowed plan must
fail with the exact ended-Use error. All payload reservations are released.

Sol reviewed the fault targeting and assertions with no blocker. The first run
failed on the test's expected lifetime error wording; the implementation already
refused the expired plan. The assertion now matches `Read plan use has ended`.
The new required harness marker is `HISTORICAL_ASSESSMENT_LOST_ACK_OK`.
The full `admissionStorageTest` harness passed in 2 minutes 38 seconds, with zero
failures/errors/skips. Log: `/tmp/protomolt-historical-create-lost-ack-qualified.log`.

Historical-specific policy/authorization changes during CREATE, physical-binding
rollback and post-commit READ revocation still require dedicated qualification.
The added lost-acknowledgment case does not close those separate requirements.
