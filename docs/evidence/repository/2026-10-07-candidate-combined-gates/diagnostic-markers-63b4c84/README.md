# Diagnostic marker qualification

The marker-only probe change was exercised at candidate HEAD
`63b4c84d55aae79d8bcd428a24eb9daf8802052b`, with no source edits during the
run. The exact pre-run worktree state and SHA-256 hashes of both edited runtime
resources are in `preflight.txt`.

Command:

```text
./gradlew :protomolt-repo-container:admissionStorageTest --rerun-tasks --max-workers=2 --console=plain
```

Result: `BUILD SUCCESSFUL in 8m 9s`. The JUnit XML records one test, zero
failures, zero errors, zero skips, and 477.186 seconds for the test method.
The gate compiled all runtime-inventory sources and ran the packaged SQL host
plus its restart probes.

During the run, the live host log at
`/tmp/junit-1117809782815905482/host.log` showed the rejected-expiry phase:

```text
ASSESSMENT_REJECTION_EXPIRY_START
REJECTED_EXPIRY_RECEIPT_LOADED_1MS_OK
REJECTED_EXPIRY_EXISTING_CAPTURED_6MS_OK
REJECTED_EXPIRY_OWNER_ROW_LOCKED_7MS_OK
REJECTED_EXPIRY_WAIT_READERS_BLOCKING_START_8MS_OK
REJECTED_EXPIRY_READERS_BLOCKED_29MS_OK
REJECTED_EXPIRY_WAIT_RETENTION_START_29MS_OK
REJECTED_EXPIRY_RETENTION_REACHED_19856MS_OK
REJECTED_EXPIRY_BLOCKER_COMMITTED_19857MS_OK
REJECTED_EXPIRY_CAPTURE_RESULT_START_19857MS_OK
REJECTED_EXPIRY_CAPTURE_REFUSED_19858MS_OK
REJECTED_EXPIRY_DELIVERY_REFUSED_19858MS_OK
REJECTED_EXPIRY_ACTIVE_SESSION_GUARD_START_19858MS_OK
REJECTED_EXPIRY_ACTIVE_SESSION_GUARD_19860MS_OK
REJECTED_EXPIRY_SESSION_DRAINED_19861MS_OK
REJECTED_EXPIRY_EVIDENCE_RELEASED_19863MS_OK
REJECTED_ASSESSMENT_EXPIRY_WAITS_OK
REJECTED_EXPIRY_COMPLETE_19865MS_OK
ASSESSMENT_REJECTION_EXPIRY_END
```

The test removed its temporary `junit-*` directory after success, so the full
live host log is not retained. The quoted markers were copied from that log
while the test process was active. The JUnit XML and Gradle result are retained
here; `gradle-result.txt` is the final 45 lines of Gradle output.
