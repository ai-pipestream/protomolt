# Managed runtime shutdown with a held schema worker and takeover

Base: `e00fd4e023edf888a1e00ef768a45a28bfa8e596`.

`FencedSchemaWorkerProbe` runs in the production-JAR storage test with PostgreSQL,
versioned LocalStack S3 and a Git schema registry. A controlled gate holds the
real descriptor load after uploads and retained reads. Caller cancellation returns
while the resolver worker remains live. Runtime shutdown records V90, but cannot
complete. After natural claim/owner expiry, real V97 transfers the claim and
retains UNKNOWN remote state. Shutdown still waits for the worker; after release it
completes without V91 for the fenced predecessor. Exact claim/reservation state,
unchanged document revision and released local byte/read reservations are checked.

The managed runtime uses a ten-second fixture lease; production defaults and SQL
timestamps are unchanged. SQL and provider ports are borrowed and remain owned by
the fixture until successful runtime drain. This test does not establish process
death, settled remote PUTs, pruning safety, performance, or automatic host recovery.

The initial packaged test passed with no skips (`initial-green.tar.gz`, 189.408s).
Sol reviewed the scenario. Subsequent changes preserve original test failures when
cleanup also fails and bind the snapshot to the current reservation successor.

```sh
./gradlew :protomolt-repo-container:admissionStorageTest --console=plain
```

The final packaged rerun passed, with no failures, errors or skipped cases
(`final-green.tar.gz`). The harness requires `FENCED_SCHEMA_WORKER_DRAIN_OK` from
the child JVM, so compilation or execution omission cannot pass this check.
