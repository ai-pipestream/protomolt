# Historical public SQL cleanup rollback

Parent: `424423e6680d6ccad3bf8e4dc9b6be9e9d4f309a`.
Test-only changes in `historical-cleanup-fairness`; Sol reviewed the fault scope,
rollback assertions and retry lifecycle without finding a blocker.

The packaged runtime probe publishes historical content through the library using
real PostgreSQL and LocalStack S3. A test-only PostgreSQL trigger targets the exact
reader incarnation. It runs after native-pin deletion and the production retention
mirror deletion, verifies that the mirror is absent, and raises a specific SQL
error. Maintenance must report that exact error.

Rollback must restore the original pin/object and mirror identities, produce no
capture-drain receipt, and retain the generation, read lifetime and byte budget.
Library and authenticated in-process gRPC retries must return the original committed
response without additional PUTs, schema resolution or host selection.

Removing the trigger allows maintenance to release both pins and mirrors and record
exactly one capture-drain receipt. Actual provider GETs verify published content and
historical identity. A new remote publication demonstrates reusable capacity.

```sh
./gradlew :protomolt-repo-container:admissionHistoricalRuntimeTest \
  --tests '*HistoricalRuntimeQualificationTest.initialOwner' \
  --max-workers=2 --console=plain
```

Passed in 1m10s: one aggregate JUnit case, 67.326 seconds, zero failures, errors or
skips. The driver requires `HISTORICAL_PUBLIC_SQL_CLEANUP_RETRY_OK`. The Gradle log
and XML are archived beside this file.

This covers a pin-release transaction rollback, not every SQL cleanup failure or
lost cleanup acknowledgement. The transport is authenticated in-process gRPC with
test identity binding; it does not establish external identity-provider behavior,
TCP failure handling, throughput or scale-out. The public historical factory
remains gated by the remaining acceptance cases in the design.
