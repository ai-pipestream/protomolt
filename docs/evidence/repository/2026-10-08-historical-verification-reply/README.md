# Lost historical verification acknowledgement

Base: a37e85e12. PostgreSQL/LocalStack production-JAR gate passed: one aggregate
test, zero failures/errors/skips, Gradle exit 0 in 27 seconds. XML is adjacent.

```sh
./gradlew :protomolt-repo-container:admissionHistoricalRuntimeTest --tests '*HistoricalRuntimeQualificationTest.initialOwner' --max-workers=2 --console=plain
```

The JDBC wrapper targets verification using the attempt ID, xmin and current
transaction ID. It throws SQLSTATE 08006 after commit. A separate Tx confirms
verification persisted. Replay returns the original attempt and token with one
PUT total. The test checks provider bytes/version and resource cleanup.

Sol reviewed the test without blockers. Successor faults, provider-worker
shutdown and later-callback lock timeouts remain unqualified.
