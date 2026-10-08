# Historical upload lease expiry

Base: 1957d1d09. The production-JAR PostgreSQL/LocalStack gate passed in 27 seconds,
Gradle exit 0, one aggregate test with zero failures/errors/skips. XML is adjacent.

```sh
./gradlew :protomolt-repo-container:admissionHistoricalRuntimeTest --tests '*HistoricalRuntimeQualificationTest.initialOwner' --max-workers=2 --console=plain
```

After an actual PUT, the fixture locks claim then owner and waits for database
time to pass both lease deadlines. SQL confirms expiration before releasing the
locks. The late result fails the execution claim check, remains unverified, and
cannot be retried by the expired owner. The test checks the stored version and
bytes, one PUT total, zero assessments, and resource cleanup.

The first fixture run used the wrong owner column. After correction, the test
passed. Sol identified a competing timeout risk; the final fixture gives this
2-second lease case a 5-second lock wait and 10-second statement limit. Other
cases and production settings are unchanged. The final run includes that fix.

This proves initial-owner expiry fencing. Successor takeover remains unqualified.
