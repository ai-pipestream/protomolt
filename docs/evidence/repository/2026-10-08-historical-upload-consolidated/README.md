# Historical upload integration checkpoint

At 7a99317f3, the reviewed successor changes and lifecycle branch are combined.
The integration check passed in 41 seconds, Gradle exit 0: three SQL lifetime
tests and one production-JAR aggregate, zero failures/errors/skips. XML is adjacent.

```sh
./gradlew :protomolt-repo-container:test --tests '*DocumentHistoricalUploadLifetimeIT' :protomolt-repo-container:admissionHistoricalRuntimeTest --tests '*HistoricalRuntimeQualificationTest.initialOwner' --max-workers=2 --console=plain
```

The earlier full storage gate is recorded in 2026-10-08-successor-upload-storage.
It passed before the lifecycle tests were combined. This checkpoint verifies their
integration; it does not claim a second full storage run or public API readiness.
