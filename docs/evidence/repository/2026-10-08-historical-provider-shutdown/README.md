# Historical provider shutdown

Base: 41a07284b. PostgreSQL/LocalStack initial-owner qualification passed with
one aggregate test, zero failures/errors/skips, Gradle exit 0 in 27 seconds.

```sh
./gradlew :protomolt-repo-container:admissionHistoricalRuntimeTest --tests '*HistoricalRuntimeQualificationTest.initialOwner' --max-workers=2 --console=plain
```

The real PUT completes before a bounded barrier delays its reply. Cancellation
marks the caller future while a separate completion latch remains pending. Owner
and uploader admission close. The uploader still reports an active provider,
retains payload/metadata reservations and cannot report idle. Historical capture
release is also prevented.

After barrier release, actual work completes with cancellation. Provider activity
and transfer reservations return to their previous values. The test retrieves the
stored version and bytes and confirms the attempt has no verified object. Owner
and capture cleanup then complete.

Sol reviewed the test without blockers. Detach timeout by itself reflects the open
request handle; worker activity and memory accounting supply the provider lifetime
evidence. This is initial-owner admission shutdown, not successor shutdown or
parent execution close. The latter has separate SQL-only tests.
