# Successor uploads: full storage gate

Base: 35d572b67. The full admissionStorageTest passed in 16m 1s, Gradle exit 0.
The aggregate test has zero failures/errors/skips; XML is adjacent.

```sh
./gradlew :protomolt-repo-container:admissionStorageTest --max-workers=2 --console=plain
```

HistoricalSuccessorCreateProbe uses the shared uploader for historical-only and
mixed successors. It checks verified selection replay, fresh successor attempt
and token identities, and unchanged predecessor verification state. Existing
assessment, recovery and publication checks remain in place. Sol reviewed it.

Later lifecycle/provider-fault tests are separate evidence. Public historical
routing remains disabled.
