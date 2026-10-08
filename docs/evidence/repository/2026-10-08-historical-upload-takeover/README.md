# Historical upload takeover

Base: 0c4f4fdcc. The packaged PostgreSQL/LocalStack gate passed in 30 seconds,
Gradle exit 0. The adjacent XML records one aggregate test, zero failures,
errors or skips. Source hashes identify the tested changes.

```sh
./gradlew :protomolt-repo-container:admissionHistoricalRuntimeTest --tests '*HistoricalRuntimeQualificationTest.initialOwner' --max-workers=2 --console=plain
```

After a real provider PUT completes, the fixture expires the predecessor claim
and installs a successor before returning the predecessor's PUT response.
The predecessor fails its claim check and cannot retry. Its object remains
unverified; its stored bytes and provider version are checked independently.

After that failure, the fixture captures the source again, activates the installed
successor and stages through the shared upload coordinator. The successor uses
its own attempt and token. Exact replay preserves that selection without a third
PUT. The test checks capture, scope and metadata-budget cleanup and confirms the
predecessor still has no verified object or assessment.

Sol reviewed the test. This qualifies same-process takeover and successor staging.
It does not qualify publication after takeover, process restart or public routing.
The focused aggregate also runs the preceding initial-owner fault scenarios;
it is not evidence of a full storage-suite run at this checkpoint.
