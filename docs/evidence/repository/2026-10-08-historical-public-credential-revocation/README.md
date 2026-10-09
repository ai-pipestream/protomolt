# Historical public credential revocation during upload

Parent: `26cddf869da52cc005fe7b8a2f1669694ee94d00`.
Sol reviewed these test additions without a blocker.

Library and authenticated in-process gRPC each use a fresh registered credential.
The fixture delays a real LocalStack S3 upload reply while PostgreSQL commits
credential revocation. Releasing the reply must produce UNAUTHENTICATED, with no
operation success, revision commit or assessment owner for that operation.

An exact retry must also return UNAUTHENTICATED without additional provider writes,
host selection or schema resolution. Separate process cleanup authority releases
historical captures and generation resources. Final byte reservations return to
baseline; transport cleanup verifies client and delivery budgets.

```sh
./gradlew :protomolt-repo-container:admissionHistoricalRuntimeTest \
  --tests '*HistoricalRuntimeQualificationTest.initialOwner' \
  --max-workers=2 --console=plain
```

Passed in 1m21s: one aggregate case, 78.924 seconds, zero failures, errors or skips.
The driver requires credential-library and credential-gRPC completion markers.
Original Gradle log and XML are archived here.

Coverage is revocation before upload completion. Final publication SQL races,
READ/WRITE changes, admission-policy changes and recovery-journal cases remain.
Authentication uses a test identity binding; external identity-provider integration,
network failures and performance are outside this evidence.
