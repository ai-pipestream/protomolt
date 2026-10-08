# Public historical READ and WRITE revocation

Parent: `021bb29acf63c4a7a8271120a77274280de6d4a8`.

The authorization fixture now covers READ and WRITE separately through library and
authenticated in-process gRPC. PostgreSQL changes the current document ACL while
a real LocalStack S3 upload reply is delayed. Removing READ preserves WRITE, and
removing WRITE preserves READ; the fixture checks this with DocumentAccessPolicy.
Historical source and destination share the same document in these examples.

After the reply is released, the request must return NOT_FOUND. SQL must contain
no operation success, revision commit or assessment owner. Retry must also return
NOT_FOUND without additional provider writes, schema resolution or host selection.
Explicit process cleanup authority releases historical captures and generation
resources. The original ACL is restored after cleanup, and later requests use the
current mutation revision rather than reviving the obsolete command.

```sh
./gradlew :protomolt-repo-container:admissionHistoricalRuntimeTest \
  --tests '*HistoricalRuntimeQualificationTest.initialOwner' \
  --max-workers=2 --console=plain
```

Passed in 1m22s: one aggregate case, zero failures, errors or skips. The driver
requires all four READ/WRITE library/gRPC markers, plus the existing credential
revocation markers. Original Gradle log and XML are archived here.

This covers ACL revocation before upload completion. Publication-first SQL races,
policy replacement and recovery-journal cases remain. The tests use a transport
identity fixture and do not establish external identity-provider integration,
network failure recovery or performance.
