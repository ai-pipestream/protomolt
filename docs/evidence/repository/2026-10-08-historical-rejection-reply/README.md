# Historical rejection commit reply loss

Base: aebfedb1a. Real PostgreSQL and LocalStack. A mixed historical/upload
candidate fails validation. CREATE succeeds before rejection fault injection.
The JDBC wrapper selects the rejection transaction by account, principal,
operation, owner generation, assessment and creation_xid. After commit, the
wrapper throws SQLSTATE 08006. The request reports that failure.

An independent transaction confirms one durable rejection. With the provider
reader closed, retry returns the same receipt and original START assessment.
Assertions cover zero published revisions, assessment session cleanup, continued
historical source retention, replay after lease expiry and memory cleanup.

```sh
./gradlew :protomolt-repo-container:admissionHistoricalRuntimeTest --tests '*HistoricalRuntimeQualificationTest.initialOwner' --max-workers=2 --console=plain
```

Gradle exit 0 in 52 seconds: one aggregate test, zero failures, errors or skips.
The aggregate requires HISTORICAL_INITIAL_REJECTION_REPLY_LOST_OK and the existing
initial-owner markers. XML and source hashes are included. Sol found no blocker.
Production code is unchanged. Restart recovery, successor rejection and public
historical routing remain unqualified.
