# Credential revocation during assessment replay

Base: 5ad0df1ba. Real PostgreSQL and LocalStack. The wrapper revokes the scoped
credential after the real provider returns an assessment batch, before validation
receives the batch. Replay and a subsequent retry with the provider reader closed
must report UNAUTHENTICATED. SQL must contain no rejection and no published revision.

The test checks assessment session cleanup while historical source protection
remains. Final shutdown must release memory. This fixture runs last for that
credential and returns through cleanup before ordinary publication probes.

```sh
./gradlew :protomolt-repo-container:admissionHistoricalRuntimeTest --tests '*HistoricalRuntimeQualificationTest.initialOwner' --max-workers=2 --console=plain
```

Exit 0 in 1m3s: one aggregate, zero failures, errors or skips. The aggregate
requires HISTORICAL_REJECTION_REPLAY_REVOKED_OK. Production code is unchanged.
This checks revocation during replay, before the decision transaction; it does
not establish revocation ordering at the final SQL commit or restart recovery.

Sol completed review with no blocker after rechecking fixture order and cleanup.
