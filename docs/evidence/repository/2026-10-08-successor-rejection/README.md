# Successor validation rejection

Base: 93107a5f0. Real PostgreSQL and LocalStack. The existing successor installation
fixture waits for predecessor expiry, captures historical sources again, and
uploads a new part under the successor attempt. A validation failure in the fresh
part produces a rejection after successful assessment CREATE.

Assertions cover the successor generation, command hash, assessment identity and
manifest hash; one durable rejection; zero published revisions; receipt replay
with the provider reader closed; assessment session cleanup; normal terminal
retirement; memory cleanup; and receipt preservation after retirement.

```sh
./gradlew :protomolt-repo-container:admissionHistoricalRuntimeTest --tests '*HistoricalRuntimeQualificationTest.initialOwner' --max-workers=2 --console=plain
```

Gradle exit 0 in 1m2s: one aggregate test, zero failures, errors or skips.
The aggregate requires HISTORICAL_SUCCESSOR_REJECTION_OK alongside initial-owner
checks. The existing full storage gate invokes this host too. This checkpoint did
not rerun that full gate. Production code is unchanged.

This case covers an installed successor in the same process. Restart recovery,
rejection races, revocation during rejection and public routing remain unfinished.

Sol reviewed the implementation and found no blocker. Successor-specific commit
reply loss and replay through a later client call remain separate test cases.
