# Historical fragment preparation

Base: `da1496d2d`. Source hashes accompany this report.

```sh
./gradlew :protomolt-repo-container:test --tests '*DocumentPublicationCommitIT.preparesSharedHistoricalFragmentsOnceAndCleansUpOnCancellation' --max-workers=2 --console=plain
```

Exit 0; BUILD SUCCESSFUL in 14s. One PostgreSQL/LocalStack test passed; zero failures, errors or skips. Coverage: source deduplication, destination copy budgets, invalid mappings before GET, capacity exhaustion and cancellation after provider delivery. Reservations return to baseline. The source owner retains capture protection.

Sol review: no blockers. An initial missing List import was fixed before execution. Managed routing, recovery, shutdown and public library/gRPC qualification remain unfinished.
