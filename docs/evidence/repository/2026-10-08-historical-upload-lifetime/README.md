# Historical transfer SQL lifetime

Base: 35d572b67. Three real PostgreSQL tests passed with zero failures, errors or
skips. Gradle exited 0 in 15 seconds. XML and the test source hash are adjacent.

```sh
./gradlew :protomolt-repo-container:test --tests '*DocumentHistoricalUploadLifetimeIT' --max-workers=2 --console=plain
```

The tests close a successor's parent execution during the child admission
transaction, with and without cancellation. The child retains registration,
source capture and metadata accounting until the transaction exits. Cancellation
proves failure and cleanup, not rollback of the admission transaction.

The third case blocks the real execution-claim row in a separate transaction.
The coordinator's 200 ms lock timeout terminates child admission with the
PostgreSQL SQLSTATE 55P03 and releases the child reservation. Parent shutdown
then releases the final registration and permits capture drainage.

These are historical-only revisions with no new uploads. Source publication uses
existing SQL fixture observations. No actual provider I/O or provider-worker
lifetime is claimed. Mixed-upload revocation, expiry, late observations and lost
verification acknowledgement remain to be qualified.

Sol reviewed the tests. The final revision replaces message matching with the
specific PostgreSQL SQLSTATE assertion requested during review.
