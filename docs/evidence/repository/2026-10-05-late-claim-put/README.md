# Late PUT after execution claim transfer

Local PostgreSQL/LocalStack correctness qualification on 2026-10-05, based on
`26cb954b` plus this test-only change. Sol reviewed the test without a blocker.

```sh
./gradlew :protomolt-repo-container:test --tests '*DocumentSelectedTransferIT' --console=plain
```

All five cases passed. The new case holds a real S3 adapter PUT before invoking
the adapter, waits for execution-claim expiry using database time, transfers to a
new epoch/token, expires the attempt, and runs exact-key cleanup while PUT remains
held. Cleanup observes absence. Releasing the PUT creates real provider bytes;
the old writer's verification fails with the execution claim fence. No part is
marked verified and the publication stays pending.

The attempt remains discoverable by the next cleanup scan, which removes the
late bytes. A distinct neighboring key's exact version remains readable. This
neighbor is a real provider object, not a fixture claiming published repository
ownership. The test uses the claim-only low-level operation primitive, not V81
session restoration or successor-owner publication.

The four earlier real-provider cases continue passing. No production code change
was needed for this interleaving. This is not a proof of coordinator takeover,
finite cleanup completion time, all-provider behavior, or RustFS performance.
