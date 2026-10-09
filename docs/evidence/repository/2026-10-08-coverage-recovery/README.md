# Coverage reconciliation commit recovery

Base: `4f4d66bffe0025783df178b82a35212424faabf6`.
Test-only checkpoint, reviewed by Sol. No production behavior changes.

```
./gradlew :protomolt-repo-container:test --tests '*DocumentPreparationCoverageRecoveryIT' --max-workers=2 --console=plain
```

4 tests pass, zero failures/errors/skips, 13 seconds elapsed. PostgreSQL cases
cover LIVE_ROOTS and successor lineage certification:

- A fault after actual JDBC commit interrupts the reply once the exact proof is
  visible from another connection. Retry returns ALREADY_* with one durable proof,
  no unresolved marker and no budget leak.
- A barrier delays commit after proof insertion. pg_blocking_pids confirms that
  a competing reconciler waits on the execution-claim lock. Before commit, another
  connection sees unresolved coverage and no proof. After commit, callers return
  VERIFIED_* and ALREADY_*, with one proof, no unresolved marker and budgets released.

An initial fixture error passed a null Flyway target; the fixture now specifies
latest. No production fix was needed. Faults and barriers wrap actual database
operations. These tests do not qualify object-store performance, RELEASE_RECEIPT
reply interruption, abort-before-commit races, pruning, hosted CI or deployment.

The external assignment `agent/repository-coverage-qualification` covers release
orderings, released anchors, depth boundaries and partial batches. That branch
starts before this checkpoint; integration must retain all acceptance coverage.
