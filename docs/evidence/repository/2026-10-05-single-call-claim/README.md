# One client call for an execution claim fence

Validation:

```
./gradlew :protomolt-repo-container:test --tests '*RepositoryExecutionClaimLedgerIT' --tests '*RepositoryClaimMutationFenceIT' --console=plain
```

All 20 real PostgreSQL cases passed (9 claim, 11 mutation-fence), with no failures,
errors or skips. BUILD SUCCESSFUL in 25s. This includes exact-PID lock waits,
expiry, transfer, wrong command digest, caught-fence rollback, consumed-proof
bypass prevention, finalization and existing-row migration cases.

The new Hibernate statistics test observes one prepared statement for the fence
and the exact unchanged lease. V80 executes the isolation check, row lock,
after-wait identity/time checks and consumed-proof update inside one database
function. It preserves V78/V79 triggers and subsequent mutation-time checks.
This proves one client SQL call, not one server statement or a measured speedup.
No RustFS latency or capacity test was run for this checkpoint.

Sol reviewed the function, Java call and test without a blocker. Complete session
persistence, activation and crash recovery remain unfinished. No new public API
or automatic failover is enabled.
