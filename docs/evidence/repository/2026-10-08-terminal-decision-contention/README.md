# Competing cancellation transactions

Base: b609f3971. Real PostgreSQL. The test locks the operation claim and starts
concurrent cancellation transactions. A recursive pg_blocking_pids query confirms
both are waiting, including a waiter behind another waiter. The owner remains
available to FOR UPDATE NOWAIT before the claim lock is released.

After release, both transactions return the same terminal observation with a
rejection receipt. SQL contains one rejection for the operation. Subsequent
cancellation also returns that receipt. The original single-waiter regression
and expired-lease replay remain in the same class.

```sh
./gradlew :protomolt-repo-container:test --tests '*RepositoryHistoricalDecisionLockIT' --max-workers=2 --console=plain
```

Final result: exit 0 in 15 seconds, 3 tests, zero failures, errors or skips.
Sol found no blocker; the unused import noted in review was removed before the
final run. XML and source hashes are included. Production code is unchanged.

The source fixture uses synthetic provider observations. These are SQL locking
and receipt tests, not provider durability evidence. Assessment rejection versus
cancellation, revocation during a pending decision and restart recovery remain
separate acceptance work.
