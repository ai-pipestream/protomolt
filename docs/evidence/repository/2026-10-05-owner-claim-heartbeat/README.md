# Claimed owner heartbeat

The owner heartbeat previously renewed only the owner lease, leaving its attached
execution claim to expire. The retained red test shows the unchanged claim
deadline. Renewal now extends both leases in one SQL transaction, preserving
claim-before-owner lock order and immutable ownership identity.

Real PostgreSQL checks cover heartbeat renewal, wrong-owner refusal, an expired
owner with a live claim, and an expired or transferred claim with a live owner.
Existing explicit claim retry, lock-wait and recovery-fence tests also pass.

Validation: `:protomolt-repo-container:test --tests '*RepositoryExecutionClaimLedgerIT'
--tests '*RepositoryOperationRecoveryFenceIT'`: 19 tests, no failures.

This is a prerequisite for durable registration of ordinary runtime sessions.
It does not enable automatic recovery, claim transfer or deployment. The tests
qualify SQL ownership behavior, not provider performance or horizontal scaling.
