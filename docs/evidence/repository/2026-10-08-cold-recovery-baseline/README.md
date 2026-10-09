# Restart recovery baseline

Tested production/test source: a8ca11413. Documentation-only commit 69dfa69c2
was made while the test ran; no compiled source changed during execution.

```sh
./gradlew :protomolt-repo-container:test --tests '*RepositoryHistoricalRetentionLoaderIT' --tests '*RepositoryHistoricalPreparationIT' --max-workers=2 --console=plain
```

Exit 0 in 5m51s: 17 retention-loader tests and 18 preparation tests, zero failures,
errors or skips. The suites use real PostgreSQL with synthetic provider observations.
The runtime includes natural lease-expiry waits. A live thread sample confirmed
RepositoryHistoricalRetentionLoaderIT.waitExpired executing PostgreSQL pg_sleep;
this duration is not a repository latency benchmark.

These tests qualify metadata ancestry and existing preparation behavior. They do
not establish fresh-process execution. Production code has no call site for the
retention loader. The reviewed design requires a registry-owned cold proposal,
reservation before predecessor/anchor loading, verified modes before reservation,
then fresh source capture and activation before execution.

Implementation ownership: the preparation object owns the loaded anchor and memory
lease. Registry operations use a checked accessor after resolution. Supersession
must release the predecessor metadata, anchor, digest and plan together. A cold
retry must not enter the warm path or infer authority from missing metadata.
Sol reviewed this representation and the design sequence without a blocker.
