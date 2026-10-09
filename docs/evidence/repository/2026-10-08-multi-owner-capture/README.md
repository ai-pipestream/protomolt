# Multiple source capture failures

Base: `f8d82559c`. Sol reviewed the added coverage without blockers.

```sh
./gradlew :protomolt-repo-container:test --tests '*DocumentHistoricalMultiRevisionPublicationIT' --tests '*RepositoryInitialHistoricalAttemptsIT' --max-workers=2 --console=plain
```

Exit 0; BUILD SUCCESSFUL in 26s. Ten tests passed, zero failures, errors or skips. The multi-revision fixture now adds a repeated old-source destination, cancels after each acquired source, drains every failed handle, and retries the same reserved owner. Distinct historical revisions require separate captures; repeated destinations share a capture. Current reuse does not acquire a historical capture. Detaching the unregistered owner returns the ledger and byte budget to baseline.

These are PostgreSQL lifecycle tests with explicitly supplied provider observations. They do not qualify provider I/O or managed host routing.
