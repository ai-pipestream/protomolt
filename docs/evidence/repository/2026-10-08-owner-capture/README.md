# Owner capture acquisition

Base: `7967a6ba6`. Source hashes accompany this report. A method Javadoc changed after testing; executable code did not.

```sh
./gradlew :protomolt-repo-container:test --tests '*RepositoryInitialHistoricalAttemptsIT' --max-workers=2 --console=plain
```

Exit 0; BUILD SUCCESSFUL in 19s. Eight PostgreSQL tests passed with zero failures, errors or skips. New cases cover capture acquisition by a reserved owner, cancellation after acquisition, ledger cleanup, retry, duplicate attachment rejection and final drainage. Source publication uses the existing SQL fixture's explicitly supplied provider observations; these cases make no provider durability claim.

Partial captures close on failure and remain in DocumentReadLedger until release succeeds. Managed host routing and shutdown composition remain unfinished.

Sol review found no blocker. Cancellation coverage uses one captured source; failure after multiple distinct captures remains untested by this case.
