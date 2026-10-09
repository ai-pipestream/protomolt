# Caller-owned upload transactions

Base: `310a85ff5`. Source hashes accompany this report. Sol review found no blockers.

```sh
./gradlew :protomolt-repo-container:test --tests '*DocumentSelectedAttemptIT' --tests '*DocumentUploadCoordinatorIT' --max-workers=2 --console=plain
```

Exit 0; BUILD SUCCESSFUL in 47s. Selected-attempt tests: 26 passed. Upload coordinator tests: 39 passed. Zero failures, errors or skips.

The extracted EntityManager operations share the caller transaction. Tests verify atomic commit/rollback of observations and claim, owner and attempt leases. Existing wrappers retain bounded encoding before SQL and original mutation order. SQL observation fixtures are synthetic; the coordinator suite exercises real provider adapters.

The first compilation exposed ambiguous Tx lambda overloads; explicit return blocks fixed them. Historical transfer authority and coordinator integration remain unfinished.
