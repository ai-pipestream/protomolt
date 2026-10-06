# Publication-first grant revocation race

Base: `f6f4d740f4aef3ee694956d65d5c3bf6fb5dd259`.

```sh
./gradlew :protomolt-repo-container:test --tests '*DocumentPublicationCommitIT.scopedJournaledPublicationUsesRealProviderAndKeepsReceiptAfterGrantRevocation' --console=plain
```

Both typed and opaque cases passed with no skips. PostgreSQL 18 and LocalStack
perform the actual admission, provider upload, validation and publication. A JDBC
barrier pauses before commit only after the publisher's own connection sees the
exact operation's newly inserted success record. A separate transaction invokes
real grant revocation. The test checks `pg_blocking_pids`, the grant UPDATE query
and lock wait state to establish that revocation waits on this publisher.

The test then releases commit and joins both tasks. It reads back exact provider
fragment bytes, validates retained typed data, and checks that grant revocation
does not erase committed receipt access while credential revocation refuses it.
The barrier releases in failure cleanup; every other JDBC operation delegates to
the real database. No success-shaped backend or response mock is used.

Sol reviewed the synchronization and found no blocker. This proves publication-first
ordering, not revocation-first refusal or expiry during final commit. Those cases
remain open. The archive contains XML and binary test results. This is local
correctness evidence, not a throughput or latency benchmark.
