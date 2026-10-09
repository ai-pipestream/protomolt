# Publication and grant revocation races

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
remained open at that checkpoint. The archive contains XML and binary test results. This is local
correctness evidence, not a throughput or latency benchmark.

## Revocation-first final authorization

Base: `f2aabbc990122349662a75d3d2a6d4f75f24e2ce`.

```sh
./gradlew :protomolt-repo-container:test --tests '*DocumentPublicationCommitIT.finalPublicationRechecksGrantAfterConcurrentRevokerCommits' --console=plain
```

Both cases passed with no skips. After real LocalStack uploads and content checks,
a separate PostgreSQL transaction holds the exact grant row. The final publisher
waits in `lock_repository_creation_grant`, verified against the blocking backend
PID. Committing revocation refuses publication: the destination remains absent,
no success record exists, and provider-versioned attempt rows remain available
for recovery. The control holds the same row without revoking and publication
succeeds after release.

Sol reviewed the test and synchronization with no blocker. This qualifies the
opaque direct final-commit boundary, not runtime retry or cleanup. Final-check
expiry remains open. `revocation-first-green.tar.gz` contains the two-case XML
and binary results.

## Expiry during the final grant check

Base: `728da6ad65645d01b3b297b1fc09510e7090cfe8`. The same command above now
runs three named cases: live, revoked and expired. All three passed with no skips.
The expiry case uses PostgreSQL's clock to set an immutable grant deadline. After
real uploads, it proves the final publisher is blocked while the grant is still
live, waits until that database clock passes the deadline, then releases the row.
The publisher refuses the expired grant with no destination or success record.
This checks expiry after waiting, rather than a timestamp sampled before the lock.
`final-expiry-green.tar.gz` retains the three-case XML and binary results.
Sol found no correctness blocker. The test requires staging and reaching the
observed lock wait within the 15-second grant lifetime; an overloaded test host
can fail that pre-expiry assertion. No clock is mocked or grant deadline rewritten.
