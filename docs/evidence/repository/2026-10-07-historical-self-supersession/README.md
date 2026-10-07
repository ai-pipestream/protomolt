# Historical owner self-supersession

Base: `3dbbd88cc39b6553f84f9b8006d86e6111ef46ae`. The source hashes in this
directory identify the final implementation and preparation test. Sol reviewed
production changes, the V98 SQL binding, and the new tests. No protobuf or migration
changed; the public historical gate remains closed.

The same retained entry can replace its own expired, unactivated claim. Before V98,
it checks complete submitted payloads, caller authorization, original historical
retention and fixed modes. A pending replacement survives lost replies and blocks
advance, plan delivery and source attachment. Retirement must prove both retained
claims fenced. Confirmation adopts the same pending identity and releases obsolete
preparation metadata. Sources or execution already attached prohibit replacement.

## Commands and outcomes

Both commands used Java 25 and real PostgreSQL 18 test containers. Archived source
fixtures use explicitly synthetic provider observations; no actual provider-write or
packaged-host qualification is claimed for this transition.

```
./gradlew :protomolt-repo-container:test \
  --tests '*RepositoryHistoricalPreparationIT' \
  --tests '*RepositoryInstalledHistoricalAttemptsIT' \
  --tests '*RepositoryHistoricalAttemptRetirementIT' \
  --tests '*RepositoryHistoricalSuccessorActivationIT' \
  --tests '*RepositoryHistoricalCaptureDisposalIT' \
  --max-workers=2 --console=plain
```

Exit 0, BUILD SUCCESSFUL in 1m55s. 55 tests, zero failures/errors/skips:
17 preparation, 7 installed-owner, 10 retirement, 12 activation, 9 disposal.
Production sources are identical to the final source hashes. Only the additional
shutdown parameter below changed afterward. Reports have the `regression-` prefix.

```
./gradlew :protomolt-repo-container:test \
  --tests '*RepositoryHistoricalPreparationIT' --max-workers=2 --console=plain
```

Exit 0, BUILD SUCCESSFUL in 1m22s. Final preparation class: 18 tests, zero
failures/errors/skips, XML time 78.772 seconds. This adds one shutdown case to the
previous 17. Together the runs cover 56 distinct cases, not 73 distinct cases.

New cases cover:

- Committed V97 with lost reply, normal RESERVED, normal INSTALLED, and committed
  V93 with lost reply, followed by actual lease expiry and same-owner replacement.
- An actual after-commit JDBC exception and interrupted confirmation after V98.
  Retry retains the exact replacement token/incarnation and creates one V98 row.
- Pending replacement blocks advancement, plan delivery and actual capture transfer;
  refused transfer leaves the caller's Work borrowable. Old-claim fencing alone
  cannot retire the pending current claim or release its metadata budget.
- Wrong modes and extra payload keys produce zero V98 writes.
- Two foreign-successor cases preserve the retained owner's budget and durable
  foreign identity while refusing replacement.
- Existing fresh-owner cases now also refuse self-supersession after attachment,
  then still activate and START successfully.
- Shutdown after uncertain V98 releases all local metadata bytes while preserving
  the committed claim and reservation. No capture-drain or historical-activation
  row appears; the original reader remains held.

## Limits and next work

The preparation loader requires a live lease. Expired discovery is instead bound by
V98's atomic exact claim, owner, preparation and mode checks. The initial review
caught this distinction before qualification; the live loader was not weakened.

No new packaged provider-host scenario was run for self-supersession. That scenario
must carry this same owner through replacement, capture, publication and byte/receipt
readback. The earlier packaged-host checkpoint at the base is not evidence for this
new transition. Attached V94 recovery, public library/gRPC routing and the preflight
revocation boundary remain separate work. These passing tests do not complete the
repository goal or establish pruning, JCR compliance or horizontal scalability.
