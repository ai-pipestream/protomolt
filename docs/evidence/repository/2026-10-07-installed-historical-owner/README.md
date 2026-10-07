# Private installed historical owner

Base: `3f9bf4865` on `refactor/repository-composition`, plus the owner/test sources
whose exact SHA-256 values are recorded beside this file. Sol reviewed the ownership,
accounting and cancellation boundaries; no safety blocker remained for this private
checkpoint. This is not a public historical publication release.

## Executed gate

```sh
./gradlew :protomolt-repo-container:test \
  --tests '*RepositoryInstalledHistoricalAttemptsIT' \
  --tests '*RepositoryHistoricalCaptureDisposalIT' \
  --tests '*RepositoryHistoricalSuccessorActivationIT' \
  --max-workers=2 --console=plain
```

Exit 0, BUILD SUCCESSFUL in 49 seconds. JUnit: 28 tests, zero failures/errors/skips:
7 installed-owner tests, 9 disposal tests, 12 activation tests. Reports and Gradle
output are archived as gzip files. PostgreSQL is the real postgres:18-alpine container.
The shared historical source fixture uses synthetic provider observations; these
results do not establish object-provider transfer/durability or public RPC behavior.

The first four-test run failed because the new identity test attempted an invalid
process-authority caller with enumerated grants. The fixture now uses a valid scoped
caller with the same principal/account and checks that the identity change is refused.
No production rule was weakened. Review also corrected duplicate retry reservations
and cancellation-insensitive waits before this final run.

## What the tests establish

- Same-key exclusion, exact caller binding and retry under a fully reserved byte budget.
- Byte-capacity rejection leaves no owner entry or activation SQL.
- Failed source transfer retains caller ownership; successful transfer retains the
  exact Work/history until workers finish. No other history or reader is fenced.
- Shutdown cancellation retains unresolved ownership and can resume cleanup.
- Execution and acknowledged START survive distinct client calls, whose runtime
  barrier becomes idle between requests. Closed source admission is not reopened.
- Real JDBC commit-reply failure and rollback preserve a retained entry. A committed
  activation resumes without a duplicate pin batch; disposal emits a drain receipt
  only for actual registration. Budgets return after successful disposal.

## Outstanding qualification

The new owner is not wired into managed routing. Retained assessment/CREATE/publication
across client calls needs a real-provider probe. Exact lost-CREATE stage reconciliation,
normal terminal retirement, pre-install ownership and library/gRPC qualification remain.
Initial fingerprint encoding uses a conservative ~49 MiB temporary reservation under
the map monitor; concurrent latency is not qualified. Entries currently retire only
on shutdown. Existing packaged probes have not been rerun for this checkpoint; the
focused results above must not be described as a full repository gate or hosted CI.
