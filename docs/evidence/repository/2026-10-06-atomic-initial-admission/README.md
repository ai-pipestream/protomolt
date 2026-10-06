# Atomic initial journaled admission

The original owner-insertion fault test failed because the execution claim had
already committed. `owner-failure-red.xml.gz` preserves that result. The implementation
now commits claim, coordinator binding, preparation, modes, operation and first owner
in one transaction, using existing SQL guards and shared insertion helpers.

The registration scope spans the whole admission. An early current-authorization
preflight refuses immediately denied callers before the session becomes uncertain;
authorization is checked again under the claim lock. Encoding and hashing occur
before locks, under an aggregate byte reservation. Exact retained-session retries
preserve tokens, seeds and leases. A failure after commit does not imply rollback.

Regression command:

```sh
./gradlew :protomolt-repo-container:test \
  --tests '*DocumentInitialAdmissionIT' --tests '*DocumentJournaledSessionsIT' \
  --tests '*DocumentPublicationSessionIT' --tests '*DocumentPublicationAbandonmentIT' \
  --tests '*RepositoryCoordinator*IT' --tests '*RepositorySuccessor*IT' \
  --tests '*DocumentSuccessor*IT' --tests '*DocumentPublicationProcessRecoveryIT' \
  --console=plain
```

Result: 202 tests across 18 suites passed; build successful in 1 minute 59 seconds.
Compressed XML reports are retained here. Six new real PostgreSQL trigger faults
after the six initial insertions leave every row family absent; retry admits the
same retained identities. Existing lost-acknowledgment tests now assert a complete
owner after the atomic commit. A real terminal rejection/replay verifies that
session eviction does not change the previously captured drain snapshot.

The manager no longer creates partial pre-owner journals as an intermediate state.
Its tests now assert that owner admission blocks pre-owner abandonment and that
rolled-back or uncertain attempts retain capacity until resolved. Legacy behavior
is tested through real standalone preparation/modes APIs, including lost abandonment
commit acknowledgment and confirmation after expiry. SQL guards were not weakened
to reproduce states the atomic manager no longer creates.

Sol reviewed the production changes and test adaptations and found no blocker.
There are no protobuf or migration changes. This is an internal journaled-session
change, not a public RPC or automatic recovery host. Follow-up sections below
qualify death immediately before/after initial COMMIT and count admission commits.
The adjacent `2026-10-06-journaled-rustfs-diagnostic` evidence records a small
latency diagnostic. Expanded admission race coverage and controlled RustFS capacity
measurements remain work. Existing partial registrations still require their own
recovery design.

The production-JAR storage qualification also passed with zero failures/errors/skips:

```sh
./gradlew :protomolt-repo-container:admissionStorageTest --console=plain
```

Build successful in 2 minutes 57 seconds. The runtime inventory contains 38
production artifacts; its compressed XML is retained here.

## Initial-commit process qualification

The subsequent five-case `DocumentPublicationProcessRecoveryIT` run passed in
93.232 seconds (build 1 minute 37 seconds), zero failures/errors/skips. Its output
is preserved separately as `initial-commit-process.xml.gz`.

Two new cases gate the actual JDBC transaction immediately before or after COMMIT,
once the writer connection sees claim, binding, preparation, modes, operation and
owner. Independent parent reads see zero or all six rows respectively, with no
upload attempt, assessment start or terminal outcome. The parent kills and reaps
the writer and checks the same visibility again; a committed claim/owner tuple and
its leases remain unchanged by termination.

A new JVM receives only public command/payload files plus test service configuration.
It publishes as generation one after rollback, or waits for lease expiry and uses
discovery, V97, reserved preparation, V93 and V94 to publish as generation two after
commit. The latter requires fresh attempt/upload tokens relative to retained seeds.
Both cases verify one final attempt, one success receipt, exact selected provider
bytes and replay without storage calls. The three existing provider-write and
unactivated-replacement kill cases also pass. Sol reviewed the extension and its
suggested identity/count assertions were included.

This closes the initial-commit process boundary for the admin/opaque fixture.
It does not establish scoped typed process recovery, automatic recovery hosting,
legacy partial-row recovery, reader-pin reclamation or performance. This follow-up
changes tests and documentation only; the production-JAR result above applies to
unchanged production code.

## Focused transaction count

`DocumentInitialAdmissionIT` now counts actual JDBC commits around the session's
`admit` call, excluding fixture construction and subsequent assessment/publication.
Initial admission and exact retry each commit twice: early authorization preflight
and the atomic journal/owner transaction. The owner identity and lease remain equal
on retry, and byte reservations return to zero. The seven-case focused suite passed
with zero failures/errors/skips; `admission-transaction-count.xml.gz` retains it.
This is a transaction-count assertion, not a latency or full-publication measurement.
