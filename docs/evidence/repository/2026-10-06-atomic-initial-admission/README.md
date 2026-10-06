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
change, not a public RPC or automatic recovery host. The process tests above kill
writers after provider writes and replacements during takeover; they do not qualify
death immediately before/after initial COMMIT. That test, expanded admission race
coverage, transaction counts and RustFS latency measurements remain work. Existing
partial registrations still require their own recovery design.

The production-JAR storage qualification also passed with zero failures/errors/skips:

```sh
./gradlew :protomolt-repo-container:admissionStorageTest --console=plain
```

Build successful in 2 minutes 57 seconds. The runtime inventory contains 38
production artifacts; its compressed XML is retained here.
