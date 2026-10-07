# Private recovery-limit decision handler

Source base: `c5a4db5cc407377d39e5851f2f4c22e41bc73201`. The implementation
and tests were qualified as working-tree changes on that base. The accompanying
`source-sha256.txt` records the exact tested files; the commit containing this
evidence is the implementation checkpoint. No protobuf or migration changed.

`RepositoryHistoricalLimitDecisions` is package-private and unmounted. It checks
the exact installed plan, locks claim before owner, rechecks authorization and
records a canonical rejection with its V110 evidence in one transaction. It
does not activate execution, renew leases, drain captures, release roots or call
a storage provider. A failed database operation is propagated, not converted
into a limit decision. The caller can retry using the same immutable identity.

## Executed checks

Both commands ran in `delegation-boundaries`, using real PostgreSQL 18:

```sh
./gradlew :protomolt-repo-container:test \
  --tests '*RepositoryHistoricalLimitDecisionsIT' \
  --tests '*RepositoryHistoricalRecoveryBoundIT' \
  --max-workers=2 --console=plain

./gradlew :protomolt-repo-container:test \
  --tests '*RepositoryHistoricalLimitDecisionsIT' \
  --max-workers=2 --console=plain
```

The first command passed five tests in 1m53s: three bound cases and the original
two fault cases. `bound-results.tar.gz` preserves its three-case bound XML;
`bounds-gradle.log` preserves the build output. The production handler and bound
test did not change afterward.

The second command followed additional lease assertions and the scoped replay
test. It passed three tests, zero failures/errors/skips, in 40s (JUnit 37.648s).
`handler-results.tar.gz` and `handler-gradle.log` preserve this final focused run.
Together these archives cover six distinct cases; the earlier two fault cases
overlap the final run and must not be counted twice.

The bound tests exercise the handler at actual capture exhaustion (16 retained
batches) and ancestry exhaustion (65 installed links). Under-bound input returns
no decision. SQL guards still reject invalid pair construction and late activation.

The fault tests inject a database failure immediately before commit and a lost
acknowledgement immediately after actual commit. They assert respectively zero
or one durable pair, then retry and confirm exactly one receipt. Changed retained
identity is rejected. Claim/owner leases are unchanged and the payload budget is
released. No execution or capture-drain row is created by the decision.

The scoped test registers a real credential binding, commits a decision, removes
source READ permission and observes NOT_FOUND on replay. Restoring permission
returns the same receipt. Revoking that credential then yields UNAUTHENTICATED;
the existing rejection remains durable.

Sol reviewed the private handler and final test assertions and found no blocker.
This is local validation and review, not hosted CI, merge or deployment evidence.

## Limits and next gates

Provider observations in these fixtures are synthetic; the SQL transactions and
faults are real. These tests make no provider durability or performance claim.
Handler cancellation around commit, competing decisions/activation, corrupted
evidence, expired-lease replay and creation-grant revocation still need explicit
qualification. No public historical recovery session or source-root release is
enabled. V110 liveness remains guard-time validation, as documented in the
[timing evidence](../2026-10-07-recovery-limit-timing/README.md).
