# Pinned historical source to staged-slot binding

An explicit internal `DocumentAssessmentSlots.prepare` overload accepts already
checked historical reference preparations. It bounds selector count/bytes, requires
the command's complete distinct selector set and account, and borrows the source
Uses. Identical selectors may serve multiple destination members. Extra or missing
selectors fail. The ordinary preparation method still refuses historical execution.

Historical binding requires a transaction-local origin/retention lock proof and
rechecks native retained-source membership. It then compares every physical
coordinate with the command selector, maps each destination member/full ordinal,
and rechecks Use liveness before returning the v2-capable slots. The caller must
retain those Uses through staging/publication and supply operation/policy/current
authorization locks. The binder grants none of those permissions.

Real PostgreSQL tests use one pinned source for two destination members and cover
missing/extra preparation, closed Use, altered physical key, missing destination
slot, omitted retention locks and cancellation. These tests supply a synthetic
`Bound` based on retained fixture identities; they are not provider verification
or end-to-end CREATE evidence. Native reference checks use the actual SQL state.

```sh
./gradlew :protomolt-repo-container:test --tests '*DocumentHistoricalSelectionIT' --tests '*DocumentHistoricalAuthorizationIT' --tests '*DocumentAssessmentSlotsIT' --tests '*DocumentAssessmentSlotSnapshotTest' --console=plain
```

Result: 53 tests, zero failures/errors/skips, 38 seconds. Local log:
`/tmp/protomolt-historical-slot-binding-qualified.log`. Sol reviewed the code and
tests with no material blocker; `git diff --check` passed.

Next: integrate historical objects and the same transaction's lock proof into
`DocumentCommitParts.bindAssessment` and assessment creation. Current destination
WRITE, policy fencing, complete owned inputs/schema evidence, staged replay and
atomic reference publication remain required. Public historical execution is still
gated. No hosted CI, merge or deployment is claimed.
