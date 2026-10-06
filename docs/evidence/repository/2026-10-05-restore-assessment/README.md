# Private typed restore assessment

`PinnedHistory.assessRestore` now prepares one destination member from one exact
typed historical source. The package-private result is not a restore endpoint or
publication grant. Source selectors are checked under current READ against the
pinned revision, including full ordinals, slots and physical object coordinates.
Mixed upload/current-reuse content is explicitly unsupported in this preparation
step. Opaque revisions remain unsupported by typed historical schema capture.

The result owns reserved fragment copies, assessment assets and an independent
read-pin Use. Input buffers may be released after capture; provider work must still
drain under its own lifetime. SQL snapshot and resolver scratch reservations end
only after the assessment owns its copies. Closing the outer history cannot release
the result's pin. Each new result view rechecks current source READ; a previously
borrowed view is not an independently reauthorized handle.

Assessment uses the supplied current policy, new command digest and evaluation time,
with exact retained definitions rather than a registry lookup or historical verdict.
The host must still authorize the destination, select and fence the authoritative
policy, bind the full executable command, and atomically publish references.

Validation:

```
./gradlew :protomolt-repo-container:test \
  --tests '*DocumentHistoricalRestoreAssessmentIT' \
  --tests '*DocumentHistoricalSelectionIT' \
  --tests '*DocumentHistoricalMaterializationIT' \
  --tests '*DocumentHistoricalValidationBudgetIT' --console=plain
```

27 tests passed, no failures/errors/skips, in 20 seconds. The new eight cases use
real PostgreSQL and retained descriptor bytes. Provider observations are the existing
explicit retention fixture, not live object-store performance evidence. Coverage
includes private-copy ownership, pin drain, policy refusal, fragment mismatch,
capacity/cancellation, selector substitution, and scoped revocation both after and
during assessment. Revocation suppresses successful results and policy details.

Sol identified a missing comparison of SQL root identity columns. The fix checks
stored fragment size/hash against the original command and selected physical part,
and locator hash against canonical encoded evidence. Corruption tests alter real
captured snapshots in memory; they do not corrupt SQL or disable immutable-history
guards. Sol's final review found no remaining material blocker in this prerequisite.

No public restore activation, hosted CI, merge or deployment is established here.
