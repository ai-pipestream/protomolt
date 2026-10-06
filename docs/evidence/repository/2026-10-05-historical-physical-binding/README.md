# Historical physical binding with transaction proof

The internal historical plan retains borrowed source reference preparations and
checks their complete distinct selector set and account. This shared check also
serves slot preparation. Historical source revisions do not enter current-source
CAS. Normal plan, assessment and publication entry points still refuse execution.

`DocumentCommitParts.bindHistoricalAssessment` uses the shared physical binder:
historical object IDs are distinct from new-upload attempts; origin and retention
locks cover the complete plan; native historical references are rechecked while
source Uses remain live; actual ledger coordinates must match the selector. The
result includes the same transaction's lock proof for subsequent slot binding.

Real PostgreSQL tests bind r1 after r3 becomes current alongside current reuse or
a newly verified upload. The latter uses explicitly synthetic provider observations
through the selected-attempt ledger. Unlike the earlier slot test, the physical
map comes from the actual shared SQL binder. Both paths bind into assessment slots,
refuse proof reuse in another transaction, keep ordinary paths unsupported, and
refuse a closed source Use.

The fixture intentionally admits an ordinary operation for its physical selection
rows. It does not establish exact historical command admission, CREATE, provider
bytes or a completed restore. Creation must preserve the operation ledger command
equality check, caller authorization, policy fence and retained schema evidence.

```sh
./gradlew :protomolt-repo-container:test --tests '*DocumentHistorical*IT' --tests '*DocumentAssessmentSlotsIT' --tests '*DocumentAtomicPublicationIT' --tests '*DocumentOperationUploadAdmissionIT' --tests '*DocumentUploadPlanTest' --console=plain
```

Result: 254 tests, zero failures/errors/skips, 59 seconds. Local log:
`/tmp/protomolt-historical-physical-binding-qualified.log`. Sol reviewed locking,
identity, lifetime and activation boundaries with no material blocker.
`git diff --check` passed. No hosted CI, merge or deployment is claimed.

The physical and slot binders currently repeat source-reference verification.
This is bounded database work, with no provider I/O under locks. Consolidating it
requires transaction-scoped checked-reference evidence; no latency improvement or
scale-out result is claimed by this checkpoint.
