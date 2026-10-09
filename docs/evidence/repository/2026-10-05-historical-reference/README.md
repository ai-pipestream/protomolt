# Historical reference transaction checks

`DocumentHistoricalReferenceAdmission` prepares bounded SQL claims from exact
selectors under an existing historical Use. Preparation checks source READ and
pinned membership, then encodes batches of at most 256 claims outside write locks.
It borrows the Use; the owner must retain it through reference publication.

The transaction check requires an active Use and `IndependentOrigins` evidence for
the same EntityManager and PostgreSQL transaction, with retention locks acquired
and every selected object included in the proposed set. Its query checks the exact
native revision UUID, publication revision, full manifest ordinal, slot, successful
sealed commit, verified original physical coordinates and DOCUMENT_HISTORY reference.
It does not require that historical revision to be current. No provider I/O or
schema decoding occurs in the transaction check. Failure marks the transaction
rollback-only, even when the caller catches the exception.

This private helper is not publication authority. The eventual writer must fence
the operation and active policy, authorize destination WRITE and historical source
READ under the complete deterministic logical lock set, then acquire drive/origin/
retention locks before checking references and publishing atomically. Current
authorization preparation does not yet include historical sources. The public
historical-reuse command guard remains enabled.

Validation:

```
./gradlew :protomolt-repo-container:test \
  --tests '*DocumentHistoricalSelectionIT' \
  --tests '*DocumentAtomicPublicationIT' \
  --tests '*DocumentReuseAdmissionIT' \
  --tests '*DocumentHistoricalRestoreAssessmentIT' --console=plain
```

All 102 tests passed with no failures, errors or skips in 31 seconds. The new checks accept exact r1 sources
while r3 is current and reject missing retention locks, omitted proposed objects,
locks from a prior transaction and a closed Use. Existing atomic publication,
current-reuse and restore-assessment regressions also pass. Fixtures use explicit
synthetic provider observations; this is SQL evidence, not object-store performance
or a completed restore. Sol reviewed the query and lock/lifetime boundary.

No hosted CI, merge, deployment or public restore availability is claimed.
