# Document source acquisition fence

2026-10-08, based on `211e34ec5`. This checkpoint adds the first SQL acquisition
boundary required by revision pruning. It does not enable pruning or delete data.

V116 requires a transaction-scoped shared document advisory lock before inserting
either CURRENT/HISTORICAL read pins or normalized preparation history roots.
Supported Java paths can reuse their existing ordered locks. Direct SQL paths use
a nonblocking acquisition and fail with SQLSTATE `40001` if an exclusive document
mutation holds the key. They never wait for an earlier lock while holding later
claim, origin or retention locks. Release and deletion triggers are unchanged.

The pre-change test in `red.xml` failed because a historical SQL pin insertion
succeeded while another session owned the exclusive document key. Final verification:

```sh
./gradlew :protomolt-repo-container:test \
  --tests '*DocumentPreparationSourceFenceIT' \
  --tests '*DocumentHistoricalReaderPinsIT' \
  --tests '*DocumentPreparationHistoryRootsIT' \
  --tests '*DocumentPreparationRootReleaseIT' \
  --tests '*DocumentHistoricalReadCaptureIT' \
  --tests '*DocumentCaptureAdmissionClosureIT' \
  --max-workers=2 --console=plain
```

Result: **41 tests, zero failures, errors or skips; build completed in 32 seconds**.
The six final XML files are archived beside this record.

The tests use real PostgreSQL sessions and force both transaction orderings:

- Exclusive document key first: both pin scopes refuse with `40001`, leave no pin
  or mirror, then succeed after the competing transaction releases its key.
- Pin first: the competing exclusive try-lock fails until rollback; pin and mirror
  roll back together.
- Canonical historical preparation first: its root insertion holds the source key
  through commit. After commit the exact normalized coverage remains present.
- Exclusive key before preparation insertion: the registration aborts and rolls
  back claim, coordinator binding, preparation, root header, roots and pin batch.
- Signed UUID halves, negative keys, XOR collisions and unrelated keys agree with
  the existing Java lock-key calculation. Transaction completion releases locks.

Two older migration fixtures initially failed before their migration assertions:
the current reader-registration SQL names `host_execution`, absent in V103/V107.
They now reuse the existing test-only pre-V112 adapter for unbound reader
registration. The adapter is disabled before migration to the current schema;
the V107-to-V108 case remains on the legacy SQL shape. No production compatibility
fallback was added. The migration, capture and release assertions all pass.

These are SQL/lifecycle tests. Their existing fixtures use synthetic verification
observations; this run makes no provider deletion, RustFS performance or end-to-end
pruning claim. The future prune transaction must still check pruned state, current
selection, durable blockers and legacy coverage, and release live references
atomically. Assessment/publication acquisition and exact-version cleanup remain
separate implementation gates. Sol reviewed the migration and test boundaries.
