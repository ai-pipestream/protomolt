# Historical CREATE ownership across handles

Base: `66e33b7b127b26ae46eb52f3492431769ecf5acb`. This draft separates V83
coordinate recovery from private CREATE permission. No wire contracts or migration
are changed. It is not complete recovery or an available public restore API.

`DocumentAssessmentStartJournal` returns the actual INSERT outcome internally.
`DocumentHistoricalExecution` grants private CREATE permission only after that
transaction and postcommit authorization return successfully. A handle that loads
an existing start can recover coordinates but cannot stage schema claims or CREATE
an assessment, even if it prepares its own valid assessment. The prior per-handle
CREATE-SQL attempt marker remains sticky.

Readback validates the historical binding and post-lock expiry on both insertion
and loading. It rejects impossible insertion counts and newly inserted IDs that
differ from the proposed ID. Ordinary start retains its coordinate-only behavior.
Sol reviewed these production changes without finding a regression.

## Verified runs

- `initial/`: 16 focused historical execution/expiry tests, then the full production
  runtime storage driver; successful in 8m30s. Includes separate-handle CREATE
  refusal before schema claims, plus existing restart/cleanup cases.
- `readback-hardening/`: the same focused classes passed in 27 seconds after the
  readback changes. Final XML, compressed log and source identities are retained.
- `start-fault-compile/`: preserved failure from a wrong package name in the new
  test helper. Corrected before the next run; no production failure was concealed.
- `start-faults/`: full driver passed in 7m52s. A fault around actual JDBC commit
  selects only the exact scoped V83 start with the current `started_xid`. Rollback
  leaves no start, an acknowledged retry gets a new UUID and may CREATE. Lost ACK
  preserves the original UUID, but both original and new handles refuse CREATE
  before schema claims; discovery returns no assessment. Sol reviewed the cases.

Commands:

```sh
./gradlew :protomolt-repo-container:test \
  --tests '*DocumentHistoricalExecutionIT' --tests '*DocumentHistoricalStartExpiryWaitIT' \
  --max-workers=2 --console=plain
./gradlew :protomolt-repo-container:admissionStorageTest --max-workers=2 --console=plain
```

## Final qualification

The concurrent-handle CREATE driver passed in 7m59s; `concurrent/` retains its
XML, compressed log and source identities. It qualifies competing invocation and
single-winner permission, not observed PostgreSQL lock contention.

The only subsequent production change removed unused extra columns accidentally
selected by ordinary `load()`, restoring its original projection. Final focused
historical execution, expiry-wait and ordinary start-journal tests passed in 37s:
25 tests, 0 failures, 0 errors, 0 skips.
Reports, log and final production hashes are in `final-focused/`. The full runtime
result precedes that projection-only cleanup; it is not an exact-final-source
full-driver rerun. Sol reviewed the complete ownership diff and approved this
private slice conditional on that focused pass, now satisfied.

If the sole acknowledged handle disappears before CREATE, another handle cannot
restage in the same generation. Explicit abandonment/drain or a qualified successor
with fresh source capture must resolve that case as part of the full goal; this
private gate is not a substitute for recovery. Successor execution remains planned work, separately specified in the design.
