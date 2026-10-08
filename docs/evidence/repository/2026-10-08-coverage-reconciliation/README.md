# Bounded root-owner coverage reconciliation

Base: `557778cf27624cfeb34a7580da953957d553f9e7`.

Adds private per-record reconciliation and account/principal keyset batches of at
most 64 keys. Each preparation uses a decode budget and explicit SQL timeouts.
Canonical bytes are decoded before taking claim/preparation locks, then immutable
identity is rechecked. Live roots require canonical coverage. Released roots require
the exact terminal receipt and capture-drain evidence. Certification and unresolved
removal share a transaction. No provider operation or pruning is enabled.

Sol reviewed root-owner proof binding, lock order, resource bounds and pagination.
No safety blocker remained. Successor installs remain explicitly unresolved and
need a separate immutable ancestry proof. Batch failure propagates: earlier entries
may have committed, but retrying the same cursor is safe. A persistently corrupt
entry stops that principal's scan; it is never silently skipped. The final whole-
account unresolved lookup is an observation, not permission to prune.

Validation on real PostgreSQL 18 test containers:

```
./gradlew :protomolt-repo-container:test \
 --tests '*DocumentPreparationCoverageReconciliationIT' \
 --tests '*DocumentPreparationCoverageCertificatesIT' \
 --tests '*DocumentPreparationRootReleaseIT' \
 --max-workers=2 --console=plain
```

21 tests, zero failures/errors/skips; 22 seconds. Compressed JUnit XML is retained
alongside this note. The eight new cases cover live legacy certification, release
proof, missing-header retention, caller/process-authority refusal and cancellation
rollback, bounded pages and unresolved keys before the cursor, corrupt preparation
bytes, certified retry after release, and corrupt release receipts. Corruption tests
explicitly alter isolated SQL storage outside the supported immutable writer; this
is fault injection, not production fallback behavior.

Still required: successor lineage proofs, partial-batch/lost-ack and concurrency
qualification, operational integration, and the remaining pruning/cleanup/restore
and RustFS performance work. This is not a full repository-suite, hosted-CI,
main-branch merge or deployment result.
