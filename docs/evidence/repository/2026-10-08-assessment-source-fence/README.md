# Assessment source acquisition fence

2026-10-08, based on `6269d24ad`. V117 adds a nonblocking source-document fence
before assessment source-slot insertion, for both REUSE and HISTORICAL_REUSE.
The immutable revision supplies the node identity; REUSE does not carry one.
The existing slot shape, complete-set sealing and physical binding checks remain.

The four pre-change tests in `red.xml` failed: exclusive document ownership did not
prevent staging, and an open source-slot transaction did not exclude a competing
exclusive owner. With V117, contention raises SQLSTATE `40001` and rolls back the
assessment owner, slots, object references and mirrors. Retry after the competing
transaction ends succeeds. Acquisition-first holds the source fence through commit.

Verification used real independent PostgreSQL sessions:

```sh
./gradlew :protomolt-repo-container:test \
  --tests '*DocumentAssessmentSourceFenceIT' \
  --tests '*DocumentHistoricalSelectionIT' \
  --tests '*DocumentAssessmentRetentionIT' \
  --tests '*DocumentHistoricalReaderPinsIT' \
  --max-workers=2 --console=plain
```

**61 tests passed; zero failures, errors or skips. Build completed in 41 seconds.**
Final XML results are archived beside this record. Sol reviewed the migration and
tests with no blockers.

The shared fixtures contain explicitly synthetic provider observations and SQL
assessment declarations. These tests establish database serialization and retention
behavior, not provider verification, pruning, deletion, throughput or a deployment.
No prune result or deletion permission is enabled. Canonical preparation coverage
certification, a pruned-state check and atomic reference release remain necessary.
