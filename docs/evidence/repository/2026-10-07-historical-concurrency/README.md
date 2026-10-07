# Historical activation contention

Base: `ee9d329938e65efc7bc766cc707ea32009523bbd`, plus the test fingerprint
in this directory. Production sources are unchanged.

Two separately captured historical readers, with distinct pin digests, compete
for the same installed successor. A JDBC commit gate holds the first transaction
after its V109 insertion. The second attempts V94 insertion; PostgreSQL's
`pg_blocking_pids` identifies the first connection as its blocker. Releasing the
gate commits the first transaction; the second receives a duplicate-key failure
before registering a capture. Assertions verify exactly one V94 and V109 row,
two total batches (original and winner), independent confirmation bound to the
winning capture, exact retry, successful drain and unchanged leases.

The original test failed because its shared 64 MiB fixture budget could not admit
two activation reservations. This test now provides a shared 128 MiB budget for
the two attempts and surfaces any early competitor failure directly. Production
reservation limits are unchanged. The single test passed, then the final version
with exact backend blocking attribution passed with the related regressions.

Final command exited 0 in 38 seconds: 19 tests, zero failures, errors or skips.

```sh
./gradlew :protomolt-repo-container:test \
  --tests '*RepositoryHistoricalActivationConcurrencyIT' \
  --tests '*RepositoryHistoricalActivationEvidenceIT' \
  --tests '*RepositoryHistoricalSuccessorActivationIT' \
  --tests '*ScopedHistoricalSuccessorActivationIT' \
  --max-workers=2 --console=plain
```

The four XML reports are archived in `focused-results.tar.gz`. Sol reviewed the
final test with no blocker. This is actual PostgreSQL 18 contention, with synthetic
source-publication provider observations. It does not qualify provider concurrency,
a rolling-back winner, process restart, throughput, public historical execution,
ancestry limits or retention release.
