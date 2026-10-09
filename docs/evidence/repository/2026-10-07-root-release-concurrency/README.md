# Concurrent release and exhausted-recovery retention

This checkpoint adds tests only. Production sources are unchanged from
`d70b21c5dcb74fc0df4dc0cbf6b31c3b88afd8d8`. Exact test hashes are retained in
`source-sha256.txt`; both runs use PostgreSQL 18. Source-publication provider
observations are fixture supplied, so this is not a provider-performance test.

## Concurrent release

```sh
./gradlew :protomolt-repo-container:test \
  --tests '*DocumentPreparationRootReleaseConcurrencyIT' --max-workers=2 --console=plain
```

Exit 0, Gradle wall time 9 seconds; two tests, no failures/errors/skips.
`concurrency-results.tar.gz` and `concurrency-gradle.log` retain the evidence.

A JDBC before-commit hook holds the first release after its receipt and complete
root deletion are tentative. A separate connection still observes one root and
no committed receipt. A second release must show an actual active claim-lock
wait in pg_stat_activity with the first backend in pg_blocking_pids before the
gate opens. Parallel Java submissions alone do not satisfy this assertion.

When the first transaction commits, both callers return the same receipt. When
it aborts, the second transaction completes the sole release. Both variants end
with one receipt, no roots, unchanged claim lease and zero shared-budget usage.

## Release at actual recovery limits

```sh
./gradlew :protomolt-repo-container:test \
  --tests '*RepositoryHistoricalRecoveryBoundIT' --max-workers=2 --console=plain
```

Exit 0; three tests, no failures/errors/skips. `bound-results.tar.gz` and
`bounds-gradle.log` retain the result and duration. The existing real 16-capture
and 65-installed-edge fixtures now qualify release after the canonical reason-4
decision/rejection, as well as the existing activation bounds.

In each terminal case, release first fails because the original capture has not
drained, even though all recorded later captures have. No roots or release
receipt change. The fixture then closes and quiesces the original reader and
records its exact QUIESCED drain. Release succeeds, includes all 16 capture
identities (capture-limit case) or both identities (ancestry-limit case), and
retries to the same receipt. Claim/owner leases remain unchanged. Cleanup does
not impose the 64-edge execution traversal bound or fabricate an ancestry proof.

An independent reader opened for the refused activation remains in its surrounding
scope during release; its own pin lifecycle remains separate. This run does not
explicitly count that unrelated reader's pins before/after release.

Sol reviewed both additions and found no blocking issue. It confirmed the real
SQL lock observation and the original-capture drain requirement. Public release
remains unmounted. Multi-root partial deletion, capture-versus-release races,
success and other rejection alternatives, actual pruning and restoration remain
separate unfinished acceptance work. These tests do not complete the full goal
or establish hosted CI, merge or deployment.
