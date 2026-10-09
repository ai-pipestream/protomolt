# Private preparation-root release handler

`DocumentPreparationRootReleases` combines canonical terminal validation,
live selector/capture checks and V111 atomic release in one bounded transaction.
Process authority is required. It neither renews leases nor accesses providers.
Its release inspection distinguishes UNKNOWN, LIVE_EXACT and RELEASED_EXACT;
the existing history-root EXACT proof remains limited to live execution.

An exact retry checks the canonical header, immutable receipt and terminal
identity, zero remaining roots, and capture count/fingerprint. It does not query
source publications or parts. This is confirmation of immutable release evidence,
not a new audit of each child pin after privileged mutation: V111 verified child
digests and drainage when inserting the receipt, and SQL guards preserve them.

The handler reserves 3 * preparation maximum bytes + 1 MiB before encoding and
retains that reservation through commit. Budget exhaustion fails without waiting
or deleting roots. Source-selector preparation happens before SQL locks, and
source-table validation is executed only for fresh live release.

## Evidence

Tested changes on `1e9918e7de8938e6ce631295c294e5e0def1e74d`; exact final sources
are recorded in `source-sha256.txt`. All retained XML has zero failures/errors/skips.

First final invocation, exit 0, 20 seconds:

```sh
./gradlew :protomolt-repo-container:test \
  --tests '*DocumentPreparationRootReleasesIT' \
  --tests '*DocumentPreparationRootReleaseIT' \
  --tests '*DocumentPreparationHistoryRootsIT' \
  --max-workers=2 --console=plain
```

`handler-history-results.tar.gz` retains nine handler cases and three existing
history-root cases from that invocation. `first-final-gradle.log` retains output.
The cancellation fixture in the eight-case SQL class was then adjusted to use
the same shared budget as its preparation, removing a redundant outer reservation
and separate budget. Only that changed class was rerun:

```sh
./gradlew :protomolt-repo-container:test \
  --tests '*DocumentPreparationRootReleaseIT' --max-workers=2 --console=plain
```

It passed all eight cases in 13 seconds. `sql-handler-results.tar.gz` and
`shared-budget-gradle.log` retain that final run. Across the two final archives,
20 distinct tests passed. Production sources were unchanged between the runs.

The new checks cover:

- Scoped caller denial, exact-capacity success, one-byte budget pressure and no
  reservation leaks on rejection, success or retry.
- Exact abandonment release/retry and changed preparation rejection.
- Canonical cancellation release using the same production handler and V111.
- Missing roots without a receipt, residual roots with a receipt and changed
  capture fingerprint reported as DATA_LOSS. Test-admin changes re-enable SQL
  guards before calling the handler.
- Actual JDBC pre-commit failure rolling back roots and receipt together.
- Actual JDBC post-commit lost acknowledgement leaving one durable receipt;
  a subsequent call returns the same committed receipt.
- Cancellation before work and at the JDBC pre-commit boundary leaving live
  roots, versus a post-commit signal preserving successful completion.
- Retry while source publication/part table names are unavailable. A positive
  control query fails against the unavailable table, while release retry succeeds.
  Renaming is controlled query-failure injection, not actual source pruning.

The tests use PostgreSQL 18. Source-publication provider observations remain
fixture-supplied; no provider performance/durability claim is made here.

Sol reviewed the handler and tests, found no blocker, and confirmed that retry
relies on the permanent receipt rather than treating released roots as live.

## Still required

The handler is private and unmounted. Remaining release qualification includes
V52 success, recovery-limit and other rejection alternatives, multiple capture
epochs and multiple roots, capture/release and two-release races, and actual
pruning/restoration. Source-table unavailability does not complete the pruning
requirement. Claimed historical publication remains gated. No public API,
full-goal completion, hosted CI, merge or deployment is claimed.
