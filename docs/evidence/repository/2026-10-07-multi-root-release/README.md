# Multiple roots and capture-versus-release qualification

Tests on production base `53bfabf98e26353ef506706bde6c90e8c356a0e8`; no production
code changed. The exact new test files are fingerprinted in `source-sha256.txt`.
Tests use PostgreSQL 18, real typed admission and real SQL. Source-publication
provider observations are fixture supplied; no physical provider test is claimed.

## Multiple roots

```sh
./gradlew :protomolt-repo-container:test \
  --tests '*DocumentPreparationMultiRootReleaseIT' --max-workers=2 --console=plain
```

Exit 0 in 8 seconds, one test with no failures/errors/skips. The Gradle log and
JUnit XML are retained as `multi-root-gradle.log` and `multi-root-results.tar.gz`.

The command references two distinct published revisions and records two roots
with one complete initial capture. A raw SQL receipt followed by deletion of
only one target root fails the deferred commit check; both roots remain and the
receipt rolls back. The private handler then releases both roots and returns the
same receipt on retry. Another preparation sharing the first source retains its
own root, no release receipt, and canonical live EXACT coverage.

## Competing capture

```sh
./gradlew :protomolt-repo-container:test \
  --tests '*DocumentCaptureRootReleaseRaceIT' --max-workers=2 --console=plain
```

Final exit 0 in 9 seconds, two tests with no failures/errors/skips. Final output
is `race-gradle.log` and `race-results.tar.gz`.

A release holds its real transaction at the JDBC pre-commit boundary. A competing
capture records its actual backend PID before calling the production claim-fence
function. The observer requires that PID's active PostgreSQL Lock wait and the
release backend in pg_blocking_pids. Only then is the release gate opened.

After either release commit or deliberate rollback, the capture is rejected by
terminal admission. There is no extra capture batch or owner. Root and receipt
counts match the release outcome. The fresh reader's pins were not in the stored
capture set and remain live until that reader closes; releasing the preparation
does not revoke an independent reader's retention.

The first observer mistakenly searched for literal FOR UPDATE text, whereas
lockLive calls `fence_repository_execution_claim`. Both initial cases failed the
observer assertion. `initial-observer-failure.log` preserves that failed run.
The fix identifies the actual waiter PID, function query and blocker PID; it does
not relax the requirement for a real observed lock wait or add blind timing waits.

Sol reviewed both tests. The multi-root review found no blocker. Its initial
race review missed the observer query mismatch; execution caught it and the main
agent corrected it before the final passing run. Sol then rereviewed the exact
PID/function-query predicate against lockLive and found no remaining blocker.
Review is not runtime evidence.

These tests cover release-holds-lock ordering for capture versus release. They
do not claim every possible ordering, public historical publication, V52 success
release, every rejection reason, actual history pruning, restoration, performance,
hosted CI, merge or deployment. The private release operation remains unmounted.
