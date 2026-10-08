# Retention lookup evidence

Base: `4102f76d17827332dbbaf0ad02afe6e43d9b5595`.
Branch: `agent/historical-retention-loader`.
Public historical routing remains disabled.

## Results

The epoch correction passed 9 loader tests and 13 reserved-preparation regressions
in 3m03s, with 0 failures, errors or skips. Reports: `epoch-correction.xml.gz`,
`reserved-regression.xml.gz`, `epoch-correction.log.gz`.

The delivery tests passed 4 cases in 20s: ACL revoke, credential revoke,
cancellation and successor installation after the first database commit.
Reports: `delivery.*`. Sol subsequently identified possible callback execution
during Hibernate startup. The callback now arms after EntityManagerFactory
construction. The corrected run passed all 4 cases in 30s with 0 failures, errors
or skips; `delivery-armed.*` is the inter-phase evidence. Sol reviewed production
code and the callback correction. `checkpoint-source-sha256.txt` identifies the
current source.

These tests use real PostgreSQL. Source publication fixtures supply synthetic
provider observations; provider I/O is outside this evidence. The successor-chain
case resolves the original preparation and checks that loading creates no execution,
capture, assessment, publication or drain records.

## Corrections

- `first-fixture-failure.*`: 5 cases passed; damaged-byte setup was rejected by the
  SQL digest constraint. The corruption fixture now explicitly removes that
  constraint inside the test database. An earlier ambiguous helper import was fixed.
- `corruption-red.*`: owner-incarnation and child-pin changes were incorrectly
  accepted. Added coordinator identity and canonical pin count/digest checks.
  `first-correction.*` records 6 passing corruption cases in 25s.
- `epoch-red.*`: replacing the whole owner tuple with a valid epoch-2 identity was
  incorrectly accepted. V94 permits successor epochs. The correction requires
  epoch 1 for generation 0, otherwise the exact preparation install identity and
  transaction. Source hashes accompany each red run.

## Commands

```sh
./gradlew :protomolt-repo-container:test --tests '*RepositoryHistoricalRetentionLoaderIT' --tests '*RepositoryReservedPreparationIT' --max-workers=2 --console=plain
./gradlew :protomolt-repo-container:test --tests '*RepositoryHistoricalRetentionLoaderIT.rejectsDamagedAncestryAndReleasesMemory' --max-workers=2 --console=plain
./gradlew :protomolt-repo-container:test --tests '*RepositoryHistoricalRetentionLoaderIT.rechecksChangesBetweenReadAndDelivery' --max-workers=2 --console=plain
```

## Outstanding

The added initial-anchor and unactivated-predecessor cases passed in 17s;
`entry-states.*` contains the reports. The actual SQL chain of 64 installations
passed the 63/64-edge boundary case in 1m47s; reports are `depth.*`.

The terminal-release case completed cancellation, 3 capture drains and V111
release while metadata remained loaded. A second load exhausted the occupied
test budget; that fixture now uses an independent budget for stale-reservation
checking. Tightening the released-state assertion then exposed DATA_LOSS where
FAILED_PRECONDITION was expected. The red report is `released-state-red.*`.
The binding query now excludes an exact V111 receipt before checking live roots.
Sol reviewed this change; it does not replace the concurrent release classifier.

A combined run of loader, reserved preparation, historical activation, root
release/replay and live-root corruption suites passed: 64 tests, 0 failures,
errors or skips; 5m29s command. All 7 terminal XML reports, the log and source
hashes are in `qualified/`. The loader suite contains 17 cases, including the
released-root correction and the complete ancestry limit. Managed routing and
library/gRPC qualification remain separate requirements. This checkpoint does
not establish complete restart recovery or public availability.
