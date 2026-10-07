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

Initial anchor, unactivated predecessor, 63/64-edge limit and released roots need
explicit cases. Rerun complete suites after those additions. Managed routing and
library/gRPC qualification remain separate requirements. This checkpoint does not
establish complete restart recovery or public availability.
