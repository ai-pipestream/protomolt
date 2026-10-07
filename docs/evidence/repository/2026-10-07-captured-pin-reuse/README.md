# Captured pin identity regression

Base `ca334b6134b3eff97e5e75ca43ba5203d66a751c`, branch
`refactor/repository-composition`, plus this checkpoint. `source-sha256.txt`
identifies the tested migration and test source. Sol reviewed both the design and
final implementation without a blocker.

Before V106, both tests in `DocumentCapturedPinReuseIT` failed because reinserting
the captured pin succeeded: ordinary reinsertion after release and a competing
insert held behind an uncommitted real V45 pin release. See `red/`. These are actual
PostgreSQL results, not a fake storage implementation. An earlier compile failure
from AssertJ overload inference is retained separately; it is not the red proof.

After V106, nine focused tests passed in 24 seconds, exit 0:

```sh
./gradlew :protomolt-repo-container:test \
  --tests '*DocumentCapturedPinReuseIT' \
  --tests '*DocumentHistoricalSourceDrainIT' \
  --tests '*DocumentHistoricalMultiRevisionPublicationIT' \
  --tests '*DocumentPreparationPinOwnerMigrationIT' \
  --max-workers=2 --console=plain
```

The new tests comprise two ordinary/migration variants and one race. The migration
variant creates and registers the captured pin under V105, then applies V106 while
it remains live. Existing pins survive; reuse after release fails. Fresh captures
and an unrelated handle on the same reader still work. The race observes the exact
deleting backend in `pg_blocking_pids` before permitting its commit, then requires
the competing insertion to fail with the new guard's error. See `green/`.

Another 26 existing tests passed in 14 seconds, exit 0:

```sh
./gradlew :protomolt-repo-container:test \
  --tests '*DocumentReaderPinsIT' \
  --tests '*DocumentHistoricalReaderPinsIT' \
  --tests '*DocumentReadReleaseIT' \
  --max-workers=2 --console=plain
```

Counts: current reader pins 17, historical reader pins 5, release 4. Combined with
the nine focused tests, 35 tests passed with zero errors, failures or skips. See
`existing/`. JUnit XML trailing whitespace was normalized without changing values.

The publication fixture's provider observations are synthetic. These runs qualify
database pin identity, migration, locking and release behavior; they do not measure
object-store performance or prove remote provider quiescence. The guard adds an
indexed pin-ID lookup to pin insertion. No throughput claim is made here, and the
full production storage suite was not rerun for this checkpoint.

V106 protects captured IDs against resurrection. It does not create durable drain
records, release retained historical roots, enable public historical execution or
prove crash recovery. Those remain separate requirements of the full goal.
