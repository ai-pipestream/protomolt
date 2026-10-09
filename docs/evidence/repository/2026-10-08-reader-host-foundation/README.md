# Reader host admission foundation

Base: `1eb7baea71149bf28d3116b7221c0b36238ea40c`.
The accompanying V112 migration and ReaderHostExecutionIT are the tested changes.

Command:

```sh
./gradlew :protomolt-repo-container:test --tests '*ReaderHostExecutionIT' --tests '*ReaderRegistrationIT' --max-workers=2 --console=plain
```

Result: 18 tests, zero failures, errors or skips; build successful in 24 seconds.
The initial test compilation required an explicit generic type to disambiguate an
AssertJ overload. The successful rerun produced the archived XML files.

Five new cases use PostgreSQL 18. They cover immutable reader bindings and host
tombstones, continued local-reader admission, and registration/read admission
against host fencing in both transaction orderings. The test waits for an actual
PostgreSQL blocker rather than assuming a sleep establishes the race. While one
host fence waits, another host registers and admits a reader successfully.
Thirteen existing registration tests cover nonce identity, uncertain commits,
cleanup retries and foreign registration protection.

This qualifies the SQL admission foundation only. It does not implement bound
Java reader constructors, external termination verification, orphan cleanup or
public historical routing. Host fencing leaves the reader ACTIVE and its pins
protected; it cannot establish quiescence. No throughput or horizontal scalability
claim follows from this test.

## Registration cleanup follow-up

The archived XML records the newer run:

```sh
./gradlew :protomolt-repo-container:test --tests '*ReaderHostExecutionIT' --tests '*ReaderRegistrationIT' --tests '*ReaderHostPinFenceIT' --max-workers=2 --console=plain
```

Result: 23 tests passed without failures, errors or skips in 19 seconds.
Coverage adds V111 migration, rollback, lost commit acknowledgment after host
fencing, foreign registration protection, and direct archive/document/assessment
insertion races. Existing reader protection remains after host shutdown. Failed
constructors retain the exact nonce and host in a QUIESCED tombstone.

Object fixtures are synthetic; the tests exercise PostgreSQL, not providers.
HISTORICAL scope uses the same retained revision, not superseded history.
The assessment fixture needed transaction-local operation write authority before
insertion. Compilation needed generic and lambda overload disambiguation.
Both fixture corrections preceded the successful run. Sol reviewed the change
without a blocker.

Ledger constructor integration, unknown-host cleanup tests, external termination
verification and orphan reclamation remain unfinished. Public routing is disabled.
