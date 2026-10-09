# Undrained coordinator mutation fence

V94 allowed the general mutation guard to accept a transferred claim when a
coordinator binding existed but no local-drain record existed. The new regression
uses real PostgreSQL, natural lease expiry and the low-level claim transfer. Before
V95 it failed because the mutation check did not throw (`red.xml.gz`). V95 requires
an exact initial binding or a live successor execution grant for bound operations.

The final coordinator/successor suite passed 79 tests in 44 seconds:

```sh
./gradlew :protomolt-repo-container:test --tests '*RepositoryCoordinator*IT' --tests '*RepositorySuccessor*IT' --tests '*DocumentSuccessor*IT' --console=plain
```

The regression also checks that mode registration fails and commits no mode row.
The first broad run had one assertion mismatch: an existing V90-only transfer test
expected the former admission error instead of V95's earlier activation error.
Its rejection behavior remained correct; the final run includes the updated assertion.

`claim-ledger.xml.gz` records the subsequent unbound claim-ledger checks. The ordinary
test task excludes `DocumentAssessmentStorageRuntimeTest`, so that filtered run is
not production-runtime evidence; the separate `admissionStorageTest` task is required.

The claim-ledger run passed all 12 cases. The subsequent
`./gradlew :protomolt-repo-container:admissionStorageTest --console=plain` also
passed its production-JAR integration test (`storage-runtime.xml.gz`), including
the real-provider graceful successor probe and retained schema/admission checks.
These are local results, not hosted CI or deployment evidence.

Sol reviewed the SQL guard without finding recursion or lock-order issues. The
change does not enable abrupt-death recovery. A raw claim transfer can still strand
a bound operation; it cannot authorize mutations. Old read pins and schema evidence
must remain protected without proven reader quiescence.
