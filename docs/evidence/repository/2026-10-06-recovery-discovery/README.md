# Private exact-operation recovery discovery

Focused command:

```sh
./gradlew :protomolt-repo-container:test \
  --tests '*RepositoryCoordinatorRecoveryDiscoveryIT' \
  --tests '*DocumentPublicationProcessRecoveryIT' \
  --tests '*RepositoryCoordinatorReservationIT' \
  --tests '*DocumentSuccessor*IT' --console=plain
```

Result: 35 tests, zero failures/errors/skips, BUILD SUCCESSFUL in 44 seconds.
The ten discovery tests use PostgreSQL; the fresh-JVM and delayed-PUT tests use
real PostgreSQL and LocalStack. Compressed XML retains the results.

Discovery uses a single exact-key metadata query with transaction-local SQL
lock/statement limits. Tests cover private authority, principal and digest checks,
no lease renewal, stale observation rejection, legacy unclaimed vs absent state,
unbound/pre-owner/unjournaled phases, both unactivated replacement windows,
terminal and abandoned operations, V90-only versus V91, and an activated epoch-two
coordinator later discovered and reserved at epoch three. A real table lock proves
SQL timeout is surfaced; post-commit cancellation does not return an observation.
The fresh-JVM process test now obtains private recovery identities through this
production API rather than its own bootstrap query.

The initial run had two fixture failures, retained as `fixture-red.xml.gz`:
creating a journaled owner without modes was correctly rejected by the existing
SQL guard, and a stale proposal was rejected by immutable reservation confirmation
before reaching the trigger expected by the test. The fixture now uses a legitimate
low-level unjournaled owner and asserts the actual earlier reservation refusal.
Sol also identified and reviewed the fix for LIVE masking structural phases.

Scope: private exact-operation observation and its use by the test-driven retry.
No public endpoint, fleet scanner, host recovery scheduler, pin-release authority,
or new process-liveness guarantee is introduced. Automatic recovery and RustFS
performance qualification remain unfinished.

Packaged runtime command:

```sh
./gradlew :protomolt-repo-container:admissionStorageTest --console=plain
```

Result: one production-JAR storage qualification passed, zero failures/errors/skips,
176.533 seconds (BUILD SUCCESSFUL in 2m 58s). Runtime inventory contained 38
artifacts. This is the existing packaged storage/admission regression; the new
private discovery is directly exercised by the focused SQL and fresh-JVM tests.
