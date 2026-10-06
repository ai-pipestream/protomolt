# Private successor activation

The focused run passed 61 PostgreSQL cases, including 10 Java activation cases.
Sol reviewed the helper, shared modes encoder, tests and scope documentation.

Activation tests cover an exact retry without lease changes, a conflicting plan,
missing install, separate coordinator/execution authority, allowed scoped access,
revocation before activation, readback after revocation, current revision conflict,
concurrent exact proposals, cancellation after binding, real JDBC post-commit reply
loss and byte-budget exhaustion. Every tested path releases its reservation.
Readback after revocation succeeds only as confirmation of a past commit; the test
separately verifies current document authorization denies execution.

The concurrent fixture needs capacity for two maximum-size encoding reservations.
The production helper reserves 33 MiB per call regardless of actual encoded size;
this is a conservative bound and does not establish memory efficiency or throughput.
The shared-budget rejection test verifies no execution identity is created.

Command:
`./gradlew :protomolt-repo-container:test --tests '*RepositorySuccessor*IT' --tests '*RepositoryCoordinator*IT' :protomolt-repo-container:admissionStorageTest --console=plain`

No public API or host session factory uses this helper yet. Provider recovery,
fresh successor sessions, scoped creation and claimed historical recovery remain
unfinished. No deployment or horizontal-scaling claim follows from this evidence.

The full command passed, including the production-JAR storage and restart test
(`runtime-final.xml.gz`). A final test-only extension distinguishes cancellation
immediately before and immediately after commit. The 11-case activation suite then
passed separately (`activation-final.xml.gz`); production code did not change
between those runs. Sol reviewed that extension. These are local results, not
hosted CI or deployment.
