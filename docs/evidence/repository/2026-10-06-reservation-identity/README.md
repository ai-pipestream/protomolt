# Shared coordinator reservation identity

V96 introduces a common immutable reservation record for successor installation.
It currently accepts only GRACEFUL records backed by exact V92 evidence. V92 still
requires local drain; V93 now holds a foreign key to the common parent. The
predecessor remote state stays UNKNOWN because local drain cannot settle remote PUTs.

On 2026-10-06, the focused suite passed 83 tests with zero failures, errors or skips
in 49 seconds:

```sh
./gradlew :protomolt-repo-container:test --tests '*RepositoryCoordinator*IT' --tests '*RepositorySuccessor*IT' --tests '*DocumentSuccessor*IT' --console=plain
```

The compressed XML files record the results. Added coverage includes:

- Migrating existing undrained V94 coordinator rows through the V95 fence.
- Backfilling an existing V95 graceful handoff without changing its exact tuple or
  timestamp; confirmation after expiry still cannot renew authority.
- Rejecting a parent without source evidence and rejecting altered successor token
  or timestamp, plus immutable parent update/delete checks.
- A real PostgreSQL trigger fault after parent insertion that rolls back the prior
  claim transfer and both child and parent records; retry then succeeds.
- Migrating an existing V93 install and V94 activation from V95, checking the new
  foreign key is validated and exact activation retry preserves both leases.
- Existing successor installation, activation, sessions and late-provider-write
  recovery against real PostgreSQL/LocalStack.

Sol reviewed the SQL and final tests without a blocking finding. A test compilation
error from generic AssertJ overload inference was fixed with typed local variables
before the passing run; it was not a runtime failure.

This is foundation work. There is no expired-unquiesced reservation source yet,
no new public API and no automatic crash recovery. Reader pins, schema retention
and old cleanup tombstones retain their existing protection. These results do not
measure RustFS throughput or horizontal scalability.

The separate `./gradlew :protomolt-repo-container:admissionStorageTest --console=plain`
also passed in 2m 56s. `storage-runtime.xml.gz` records its production-JAR test,
including graceful successor provider publication and retained-schema checks.
All results here are local validation, not hosted CI, merge or deployment.
