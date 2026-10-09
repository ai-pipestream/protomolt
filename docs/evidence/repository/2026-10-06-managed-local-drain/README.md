# Managed journaled host drain

The focused container drain/local-drain suites and service `ManagedSchemaHostIT`
passed 34 tests in 40 seconds. Their JUnit results are retained here.
`./gradlew :protomolt-repo-container:admissionStorageTest --console=plain`
then passed the complete production-JAR and restart regression in 3 minutes
2 seconds; `runtime.xml.gz` records its result.

The new probe uses PostgreSQL, versioned LocalStack storage and an actual Git
schema registry. It holds a descriptor load after publication cancellation.
Two admitted operation identities have V90 markers but no V91 markers while the
worker remains alive. After release, a database trigger fails the second V91
insert. The first marker remains durable and the host retains usable SQL. Retry
confirms the first timestamp, records the other marker and releases the host pool.
Borrowed registry access remains usable. The second operation is owner-admitted;
the fixture does not require two independent descriptor loads.

A startup-negative case refuses an absent runtime bundle, closes newly registered
readers and leaves schema access owned by the caller. Sol reviewed the lifecycle
and failure assertions without finding a blocker.

This qualifies internal managed composition. Public builders, protobuf contracts
and RPC availability are unchanged. It proves neither remote-effect quiescence nor
successor safety. A subsequent successful opaque publication and exact receipt replay prove that
completed sessions require no V90 or V91 drain markers. That extended complete
regression passed in 2 minutes 56 seconds; see `terminal-runtime.xml.gz`. Temporary host logs are cleaned after successful runs;
the persisted JUnit result records the aggregate test that requires the probe's
success marker. LocalStack results are correctness evidence, not performance data.
