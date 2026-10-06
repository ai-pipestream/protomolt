# Managed publication host

The packaged PostgreSQL, versioned LocalStack and Git-schema integration passed
in 3 minutes 39 seconds. The public RepoServices builder now supplies all three
managed publication scenarios in ManagedJournaledDrainProbe. One scenario opts
into publication transport with historical reads disabled. Library-only scenarios
assert that publication transport is absent.

The mounted scenario uses the built-in API-token interceptor with a fixture
credential resolver. Tokenless startup is refused, and requests without a token
receive UNAUTHENTICATED. A typed call waits inside a real Git-backed schema read.
A 100 ms host close times out in PUBLICATION_RPC: the listener remains open, SQL
remains usable, and new publication requests receive UNAVAILABLE. After releasing
the schema read, the accepted call completes with one durable outcome. A second
close terminates the listener and releases SQL and the schema cache.

Command: `./gradlew :protomolt-repo-container:admissionStorageTest --console=plain`.
JUnit reports one packaged integration test with no failures, errors or skips.
The suite includes the preceding real in-process/Netty transport parity and fault
checks. It is not a deployment or a horizontal-scaling qualification.

After that run, public transport options gained a minimum delivery-budget check
covering one maximum request envelope, canonical command and terminal response.
The existing 32 MiB integration configuration exceeds that minimum. The focused
configuration and affected lifecycle tests then passed: nine tests, zero failures
or skips, in four seconds.

Command: `./gradlew :protomolt-repo-service:test --tests '*ManagedPublicationOptionsTest' --tests '*GrpcServerLifetimeTest' --tests '*LifecycleShutdownTest' --console=plain`.

Sol reviewed composition, callback authority, startup order and shutdown retention.
Drain and recovery remain separate host policies. The default builder does not
enable journaled publication or its RPC. Public options expose no SQL/container
types; assessment code is a trusted local bundle, not caller-supplied code.
