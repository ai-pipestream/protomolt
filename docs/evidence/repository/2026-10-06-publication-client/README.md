# Remote publication repository

The final real-store integration passed in 3 minutes 37 seconds. The packaged test
reports zero failures, errors or skips. The 39 SPI tests passed in the preceding
run and remained up-to-date in the final invocation. Runtime dependency checks and
Maven POM generation passed.

```
./gradlew :protomolt-repo-publication-grpc:checkRuntimeBoundaries :protomolt-repo-publication-grpc:generatePomFileForMavenPublication :protomolt-repo-spi:test :protomolt-repo-container:admissionStorageTest --console=plain
```

RemoteDocumentPublicationRepository implements the same publication SPI over a
host-authenticated future stub. Its exact bound caller cannot be replaced by a
request argument. Input, canonical command and response allowances last through
validation and handoff. The original earlier stub deadline is preserved with
Deadline.minimum. The client adds no retries; the host owns channel policy.

ManagedJournaledDrainProbe publishes through this client against actual PostgreSQL,
versioned LocalStack and Git-backed schema storage. It compares local, direct gRPC
and remote-client receipts; rejects a different caller, pre-cancellation and an
expired stub before invoking the repository; verifies scoped replay and revocation;
and propagates local cancellation to an active server producer without releasing
the server's resources early. Client reservations reach zero and replay succeeds
after cancellation.

Controlled faults map ABORTED to CONFLICT and UNIMPLEMENTED to UNSUPPORTED. A
forwarding interceptor corrupts an actual receipt after server validation, proving
that the client's shared SPI validator independently refuses it with DATA_LOSS.
The shared validator checks correspondence, not independent durability.

The generated POM names only repository SPI, repository protobuf and byte SPI as
direct dependencies. The resolved production runtime gate excludes server,
container, engine, SQL, Kafka and selected storage SDK artifacts. An isolated
consumer resolving a published artifact remains a separate packaging check; this
run generated local metadata and did not publish a release.

Sol reviewed the client, shared validation, test faults, deadline policy and guide.
No throughput, horizontal-scaling or additional-provider qualification is claimed.
