# Publication transport checkpoint

The packaged storage integration passed in 3 minutes 39 seconds. JUnit reports
one integration test, zero failures, errors or skips; its subprocess probes use
real PostgreSQL, versioned LocalStack and Git schema storage.

Command: `./gradlew :protomolt-repo-container:admissionStorageTest --console=plain`.

ManagedJournaledDrainProbe now publishes its opaque control through an actual
in-process gRPC server in all three lifecycle scenarios. Library replay and
transport replay must equal the original response. The probe refuses absent
authentication, a host binding that drops the authenticated credential, an empty
request and incomplete replay uploads. Delivery reservations return to zero.
Authentication is a fixture interceptor, not a production API-key deployment.
The final run also covers an exact registered scoped credential reaching the SPI,
successful scoped replay, and refusal after credential revocation. Credential
setup invokes the internal authority through test reflection; no public
credential-provisioning API was added.

Controlled faults surround the real repository. Corrupt committed and rejected
receipts return DATA_LOSS; an unexpected runtime exception returns a generic
INTERNAL description. During cancellation the producer remains active until the
test releases it. Its byte reservation remains allocated and a second call is
refused at capacity. Once the producer exits, reservations reach zero and replay
succeeds through the same adapter.

A real loopback Netty server accepts the same receipt replay, then rejects a wire
request above its 10 MiB parser limit with RESOURCE_EXHAUSTED. The publication SPI
invocation count does not change for that request. Channels and servers shut down
and terminate within their test bounds.

The adapter validates requests and response correspondence, preserves principal,
process authority and credential identity, and maps unexpected runtime failures
to a generic INTERNAL status. The repository remains responsible for durable
outcome authority. The service is not mounted by public host configuration.

Sol reviewed the adapter, fault tests and Netty fixture and found no checkpoint
blocker. Public journaled composition, production authentication wiring and
service mounting remain. These runs do not qualify performance or scaling.
Fatal JVM Errors are outside the adapter's runtime-exception status mapping.
