# Historical occurrence gRPC client

The new `protomolt-repo-history-grpc` leaf provides one-shot selected historical
reads. Its production dependency gate excludes SQL, Kafka, provider SDKs and
repository server assemblies. The supplied authenticated stub owns remote
identity; each read makes a fresh RPC. An already delivered result does not
promise a new server authorization decision on local access.

Inventory classification: the Java client entry point and module are new;
`ReadHistoricalOccurrence` and all protobuf messages, names, tags and Any URLs
are unchanged. The client introduces no mutation, retry or receipt contract.

The production-JAR storage probe exercises the client through the real gRPC
service, PostgreSQL and the LocalStack-backed adapter. It checks retained dynamic
decoding, result byte ownership, open-call capacity, revoked-access refusal on a
new read, idempotent close and post-close refusal.

The lifecycle probe holds an actual server response after storage and validation.
It checks client cancellation, deadline, interruption with preserved interrupt
status, and host exceptions shaped like input or capacity failures. Each case
checks client reservation release, cancellation at the server, producer drain and
a successful subsequent read through the same client. No successful storage
behavior is mocked.

Verification command:

```
./gradlew :protomolt-repo-history-grpc:check :protomolt-repo-container:admissionStorageTest
```

The retained XML is the storage wrapper's result; its child-process assertions
cover the scenarios above. This is local correctness evidence, not a RustFS
performance run, published Maven artifact check, hosted CI result or deployment.
Managed historical mounting and full remote repository SPI parity remain open.
