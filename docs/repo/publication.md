# Publish a document through a local or remote repository

`DocumentPublicationRepository` publishes complete revisions and returns a durable
committed or rejected receipt. A call can contain several document members. The
publication intent identifies the operation, destinations, ownership and parts;
each member also declares typed or opaque admission. Payloads supply exactly the
upload slots named by the intent.

The library and gRPC client implement the same interface. Both use the repository's
current authorization and schema policy. A valid request is not an authorization
grant, and a structurally valid receipt alone is not independent proof of storage
durability.

## Local composition

Build `RepoServices` with `ManagedPublicationOptions` and `ManagedSchemaAccess`,
then obtain `services.publicationRepository()`. This opts into journaled publication;
the older builder overloads do not enable it.

Options identify a trusted local assessment bundle, its retention windows and a
drain-authority callback. Recovery authority is a separate `withRecovery` option.
The callbacks resolve current permission for the exact account, principal and
operation. They must not convert arbitrary request identities into process authority.

`withTransport` adds authenticated gRPC access and its application byte/call limits.
Historical reads are independent. Built-in listeners require an operator API token;
scoped credentials can be resolved by the host's credential resolver. Embedded
listeners must install authentication and the 10 MiB inbound parser limit.

Close the owning `RepoServices` when finished. If drain times out, resources remain
allocated for accepted work; retry close after that work stops. Close borrowed
schema-registry resources only after the composition closes successfully.

## Remote composition

The `protomolt-repo-publication-grpc` module contains
`RemoteDocumentPublicationRepository`. It does not depend on SQL, repository server
implementations, Kafka or storage-provider SDKs. Use the same project version as
the protobuf contracts and repository SPI; this checkpoint does not publish a release.

The host supplies an authenticated future stub and its matching `RepositoryCaller`.
For an existing `authenticatedStub`, `boundCaller` and complete `request`:

```java
var budget = new PayloadBudget(32L * 1024 * 1024);
DocumentPublicationRepository repository = new RemoteDocumentPublicationRepository(
        boundCaller, authenticatedStub, budget, Duration.ofSeconds(30), 4);
PublishDocumentResponse receipt = repository.publishDocument(
        boundCaller, request, RepositoryReadControl.NONE);
```

The types come from `ai.protomolt.proto.repo.publication.grpc`,
`ai.protomolt.proto.repo.spi`, `ai.protomolt.proto.repo.blob.spi` and
`ai.protomolt.proto.repo.v1`; `Duration` is from `java.time`.
The host owns the channel and its credential/connection policy. The caller passed
to `publishDocument` must equal the client's bound identity. To use another identity,
construct another client with the corresponding authenticated stub. Identity fields
are not sent as substitute credentials.

The client checks upload coverage, lengths, checksums and modes before sending. It
uses the shortest client, stub or call-control deadline. Local cancellation cancels
the outstanding RPC. Incoming response size and receipt correspondence are checked
before returning. Application byte reservations last through validation and response
handoff; the returned protobuf belongs to the caller. This budget does not account
for all caller allocations, channel buffers or JVM overhead.

## Retry the same operation after an ambiguous error

Cancellation, a deadline or a broken connection can occur after a commit. Such an
error does not establish rollback. Retry the same operation ID, intent, modes and
payloads to recover its receipt. Changing those inputs is a different request, not
a continuation of the original one. Replay rechecks current authorization and
credential validity, so revocation can prevent later delivery.

The client adds no retries. The host owns the gRPC channel retry policy. Remote
`ABORTED` and `UNIMPLEMENTED` statuses become repository `CONFLICT` and `UNSUPPORTED`.
A malformed or mismatched receipt becomes `DATA_LOSS`; it is not returned as a
successful result.

## Tested behavior and remaining work

The real-store [transport fixture](../../repo/container/src/test/resources/runtime-inventory/ManagedJournaledDrainProbe.java)
contains complete requests and compares local, direct gRPC and remote-client
receipts. It exercises PostgreSQL, versioned LocalStack and Git-backed schemas,
including scoped credential revocation, cancellation and corrupted wire responses.
This is integration evidence, not a throughput or horizontal-scaling qualification.
Partial hydration and additional complete-document providers remain separate work.
