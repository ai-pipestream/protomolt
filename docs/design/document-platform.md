# Document platform architecture

The document platform uses framework-free Java services, protobuf contracts,
blocking APIs on virtual threads, PostgreSQL ledgers, S3-compatible object
storage, and Kafka events encoded by ProtoMolt.

## Service boundaries

### Repository service

Repository service is the document store. PostgreSQL indexes each stored
state, while object storage holds document parts and raw blobs. Only repository
service resolves drive credentials or accesses object storage.

Documents are split into independently addressable protobuf parts: `CORE`,
`BLOBS`, `CHUNKS`, and `PARSED`. Reads select only the required parts. A
manifest records each object's key, size, checksum, and state.

The service provides:

- `DocumentService` for save, partial save, read, list, replay, and delete;
- `DocumentStreamService` for chunked uploads;
- `DriveService` for account-scoped storage configuration;
- a content-addressed blob API;
- two-phase purge, sweeping, reconciliation, and coherence checks; and
- transactional document events through an outbox.

See [`repo/README.md`](../../repo/README.md) for storage, lifecycle, eventing,
configuration, and tests.

### Account service

Account service owns tenant records, account lifecycle, identity resolution,
and the provisioning of each account's `intake` and `pipeline` drives. It is a
peer process with its own database. Its only runtime dependency on repository
service is the `DriveService` gRPC contract.

Account mutations and their Kafka event records commit in one transaction.
Drive provisioning happens before the account transaction, and
`CreateDrive` is idempotent.

See [`account/README.md`](../../account/README.md) for the API, configuration,
eventing, and tests.

### Intake and parsing

Intake is the public write boundary. It authenticates the caller, resolves the
account and drive, wraps payloads in the repository contract, and saves them
through `DocumentService`.

The parsing coordinator selects parsers with CEL rules, invokes compatible
parser services, records every parser result, and folds document-level claims
into search metadata. The current modules define the wire contracts; service
implementations are separate planned work.

See [intake and parsing](intake-and-parsing.md).

### Engine, search, and frontend

The engine owns pipeline graphs, hydration, clusters, and demand-driven module
work. Search consumes repository streams or replay and owns index shapes. The
frontend uses gRPC-web-friendly service methods.

These concerns do not belong inside repository or account service.

## Identity and tenancy

`NodeAddress` is the canonical document coordinate. It contains `doc_id`,
`graph_address_id`, `account_id`, and `graph_id`. Deterministic UUIDv5 values
derive from those logical coordinates, so repeated saves are idempotent.

Accounts are ownership boundaries. Clusters are routing domains and never
ownership credentials. Drives are account-scoped namespaces that bind a
bucket, prefix, region, and opaque credential reference.

## Security

Document security uses typed principals and deny-wins access rules. Search may
flatten read policy into index fields, but repository service keeps the
authoritative document policy.

Credentials remain host-owned. Protobuf messages carry opaque references, not
secret values.

## Concurrency

Services use blocking code on virtual threads. Parallel part I/O, gRPC calls,
upload handling, and event relays use one virtual thread per bounded unit of
work. APIs expose no reactive types.

## Storage consistency

Object writes and ledger rows are not one distributed transaction. An object
without a live owning ledger row is an orphan and may be reclaimed after a
minimum-age guard.

Soft delete writes a tombstone and purge record atomically. The asynchronous
purger deletes the recorded object keys, then removes the row. A staleness
check protects documents revived after purge was requested. Reconciliation
finds orphan objects; coherence checks find missing objects referenced by
manifests.

## Event delivery

Repository and account services use transactional outbox tables. State changes
and event records commit together. Virtual-thread relays publish protobuf
events to Kafka with at-least-once delivery, so consumers deduplicate by event
ID.

Kafka is optional. When it is not configured, services do not write outbox
events or start relay loops.

## Module layout

| Area | Modules |
| --- | --- |
| Repository | `repo/proto`, `repo/container`, `repo/service` |
| Accounts | `account/proto`, `account/service` |
| Intake | `intake/proto` |
| Parsing | `parse/proto`, `parse/document` |

Repository and account services support in-process transports for tests and
embedding, plus standalone Netty gRPC servers with health and reflection.
