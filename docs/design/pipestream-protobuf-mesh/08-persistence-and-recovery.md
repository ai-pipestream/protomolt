# Persistence and recovery

## Objective

Provide durable, replayable mesh state without binding contracts or reducers to
one database, artifact store, or registry implementation.

## Dependencies

Requires the [core contracts](01-core-contracts-and-annotations.md). Define
interfaces early so service advertisement and the recursive runtime can use
them independently.

## Repository boundaries

- `EntityRepository` stores entity and current scope projections.
- `TranscriptRepository` appends immutable control and lifecycle events.
- `AdvertisementRepository` stores leased node and service advertisements.
- `ArtifactRepository` stores payloads, claim checks, checkpoints,
  descriptors, generated clients, and results.
- `RouteProfileRepository` stores immutable route, transform, security, and PII
  profile revisions.
- `LeaseRepository` provides fenced ownership with expiry and renewal.

Provide in-memory implementations for default tests. Provide PostgreSQL for
durable state, leases, and query projections; RustFS through the S3 adapter for
content-addressed artifacts; and the Git registry for immutable schemas and
processing profiles.

## Persistence rules

Reducers validate a transition before it is appended. Events are immutable,
sequenced per entity or scope, idempotent by frame id, and protected against
conflicting duplicate ids. Projection updates use optimistic version checks.
Lease fencing prevents an expired worker from committing a late result.

Artifacts are written by digest and verified on read. Repository rows store
references and safe metadata, not large payload bodies. Retention and deletion
follow the effective security posture and preserve required audit tombstones.

## Recovery

At startup, replay active transcripts, verify projection versions and digests,
expire stale advertisements and leases using a trusted clock, and reoffer work
from the latest valid checkpoint. At-least-once delivery is safe only when
frames and outputs retain idempotency ids and immutable content fingerprints.

## Indexed fields

Index fields only for a named query: entity and scope ids, parent id, state,
type, processor, node, tenant, created and updated time, deadline, lease expiry,
advertisement expiry, and registry revision. Do not index payloads, raw
transcripts, PII findings, credentials, prompt text, or artifact bytes.

## Tests

Define a repository conformance suite shared by in-memory and durable adapters.
Cover append and replay, optimistic conflict, identical and conflicting
duplicates, lease fencing, restart recovery, expired advertisements, artifact
digest mismatch, checkpoint selection, partial writes, retention, and query
index behavior. Durable tests can be opt-in; default unit tests need no
container.

## Acceptance criteria

- A killed coordinator reconstructs the same active state from durable events.
- Expired owners cannot commit results after reassignment.
- Artifact corruption is detected before deserialization or rehydration.
- In-memory and PostgreSQL adapters pass the same behavioral suite.

## Exclusions

Do not store protobuf payloads in searchable columns, require PostgreSQL or
RustFS for unit tests, or let adapters repair invalid reducer input.
