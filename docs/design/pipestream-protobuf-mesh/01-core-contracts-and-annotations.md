# Core mesh contracts and annotations

## Objective

Define the transport-independent protobuf vocabulary shared by every mesh
node, processor, route, and persistence adapter. This package is the contract
gate for the other work packages.

## Ownership

Add a new protobuf API under `mesh/proto` and a small Java contract module
under `mesh/contracts`. Do not add networking, storage, or processor logic.

The package owns:

- `EntityEnvelope`, `EntityHeader`, and `SchemaReference`;
- entity, scope, processor, completion, and terminal-state enums;
- typed claim checks and artifact references;
- route, processing, security, trace, and evidence references; and
- typed message and field options for mesh processing roles.

## Contract requirements

An `EntityEnvelope` carries exactly one `google.protobuf.Any` payload or one
typed claim check. It also carries the fully qualified payload type and a
canonical descriptor-set SHA-256 fingerprint. A type URL alone is not a
sufficient schema identity.

The header includes stable entity, parent, and scope identifiers; scope depth;
data layer; payload length and digest; deadline; completion policy; profile
references; security posture digest; and trace and evidence correlation ids.
The header contains no credentials, bearer tokens, private keys, or raw policy
documents.

Message options may name the processing profile, result type, capability set,
route profile, recursion limits, PII requirements, LLM permission, approval
policy, and evidence policy. Field options identify routing keys, instructions,
grounding, attachments, scatter sources, results, evidence, PII scan targets,
and values prohibited from remote disclosure.

Existing metadata, validation, indexing, LLM, quality, and projection options
remain authoritative. Mesh options reference their behavior instead of
duplicating it.

## Validation and metadata

- Validate all identifiers, type names, digests, URIs, depths, sizes, and
  timestamps with `validate.v1`.
- Add message-level CEL rules for mutually exclusive inline payload and claim
  check forms, parent and depth consistency, deadline order, and required
  schema identity.
- Annotate every persisted field with `meta.v1` sensitivity.
- Index only entity id, scope id, parent id, state, processor id, type name,
  created time, and terminal time when the query API uses them.
- Do not index payload bytes, claim-check secrets, posture details, evidence,
  transcripts, or provenance.
- Define canonical protobuf hashing rules and test unknown-field handling.

## Tests

Use generated test messages and in-process validation. Cover valid inline and
claim-check entities, invalid schema identity, digest and length mismatch,
parent and depth inconsistency, expired deadlines, invalid type URLs, option
round trips, and canonical fingerprint stability.

## Acceptance criteria

- `buf format`, `buf build`, `buf lint`, and breaking-change checks pass.
- Java round trips preserve every contract field and custom option.
- A resolver can prove that an `Any` type and canonical descriptor fingerprint
  identify the same message definition.
- Invalid envelopes fail before routing, persistence, or processor execution.

## Exclusions

No service discovery, route execution, persistence adapter, gRPC stream, QUIC
adapter, LLM call, or PII implementation belongs in this package.
