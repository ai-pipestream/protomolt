# MCP and node surfaces

## Objective

Expose one typed mesh runtime through a gRPC bidirectional node service and a
safe MCP interface that lets an LLM discover contracts and operate the mesh
without inventing wire formats.

## Dependencies

Requires the [core contracts](01-core-contracts-and-annotations.md) and stable
catalog and runtime interfaces. Advertisement, routing, artifacts, and scopes
can appear incrementally as their services become available.

## gRPC node service

Define an authenticated bidirectional stream with typed frames for handshake,
capability negotiation, advertisements, entity submission, admission,
assignment, status, checkpoints, barriers, results, cancellation, errors,
heartbeats, and lease renewal. Application inputs and outputs travel as
`EntityEnvelope`; lifecycle control does not travel as arbitrary `Any`.

Frames have stable ids, per-lane sequence numbers, timestamps, protocol
revision, and optional resumable checkpoint reference. Duplicate identical
frames are idempotent. Conflicting duplicates and sequence gaps are findings,
not silently repaired state.

## MCP interface

Expose resources for node capabilities, service advertisements, descriptor
catalog, entity types, route and transform profiles, active entities and
scopes, artifacts, and evidence. Expose typed actions for validate, advertise,
register, generate client, submit, watch, cancel, approve, request revision,
and replay.

MCP bootstrap instructions should lead an LLM through this sequence:

1. inspect node capabilities and security requirements;
2. resolve the input and expected result descriptors;
3. inspect eligible registered routes and processors;
4. validate a sample entity locally;
5. submit or advertise through a typed action;
6. monitor structured status and barrier events; and
7. accept only a validated, evidence-backed typed result.

## Surface safety

Do not expose arbitrary outbound targets, credential values, header maps, CEL,
SQL, file paths, or class names as action parameters. Authoring and promotion
actions require stronger authorization than submission and inspection.
Resources apply tenant and posture filtering before serialization.

## Tests

Run the same scripted entity lifecycle through direct Java calls, typed gRPC,
and MCP streamable HTTP. Compare normalized outcomes and evidence. Cover
reconnect, idempotency, sequence gaps, cancellation, expired lease, access
denial, resource filtering, invalid `Any`, and bootstrap discovery from an
empty client session.

## Acceptance criteria

- A newly connected LLM can discover the mesh and submit a custom protobuf
  without prior prose knowledge of that application contract.
- gRPC and MCP invoke the same service layer and produce equivalent evidence.
- Bidi reconnect resumes from the last accepted checkpoint and sequence.
- Unsafe authoring or outbound controls are absent from normal entity actions.

## Exclusions

Do not create a second MCP-only state model, translate every application into
one task proto, or expose internal repository records without policy filtering.
