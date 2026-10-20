# mesh/

The protobuf mesh profile: the entity vocabulary mesh nodes share
(`EntityEnvelope`, `EntityHeader`, `SchemaReference`, claim checks and the
`mesh.v1` processing options), the contract gate that validates envelopes and
fingerprints descriptor sets, and the memory-resident cluster directory.

| Gradle project | Directory | What it is |
|---|---|---|
| `:protomolt-mesh-proto` | `mesh/proto` | The mesh core contract: envelopes, schema references, claim checks, `mesh.v1` options |
| `:protomolt-mesh-contracts` | `mesh/contracts` | The contract gate: fail-fast envelope validation, canonical descriptor-set fingerprinting, schema-identity resolution |
| `:protomolt-mesh-cluster` | `mesh/cluster` | The cluster directory: advertisement and presence contracts, the in-memory TTL directory, the delegation hello bridge, and the six `mesh-*` verbs |

Docs: [cluster directory](../docs/core/cluster-directory.md). The
[PipeStream protobuf mesh design set](../docs/design/pipestream-protobuf-mesh/README.md)
is non-normative design history, not a description of these modules.
