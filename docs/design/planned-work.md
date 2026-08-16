# Planned work

This page lists open product and hardening work. Current behavior belongs in
the feature guides linked from the [documentation index](../README.md).

## Service connectivity

### Authenticated reflection

Service profiles can name an opaque credential reference. Apply host-resolved
gRPC call credentials consistently to registration, refresh, inspection, and
invocation. Start with `env:` bearer credentials. Keep credential values out
of profiles, descriptor artifacts, summaries, logs, and errors.

### Shared outbound policy

Use one policy boundary for reflection, invocation, workflows, pipelines, and
schema gathering. Add scheme, host, port, resolved-address, deadline, payload,
and concurrency limits. Recheck resolved addresses before connecting.

### Registry federation

Mirror selected subjects between the Git-backed registry, Confluent, and
Apicurio. Add branch-per-scope workflows, Maven descriptor publication, and a
binary `FileDescriptorSet` registration endpoint.

## Pipeline applications

### Durable execution

Connect external-completion pipeline steps to the job coordinator. Persist
step checkpoints and evidence so a run can survive process restart, human
approval, and worker reassignment.

### Standalone application generation

Generate a runnable application from a checked workflow or pipeline. Each output
should contain:

- protobuf messages and gRPC clients;
- a runner with configuration-bound service profiles;
- credential-provider hooks and health endpoints;
- recorded replay fixtures and contract tests;
- dependency locks and container files; and
- workflow, schema, generator, and template fingerprints.

Java and Python are the first targets. Go and TypeScript follow after their
gRPC generators are available through the code-generation runtime.

### Template compatibility

Define compatibility checks for generated applications and migrations between
template versions. Promotion should fail when an input change breaks the
recorded contract.

## Agent delegation

Extend delegation where deployments need multi-coordinator ownership and
stronger recovery:

- automatic reoffer from the latest valid checkpoint after restart;
- fenced or compare-and-set transcript writes for multiple coordinators;
- capability-based offer routing across multiple connected workers;
- repository and artifact evidence verifiers backed by configured stores;
- caller-supplied idempotency keys on MCP mutation tools; and
- provider adapters beyond Codex and Kimi.

Keep provider authentication and process lifecycle inside each adapter. The
delegation stream carries bounded tasks, progress, checkpoints, and evidence,
not provider credentials.

### Local worker sidecar

Add a typed gRPC transport between a local ProtoMolt sidecar and its coding
worker. The sidecar should own coordinator TLS, workload identity, reconnect,
cursor persistence, and transcript writes. The worker should expose only a
bounded task-scoped workspace execution API on a private local endpoint.

Do not use a host Docker socket or a Kubernetes administrative credential as
the execution protocol. Image builds should go through an explicitly enabled
rootless or remote builder with separate credentials and policy.

## Retrieval and metadata

### Retrieval evidence

Add an engine-neutral retrieval contract containing document and chunk
identity, source URI, offsets, schema version, transformation lineage, engine
score, and rerank score. Retrieval results should remain traceable to their
source through mapping, projection, indexing, and reranking.

### Metadata propagation

Classify protobuf field options as transferable, contextual, derived, or
prohibited. Apply the rules to projection, merge, join, inference, fan-out,
and indexing. Reject conflicting sensitivity, validation, or indexing policy
instead of dropping it.

### Index shapes

Define a protobuf `IndexShape` for one-to-many document projection and map
carving. A shape should declare the target index, field definitions, protobuf
or CEL value sources, split rules, and metadata propagation behavior.

### Sensitive vectorization

Require explicit opt-in before encrypted or otherwise restricted field content
can feed an embedding provider.

## Security and operations

### ARM64 build and Jetson inference processors

Expose native image builds through a bounded, task-scoped processor rather
than a Docker socket. The contract should allow declared repository, commit,
build target, resource limits, artifact destination, and required checks. It
should reject arbitrary host mounts, privileged containers, unbounded network
credentials, and undeclared output paths.

Nano1 provides the native ARM64 host and a manual-only trusted runner. Its
worker-image smoke builds are suitable for Jetson and Raspberry Pi targets
that use compatible ARM64 bases. The remaining build processor must expose
this capacity through a bounded task API and publish progress, immutable
artifact references, logs, checksums, and terminal evidence through
delegation.

Nano1 also runs DJL Serving on a JetPack 7.2 compatible ARM64 TensorRT image.
Its startup gate builds and executes a CUDA engine, and its live acceptance
checks the returned GPU identity and result. CPU inference and CPU model
offload are prohibited. GPU memory reservation across multiple inference
processors still needs a scheduler-owned budget instead of independent static
container limits.

Nano1 also exposes a reflected TEI gRPC service over its Tailscale address. It
serves a pinned `BAAI/bge-small-en-v1.5` model through a CUDA-only ARM64 build
and has live evidence for model identity, 384-dimensional normalized vectors,
semantic ordering, and coexistence with DJL. Future capacity work should read
provider queue depth instead of treating the publisher's scheduler-owned
in-flight count as the complete device load.

### Authorization scopes

Separate authentication from authorization. Define scopes for schema reads and
writes, service invocation, workflow and pipeline execution, artifact access, and
worker coordination. Test every route and action against its required scope.

### Console sessions

Add a server-side browser session or backend-for-frontend flow so the console
can operate in token mode without storing the process credential in the
browser.

### Transactional registry writes

Build Git commits with JGit plumbing and atomically advance refs. Add fault
injection around blob, tree, commit, and ref updates, plus documented recovery
behavior.

### Protocol and API compatibility

Run MCP fixtures against the official inspector. Define stability labels for
Java APIs, protobuf contracts, stored formats, REST routes, and action
envelopes. Enforce compatible changes in CI once a release baseline exists.

### WASM generator supply chain

Digest-pin generator build images and archives. Package the required license
and notice material and keep the binary checksum enforced by the build.

### Search door hardening (from the Phase 2 review)

The 2026-08-15 review confirmed these as real gaps, deliberately
deferred; the hardening train closed most of them (see
[the door guide](../search/door.md)): `search.v1` carries
`DeleteDocument`, the `delete-and-unindex` workflow ties un-indexing to
repository deletion, `replay-documents` with `prune` reconciles a subject
against the repository listing (which now serves only AVAILABLE rows),
durability commits batch behind a near-real-time searcher, every repo
read carries a call deadline, and `k` is bounded. Still open:

- `SearchHit.stored` is string-only, and `SearchRequest` constraints are
  enforced in the door's Java refusals rather than by proto validation
  annotations (the repo-wide annotation family is not yet enforced
  anywhere in production).
- Body derivation belongs in the coordinator: the text parser claiming
  `body` fixed the reference path, but gRParse-parsed documents still index
  with an empty body. The long-term home is coordinator-side derivation
  from parsed text items, so every parser's output becomes searchable.
- `apps/serve` (`ProtoMoltServe`) still has the swallowed
  JsonProcessingException workflow-repository pattern the registry module
  was cured of; same fix applies (serve track).
