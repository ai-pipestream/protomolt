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

Use one policy boundary for reflection, invocation, chains, pipelines, and
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

Generate a runnable application from a checked recipe or pipeline. Each output
should contain:

- protobuf messages and gRPC clients;
- a runner with configuration-bound service profiles;
- credential-provider hooks and health endpoints;
- recorded replay fixtures and contract tests;
- dependency locks and container files; and
- recipe, schema, generator, and template fingerprints.

Java and Python are the first targets. Go and TypeScript follow after their
gRPC generators are available through the code-generation runtime.

### Template compatibility

Define compatibility checks for generated applications and migrations between
template versions. Promotion should fail when an input change breaks the
recorded contract.

## Agent delegation

Extend the in-process coordinator where deployments need durable or remote
coordination:

- automatic reoffer from the latest valid checkpoint after restart;
- fenced or compare-and-set transcript writes for multiple coordinators;
- capability-based offer routing across multiple connected workers;
- repository and artifact evidence verifiers backed by configured stores;
- MCP resources and tools for task status, review, cancellation, and event
  waiting; and
- production `WorkerRunner` adapters for selected LLM providers.

Keep provider authentication and process lifecycle inside each adapter. The
delegation stream carries bounded tasks, progress, checkpoints, and evidence,
not provider credentials.

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

### Authorization scopes

Separate authentication from authorization. Define scopes for schema reads and
writes, service invocation, chain and pipeline execution, artifact access, and
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
