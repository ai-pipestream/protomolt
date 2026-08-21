# Planned work

This page lists open product and hardening work. Current behavior belongs in
the feature guides linked from the [documentation index](../README.md).

An entry states the ask first and then where the tree actually stands, so a
reader can tell an untouched item from a half-built one without reading the
code. An item with nothing left open does not belong here at all.

## Service connectivity

### Authenticated reflection

Service profiles can name an opaque credential reference. Apply host-resolved
gRPC call credentials consistently to registration, refresh, inspection, and
invocation. Start with `env:` bearer credentials. Keep credential values out
of profiles, descriptor artifacts, summaries, logs, and errors.

The contract half exists and the behavior half does not. A profile carries
`credential_ref`, `trust_ref`, and `client_certificate_ref`, and profile
validation refuses anything that is not a namespaced `<scheme>:<name>`
reference, so raw secrets cannot be stored. Every gRPC path that would use
one then refuses it by name with `unsupported-transport`, which is the
honest placeholder: registration, refresh, and invocation all require a
credential resolver that is never supplied, and no `CallCredentials` is
constructed anywhere in the repo. An `env:` resolver already exists for the
inference lane and is the piece to reach across, not rebuild.

### Shared outbound policy

Use one policy boundary for reflection, invocation, workflows, pipelines, and
schema gathering. Add scheme, host, port, resolved-address, deadline, payload,
and concurrency limits. Recheck resolved addresses before connecting.

`OutboundChannelPolicy` is that boundary and already enforces scheme, host
(with wildcard suffixes), port, plaintext and TLS toggles, a deadline
ceiling, a leased concurrency semaphore, and syntactic target hardening down
to DNS labels and IPv6 groups. Three of the five call sites route through it:
reflection, invocation, and workflows. Still open: payload and message-size
limits, which the policy does not express at all; resolved-address validation
and the re-check before connect, which the class deliberately does not do
today (it validates without resolving); and pass-through for pipelines, whose
transport takes a caller-supplied channel resolver, and for schema gathering,
whose git, Maven, and registry-publisher clients each carry their own
transport.

### Registry federation

Mirror selected subjects between the Git-backed registry, Confluent, and
Apicurio. Add branch-per-scope workflows, Maven descriptor publication, and a
binary `FileDescriptorSet` registration endpoint.

What exists is a different, narrower thing: `RegistryFederation` pulls whole
remotes git-to-git and never pushes, with no subject selection. The Confluent
and Apicurio publishers push a build-time source set and neither reads from
nor writes to the Git-backed registry's subjects, so there is no mirror
between the three. The registry serves a binary descriptor set for reads
only; registration remains JSON-text. Maven appears solely as a consumer,
resolving coordinates to extract protos.

## Pipeline applications

### Durable execution

Connect external-completion pipeline steps to the job coordinator. Persist
step checkpoints and evidence so a run can survive process restart, human
approval, and worker reassignment.

The workflow half is finished: runs park on an external-completion step,
checkpoints persist, leases requeue on expiry, `complete-step` resumes
idempotently, and evidence has a durable home. The gap is precisely the
pipeline half the title names — `PipelineExecutor` still refuses an
external-completion step because no coordinator is wired to it.

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

- automatic reoffer from the latest valid checkpoint after restart. Resuming
  from a checkpoint works today, but the caller names the checkpoint; nothing
  re-offers on its own, and lease expiry only expires;
- fenced or compare-and-set transcript writes for multiple coordinators. The
  transcript repository is a full-snapshot replace with no precondition;
- capability-based offer routing across multiple connected workers. Workers
  advertise capabilities and several sessions can connect, but every offer
  still names one worker explicitly and nothing matches a spec against the
  advertisements;
- repository and artifact evidence verifiers backed by configured stores. The
  reviewer seam exists with only manual and accept-all implementations
  shipped; the one real verifier is test-scoped;
- caller-supplied idempotency keys on MCP mutation tools. The jobs submit path
  has them; no delegation verb does, and the coordinator's frame dedupe is not
  a substitute; and
- a pluggable provider registry. A third adapter (OpenAI-compatible chat
  completions) now ships beside Codex and Kimi, but the selector is a closed
  three-way choice rather than a registry.

Keep provider authentication and process lifecycle inside each adapter. The
delegation stream carries bounded tasks, progress, checkpoints, and evidence,
not provider credentials.

### Local worker sidecar

Add a typed gRPC transport between a local ProtoMolt sidecar and its coding
worker. The sidecar should own coordinator TLS, workload identity, reconnect,
cursor persistence, and transcript writes. The worker should expose only a
bounded task-scoped workspace execution API on a private local endpoint.

The control link is MCP over HTTP today, and the worker images are not gRPC
sidecars however convenient the word would be. The agent host already owns
reconnect and cursor persistence over that link; what is missing is the typed
transport itself, a worker-side workspace-execution service, and TLS with
workload identity.

Do not use a host Docker socket or a Kubernetes administrative credential as
the execution protocol. Image builds should go through an explicitly enabled
rootless or remote builder with separate credentials and policy.

## Retrieval and metadata

### Retrieval evidence

Add an engine-neutral retrieval contract containing document and chunk
identity, source URI, offsets, schema version, transformation lineage, engine
score, and rerank score. Retrieval results should remain traceable to their
source through mapping, projection, indexing, and reranking.

`SearchHit` carries three of those today — `doc_id`, `chunk_id`, and the
engine `score` — plus a `stored` map of whatever the mapping declared, which
is where a source URI lands by operator convention rather than by contract.
The rest is genuinely absent: chunk offsets exist on `SemanticChunk` and
survive chunking in memory but are dropped at index time, no schema-version
or lineage field exists anywhere, and no rerank score reaches the wire (the
one two-score type is OpenSearch-module-local and unwired). Offsets are the
cheapest first step, because the value already exists and only needs
carrying.

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

## Evidence and receipts

### Signed work records

The receipt layer is implemented — canonical signed records, the strict
offline verifier and conformance corpus, the projector and its three
verbs, the trust-snapshot mount, the transparency log document, disclosure
projections, and the evaluation sidecar. Design of record:
[signed work records](receipts.md). Still open:

- **A second projector.** The delegation transcript, once the workflow-run
  corpus has aged.
- **Lane custody for the trust mount.** The verifying verbs default to
  the operator's pinned snapshot file (`PROTOMOLT_TRUST_SNAPSHOT`);
  `TrustSnapshotMounts` follows the config lane, and a consumer that
  re-scopes trust live from the lane is still open.
- **Composable later, deliberately not in v1:** trusted timestamps
  (RFC 3161), transparency receipts (SCITT), environment attestation
  (RATS/EAT), countersignatures beyond the issuer.

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

The layer is complete. The closed vocabulary and caller model, per-action
declarations enforced at the catalog, the access-policy document (startup
file and config-lane mount), scoped serving on gRPC, REST, MCP, the
registry's route split, and identity on the serving roles all landed with
the first train; the document platform then made a node's posture one
environment decision (`PROTOMOLT_API_TOKEN`, with `PROTOMOLT_ACCESS_POLICY`
re-scoping live off the lane). Console sessions bind to principals through
one shared mechanism serving both the task console and the search console,
so a guarded node serves its console instead of refusing it. External
caller stores mount behind the policy — OIDC introspection and a JDBC
principal table, mirroring the intake service's key stores. Per-scope
budgets cap requests per minute and payload size wherever scopes are
already checked. The metric mapping's row and member security rewrites
descriptions and queries from the caller. Design of record:
[authorization scopes](authorization-scopes.md). Still open:

- **MCP resources stay authenticated-only.** `tools/list` and `tools/call`
  are scope-checked; resource reads (registry documents, service profiles,
  delegation transcripts) are not, a recorded edge rather than a silent one.
- **Composable later, deliberately not in v1:** signed scope assertions,
  where a receipt-layer trust snapshot vouches for an external issuer's
  scope claims. The verification machinery exists; binding it to call
  credentials is its own decision.
- **RANGE and CEL row filters** on metric access, which today expresses
  row security as equality sets ([metric mapping](metric-mapping.md) v1.1).

### Transactional registry writes

Build Git commits with JGit plumbing and atomically advance refs. Add fault
injection around blob, tree, commit, and ref updates, plus documented recovery
behavior.

Writes today go through JGit porcelain — `add` then `commit` over a non-bare
working tree, serialized by a process lock and a file lock — so there is no
object inserter, no commit builder, and no atomic ref advance, whatever the
surrounding comment says. No fault-injection tests and no recovery
documentation exist.

### Protocol and API compatibility

Run MCP fixtures against the official inspector. Define stability labels for
Java APIs, protobuf contracts, stored formats, REST routes, and action
envelopes. Enforce compatible changes in CI once a release baseline exists.

Protobuf contracts are covered: CI runs `buf lint` and `buf breaking` against
the pull request's base branch. Everything else is open — no inspector-driven
MCP fixtures, no stability annotations of any kind, no Java binary-compatibility
tooling, and no REST-route or action-envelope check. Note that comparing
against a base branch is not the same as comparing against a release baseline,
which is what the last sentence asks for.

### WASM generator supply chain

Digest-pin generator build images and archives. Package the required license
and notice material and keep the binary checksum enforced by the build.

The checksum half is done and stays done: the build verifies the embedded
binary's SHA-256 against a recorded provenance file as part of `check`, and
the provenance notes pin the upstream commit and tool versions. Open: the
build image and downloaded tool archives are not digest-pinned, so a rebuild
is not hermetic, and the repository `NOTICE` carries no attribution for the
components compiled into the binary.
