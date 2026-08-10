# gRPC recipe workbench

## Status

Accepted direction. Phase 1 now provides the durable service workspace,
reflected-schema artifacts, agent-facing MCP workflow, typed service lifecycle
actions, shared outbound channel policy, and generated catalog inventory.
Recipe evidence and promotion remain staged work below.

## Objective

ProtoMolt should turn a successful service exploration into software that no
longer depends on the exploring agent:

1. discover or load the contracts of one or more gRPC services;
2. inspect their methods and operational constraints;
3. probe them with controlled, validated requests;
4. compose and type-check an adapter or multi-service flow;
5. record enough evidence to replay and review the result; and
6. promote the proven recipe into a versioned, standalone application.

The intended lifecycle is **explore -> verify -> promote**. An LLM may suggest
semantic mappings and repair invalid structured output, but descriptors,
validation rules, CEL type checking, and recorded executions remain the source
of truth.

## Product model

### Service workspace

A service profile gives an endpoint a stable logical identity. It separates
portable recipes from environment-specific connection details and stores no
secret values.

The first profile contract contains:

- a stable service name;
- one or more named endpoints;
- plaintext or TLS transport selection;
- optional custom trust, client-certificate, and credential references;
- reflection or registry schema source and its descriptor fingerprint;
- health or readiness probe configuration;
- method policy such as read-only, mutating, idempotent, or approval-required;
- known-good request fixtures and links to recorded runs.

Credential and key material is resolved by the host. Profiles and recipes only
carry opaque references. All outbound connections eventually pass through one
shared channel policy enforcing host, address, port, transport, deadline, and
concurrency budgets.

### Recipe

A recipe is the durable result of exploration. It identifies its input and
output contracts and composes typed steps over registered service profiles.
Each step records the method, request mapping, validation behavior, deadline,
and execution policy. Recipe definitions are protobuf contracts stored and
versioned by the registry.

The existing chain runner remains the initial execution backend. The pipeline
contract becomes the long-term execution language because it already models
unary, client-streaming, server-streaming, and bidirectional calls alongside
CEL, projection, unnest, collect, and run records.

### Run evidence

Every exploratory or promoted execution produces a run record containing:

- recipe, service-profile, and descriptor fingerprints;
- method and request/response type names;
- redacted request and response artifact references;
- status, elapsed time, validation findings, and retry information;
- per-step input/output counts for streaming flows;
- mapping decisions and the source of each derived field; and
- model, prompt, persona, schema, token, and attempt provenance for inference.

Large descriptor sets, generated projects, fixtures, and payloads are stored as
content-addressed artifacts. MCP and action results return references rather
than copying those artifacts into an agent's context.

## Agent-facing MCP workflow

ProtoMolt's MCP server must explain the preferred workflow in its initialize
response and make the durable objects above available as resources. The first
512 characters of the instructions are self-contained so clients that truncate
guidance still receive the safe golden path.

The initial workflow is:

1. `service-register` or `reflect` to establish a service contract;
2. `service-inspect` to choose a method and understand its policy;
3. `service-probe` to run a bounded request and capture evidence;
4. `compose-recipe` to build candidate typed mappings;
5. `verify-recipe` for static checks and optional live fixtures;
6. `promote-recipe` to version the recipe and generate an application.

MCP resources are organized around services, methods, recipes, runs, and
artifacts. Resource templates allow a client to fetch one exact object without
loading every schema or all generated source. Tools publish read-only,
idempotent, mutating, and approval-required metadata where the negotiated MCP
revision supports it.

The protocol implementation has a real lifecycle: initialize, initialized,
cancellation, transport or session termination, negotiated capabilities,
pagination, and conformance fixtures for each supported revision.

## Structured inference

The existing LLM descriptor annotations, prompt renderer, response-format
shaper, validation feedback renderer, inference SPI, and OpenAI-compatible
providers become one `generate-structured` operation:

1. resolve the target protobuf message and its declared LLM policy;
2. render the prompt and strict JSON Schema response format;
3. invoke the selected catalog model;
4. parse the output into the target protobuf message;
5. validate the message;
6. run a bounded repair loop over structured validation feedback; and
7. return the typed message and complete provenance.

Provider profiles must support credential references, TLS configuration,
structured-output capability declarations, and streaming. A model may suggest
a value; it cannot override validation, sensitivity, or grounding rules.

## Standalone application generation

Promotion emits an application, not only protobuf message classes. The first
targets are Java and Python, followed by Go and TypeScript. Each generated
project includes:

- complete message and gRPC client code;
- a recipe runner with configuration-bound service profiles;
- credential-provider hooks and health/readiness endpoints;
- recorded contract fixtures and replay tests;
- Docker and dependency-locking files; and
- a build manifest containing recipe, schema, generator, and template versions.

The code generator therefore needs the Python, Go, and TypeScript service
plugins in addition to its current grpc-java plugin. Generated files are
returned as an artifact bundle rather than inline source for agent calls.

## Retrieval and provenance

Search writers, OpenSearch semantic retrieval, embeddings, and reranking are
already present, but agents need one engine-neutral retrieval contract. A
future `retrieve` operation returns evidence records containing document and
chunk identity, source URI, byte or character offsets, engine and rerank
scores, schema version, and transformation lineage. This provides the bridge
from OpenNLP analysis through distributed search to source-grounded answers.

Field metadata requires explicit propagation rules across mapping, projection,
merge, join, inference, and indexing. Sensitivity, validation, and indexing
policies are classified as transferable, contextual, derived, or prohibited;
conflicts fail promotion rather than silently dropping policy.

## Delivery sequence

### Phase 1: agent-operable foundation

- Add MCP server instructions and protocol lifecycle tests.
- Introduce protobuf service-profile contracts and a repository interface.
- Add service registration, listing, description, and refresh actions.
- Expose service profiles and method contracts as MCP resources.
- Introduce the shared outbound channel-policy seam.
- Replace manually maintained action-count documentation with generated or
  test-verified inventory.

Acceptance: Codex can connect to ProtoMolt, follow the advertised workflow,
register a reflection-enabled test service, inspect its methods, and recover
the same profile after process restart without copying a descriptor set through
conversation context.

### Phase 2: record, replay, and recipes

- Capture bounded probe executions as run evidence.
- Add content-addressed artifact storage and redaction.
- Define the protobuf recipe contract and compile current chains into it.
- Suggest structural mappings and type-check every candidate.
- Add static, fixture-replay, and opt-in live verification.
- Store and version promoted recipes in the registry.

Acceptance: an agent composes two live services, records a passing fixture,
replays it offline, and promotes a versioned recipe whose exact dependencies
are recoverable.

#### Phase 2 ownership and delegation

The core contract is the dependency gate for the parallel work packages below.
Agents should not invent a competing recipe, artifact, or run-evidence model.
The contract gate is commit `3f6e30b`. Each delegated agent should branch from
that commit, stay inside its listed boundary, and return a focused pull request
for integration review.

| Work package | Status | Ownership boundary | Acceptance evidence |
| --- | --- | --- | --- |
| Recipe and run-evidence contracts | **LANDED: `3f6e30b`** | Protobuf messages, validation, repository interfaces, shared test fixtures, and MCP wiring seams | Contracts round-trip, reject invalid identities and references, and preserve exact service-profile fingerprints |
| Content-addressed artifact store and redaction | **LANDED: #87** | Artifact repository implementation and tests only; no recipe or action API changes | Duplicate content has one identity, changed content fails verification, bounds are enforced, and sensitive fixtures prove redaction before persistence |
| Existing-chain compiler | **LANDED: #88** | Adapter from `ChainDefinition`/`ChainJson` to the published recipe contract and its tests | Existing chain fixtures compile deterministically and every method/type reference resolves against embedded descriptors |
| Offline fixture replay | **LANDED: this PR** | Replay verifier, fixture loader, deterministic result model, and tests; no live network invocation | A recorded passing run replays without a server and altered request, response, or descriptor evidence fails clearly |
| Registry promotion adapter | **LANDED: #89** | Recipe version storage in `GitSchemaRegistryStore`, compatibility checks, and tests | Promotion is immutable, recoverable by version, and rejects unresolved artifacts or dependency fingerprints |
| Structural mapping suggestions | **READY: branch from main** | Descriptor-grounded candidate generation and type-check tests; no provider-specific LLM integration yet | Suggestions identify their source and target fields, pass the same compiler type checker, and never bypass validation |

Delegated pull requests must include tests and a short update to this table.
They must not add credentials, release automation, or unbounded live calls.

### Phase 3: structured inference

- Implement `generate-structured` and its bounded validation-repair loop.
- Wire structured response formats into compatible providers.
- Add provider credentials, transport policy, and streaming action support.
- Allow structured inference as a recipe step with full provenance.

Acceptance: a model fills a validated protobuf form, repairs a deliberately
invalid first attempt, and produces a replayable evidence record without
exposing credentials or ungrounded fields.

### Phase 4: pipeline and application promotion

- Implement pipeline checking and execution across all gRPC streaming shapes.
- Compile recipes to the pipeline contract.
- Add complete Python, Go, and TypeScript gRPC generation.
- Generate Java and Python standalone applications, then Go and TypeScript.
- Add promotion compatibility checks and template-version migrations.

Acceptance: a proven multi-service recipe is generated as a containerized
application, passes its replay and live contract tests, and runs without an
agent or a ProtoMolt exploration session.

### Phase 5: grounded retrieval

- Define the engine-neutral retrieval and evidence contracts.
- Preserve source offsets and policy metadata through the full transform path.
- Connect OpenNLP analysis, indexing, retrieval, and reranking recipes.
- Produce source-grounded answer bundles suitable for an MCP client.

Acceptance: one recipe analyzes a document, indexes it, retrieves a grounded
passage, and returns provenance and offsets sufficient to verify the answer
against the original source.

## Non-goals and safety boundaries

- ProtoMolt does not store plaintext credentials in profiles, recipes, runs, or
  Git.
- Reflection does not imply permission to invoke every discovered method.
- An LLM suggestion never bypasses descriptor resolution, policy, validation,
  or live verification.
- Live verification is explicit for mutating or approval-required methods.
- Promotion does not publish a release. Release workflows remain separate,
  deliberate operations.

## Immediate implementation slice

The delivered Phase 1 foundation includes MCP instructions and lifecycle,
service-profile contracts, bounded reflection, durable profile and descriptor
storage, service registration and inspection tools, paginated MCP resources,
restart recovery tests, a host-configurable outbound channel policy, and a
generated catalog inventory. Phase 2 begins with recipe contracts, probe run
evidence, artifact storage, replay, and promotion.
