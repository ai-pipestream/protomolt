# gRPC recipe workbench

## Status

Phase 1 and Phase 2 are implemented. The current integration exposes the recipe
workbench through MCP, typed gRPC, REST, and OpenAPI, with durable artifact and
run-evidence storage mounted by the host. Phase 3 structured inference is the
next delivery phase.

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

The implemented workflow is:

1. `service-register` or `reflect` to establish a service contract;
2. `service-inspect` to choose a method and understand its policy;
3. `grpc-invoke` to probe a method with a bounded request;
4. `suggest-mappings` and `check-chain` to compose a descriptor-grounded chain;
5. `compile-recipe` and `record-recipe-run` to capture the checked recipe and live fixtures;
6. `replay-recipe` to verify those fixtures offline; and
7. `promote-recipe` to store an immutable recipe version in the git registry.

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
| Offline fixture replay | **LANDED: #90** | Replay verifier, fixture loader, deterministic result model, and tests; no live network invocation | A recorded passing run replays without a server and altered request, response, or descriptor evidence fails clearly |
| Registry promotion adapter | **LANDED: #89** | Recipe version storage in `GitSchemaRegistryStore`, compatibility checks, and tests | Promotion is immutable, recoverable by version, and rejects unresolved artifacts or dependency fingerprints |
| Structural mapping suggestions | **LANDED: #91** | Descriptor-grounded candidate generation and type-check tests; no provider-specific LLM integration yet | Suggestions identify their source and target fields, pass the same compiler type checker, and never bypass validation |
| MCP integration and acceptance | **LANDED: #92** | Workbench actions, host-owned artifact/evidence wiring, typed RPCs, MCP guidance, and the live acceptance test | One MCP session suggests mappings, compiles a two-service chain, records redacted evidence, replays offline, promotes into git, and recovers the version after restart |

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

#### Phase 3 starting point and delegation

Do not rebuild the pieces already on `main`. ProtoMolt already has `llm.v1`
and `quality.v1` descriptor annotations, `PromptRenderer`,
`ResponseFormatShaper`, `ValidationFeedbackRenderer`, strict protobuf JSON
parsing, the inference catalog and provider SPI, unary and streaming inference
actions, and OpenAI-compatible OpenVINO and generic providers. Phase 3 joins
those capabilities behind one descriptor-grounded operation.

| Work package | Status | Ownership boundary | Acceptance evidence |
| --- | --- | --- | --- |
| Structured generation contract and coordinator | **LANDED: #95** | Add the typed request, result, and per-attempt provenance contracts plus one bounded coordinator. Resolve the target descriptor, render the prompt and response schema, call `InferenceEngines`, parse strict protobuf JSON, validate, and retry only from rendered validation feedback. Do not change provider HTTP transports, recipe steps, or deployment. | A scripted provider returns invalid JSON or an invalid message on its first attempt and a valid message on its second. The coordinator stops at a validated `DynamicMessage`, records both attempts and token/model/schema provenance, rejects an unknown type or incompatible model before invocation, and never exceeds a validated maximum of three attempts. Tests use an in-process fake provider and require no container or GPU. |
| Provider structured-output transport | **LANDED: #96** | Carry the coordinator's named, validated JSON Schema through `GenerateRequest`, advertise structured-output capability in catalog surfaces and launcher configuration, and emit the strict OpenAI-compatible response-format envelope from the shared OpenAI/OpenVINO transport. | Exact `/v1` and `/v3` wire tests prove the schema envelope, the catalog and transport reject incapable models before HTTP, malformed schemas fail without being echoed, and model labels or credential references never enter provider bodies. |
| Recipe step and evidence integration | **LANDED: #98** | Add structured inference as a recipe step and persist bounded, redacted prompt, response, validation, and attempt evidence through the existing artifact and run-evidence repositories. | Offline replay verifies the selected model, prompt/schema fingerprints, typed output, validation result, and attempt history without calling a provider. |
| Live structured-inference acceptance | **WAITING ON RECIPE INTEGRATION** | Exercise one explicitly configured compatible model through MCP and typed gRPC. This package owns test configuration only, not model deployment. | A live opt-in test performs one repair, returns a valid protobuf message, and proves that secrets and sensitive fields are absent from stored evidence. |

The next agent should take only the row marked `NEXT AGENT` and branch from
`main` after the preceding package lands. Any new persisted field must carry
validation and sensitivity metadata. Add index annotations only when the field
is actually part of a searchable index contract; operational request and
provenance fields must not be mislabeled as indexed data.

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

## Current implementation

The delivered Phase 1 foundation includes MCP instructions and lifecycle,
service-profile contracts, bounded reflection, durable profile and descriptor
storage, service registration and inspection tools, paginated MCP resources,
restart recovery tests, a host-configurable outbound channel policy, and a
generated catalog inventory.

Phase 2 delivers the full record-replay-promote loop:

- `FileSystemArtifactRepository` (surface/grpc/recipe): content-addressed
  artifact storage. Content lives at `<root>/<sha256>` with a reference proto
  sidecar, writes are atomic (temp file plus move), duplicate content shares one
  identity, and `find` re-hashes content and re-validates the stored reference
  so tampering fails loudly. A 16 MiB bound (`RecipeValidation.MAX_ARTIFACT_BYTES`)
  rejects oversized artifacts.
- `ChainRecipeCompiler` (transform/chain): compiles a resolved `ChainDefinition`
  into the durable `GrpcRecipe` contract. Each distinct service FQN becomes one
  `ServiceDependency`, endpoints are sanitized into portable aliases, and the
  descriptor fingerprint is the sha256 of a canonically sorted
  `FileDescriptorSet`, so compilation is deterministic.
- `RecipeReplay` (transform/chain): offline verification of recorded runs. It
  reuses `ScopedProtoMapper`, `MessageScope`, and `CelProtoMapper` so replay
  mapping semantics are identical to the live `ChainRunner`, re-evaluates step
  gates, and reports verdicts as data (`ReplayResult`/`StepReplay`) rather than
  throwing. Altered request, response, or descriptor evidence fails clearly.
- Registry promotion (`GitSchemaRegistryStore`): recipes are stored as binary
  `recipes/<name>/<version>.pb` objects. Promotion is immutable (an identical
  re-save is a no-op, a divergent one is rejected), validated on both write and
  read, and exposed through the `RegistryRecipeRepository` adapter.
- `MappingSuggester` (transform/shapes): descriptor-grounded mapping
  candidates over name-normalized fields, one nesting level deep, requiring
  exact type, cardinality, and message-type agreement. Maps, `Struct`, and
  `Any` are never suggested, and every candidate is gated through the same
  `RuleChecker` the compiler uses, so a suggestion can never bypass validation.
- MCP integration (#92): five workbench actions, `suggest-mappings`,
  `compile-recipe`, `record-recipe-run`, `replay-recipe`, and
  `promote-recipe`, are exposed through MCP, typed gRPC, REST, and OpenAPI. The
  initialize response teaches the full discovery-to-promotion path, while the
  host owns a persistent recipe workspace containing artifacts, run evidence,
  and registry state. `ChainRunner` observation records sensitivity-redacted
  fixtures; unsafe run/version identities, excessive mapping sources,
  unresolved `Any`, and unredacted sensitive data fail before persistence.
  The acceptance test drives two live gRPC services through streamable HTTP
  MCP, executes all five actions, restarts the host, and recovers the exact
  promoted recipe fingerprint from registry storage.
