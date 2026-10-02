# SPI composition and dependency boundaries

This work separates reusable protocol and dispatch code from optional capabilities.
Java SPI discovers trusted implementations installed in the application. gRPC remains
the boundary for services implemented in other languages or running on other nodes.

## Required outcomes

1. **Actions:** keep public action types, dispatch, authorization, budgets and contract
   validation usable without toolkit implementations. Load action families through
   `ActionProvider`; make schema compilation and named toolkit contracts optional.
2. **MCP:** separate protocol handling from registry, workspace, workflow, delegation,
   code generation and Git capabilities. Preserve a full assembly and provide a
   minimal, usable gRPC integrator assembly.
3. **Service modules:** reuse `ServiceModule`, `NodeContext`, `ServiceMount` and
   `Composer` for configured capability discovery and lifecycle. Remove concrete
   capability construction from general launchers where it prevents independent use.
4. **Registry:** separate store interfaces from Git and workflow implementations so
   applications select their storage without taking unrelated implementation dependencies.
5. **CEL:** extract shared CEL support used by mapping and validation, preserving
   validation semantics and conformance.

## Rules for implementation

- An SPI/core module must not depend on its implementation modules. Application
  assemblies select implementations with runtime dependencies.
- Do not split a production Java package across modules. Preserve existing public
  types where feasible; record required dependency or source migration explicitly.
- Discovery must not start workers, open network connections or load models.
  Configure selected capabilities before starting them. Unselected capabilities must
  not require valid operational configuration.
- Missing selected providers, duplicate identities, invalid configuration and provider
  construction failures must be explicit failures. Do not silently select a different
  provider or replace invalid values with guessed configuration.
- Preserve causes when translating failures. Cleanup failures must remain observable;
  preserve the original failure and attach cleanup failures where appropriate.
- Keep validation and authorization in the dispatch boundary. Providers must not
  advertise capabilities that are not installed or ready to be served.
- Preserve shared budget accounting, cancellation and resource ownership. Shutdown
  and failed startup must release acquired resources in reverse order.
- A module extraction must remove dependencies for a supported consumer or establish
  an independently useful capability. Class splitting alone is not a packaging result.

## Verification

For each boundary, test a minimal consumer without optional implementation JARs and
a full assembly with its existing capabilities. Exercise missing, duplicate and broken
providers. Check generated Maven and Gradle metadata, classpath dependency gates,
existing affected tests and startup/shutdown behavior. Record before/after dependency
counts separately from runtime or performance claims.

The extracted boundaries and their verification paths are described below.
Local checks do not establish publication or deployment; consumers of published
artifacts must use a version containing the relevant modules.

## MCP transport boundary

`protomolt-mcp-transport` contains JSON-RPC handling, MCP sessions, stdio,
resource composition and the catalog-backed workspace resource. Its production
dependencies are the action core and SLF4J. It does not discover or construct
registry, workflow, delegation, Git, indexing or code-generation implementations.
An embedding application supplies its `ActionCatalog` and optional `McpResources`.
The runtime dependency gate rejects those optional implementation modules.

The existing `protomolt-mcp` remains the full distribution. Its public server,
session and resource types delegate to the transport module, retaining the
original resource page type and registry constructor. Existing callers need no
import changes. New minimal embeddings use `ai.protomolt.proto.mcp.transport`.
The `protomolt-grpc-mcp` assembly loads the `grpc-invoke` service module at runtime,
requiring an explicit target and transport. See [gRPC tools over MCP](../apps/grpc-mcp.md)
for its scope and local build instructions.

Unexpected asynchronous response failures are retained by the session even after
the failed task leaves the in-flight map. Subsequent handling, submission,
dispatch, draining and close report the original cause. Additional concurrent
failures are logged. A drain timeout or interruption is explicit, and interruption
preserves the thread flag. Normal tool error responses and explicit request
cancellation do not poison a session. Cancellation cleanup removes only its own
request, preserving a later request that reused the same ID.

Composer shutdown attempts all resources and channels before reporting cleanup
failures. A startup failure retains its original cause and carries cleanup
failures as suppressed exceptions. Channel termination timeout and interruption
are failures; interruption preserves the thread flag.

`PROTOMOLT_ROLES` requires a name between every comma. Leading, trailing or repeated
separators and whitespace-only entries fail before any role is wired. Spaces around
valid names and case normalization remain supported. A comma-only value cannot
start a node with no mounted capabilities.

## Shared CEL foundation

`protomolt-cel` supplies the existing environment factory, evaluator, expression
validation and exception types in `ai.protomolt.proto.cel`. It depends on CEL,
protobuf and SLF4J, with no mapping implementation. Protobuf validation now uses
this foundation directly; a runtime gate prohibits both mapper modules. Quality,
metric and parser routing code also select the foundation where mapping is unused.

`protomolt-mapper-cel` supplies `CelProtoMapper` and `CelMappingRule` in
`ai.protomolt.proto.mapper.cel`, depending on the foundation and field mapper.
**Java migration:** callers of these two classes must update their imports from
`ai.protomolt.proto.cel` and recompile. Public Java signatures referring to them
change with the package move. Keeping wrappers in the old package would split
that package across JARs, against the module rules. Shared evaluator types retain
their names; consumers using only those types can depend on `protomolt-cel`.
No protobuf definitions or stored descriptor identities change in this extraction.

The extraction preserves evaluation behavior in a separate commit. The subsequent
failure-handling change makes `evaluateBoolean`, `tryMap` and `mapFirstCandidate`
propagate compilation and evaluation errors. Only a successfully evaluated false
filter skips a candidate; a malformed expression cannot select a later fallback.
Filtered-out selectors remain unevaluated. Callers relying on error-driven fallback
must express the intended presence or eligibility condition as a filter.

## Registry storage boundary

`protomolt-registry` contains storage contracts, in-memory storage and shared
registration/descriptor-set validation. Its runtime dependency gate rejects
JGit and the Git and workflow implementation modules.

`protomolt-schema-registry-git` contains `GitSchemaRegistryStore`,
`RegistryFederation` and `RegistryWorkflowVersionRepository`. Applications that
use these implementations must select the optional module explicitly. This
module registers a storage provider for the generic registry role described below.
Existing full launchers retain their explicit Git assembly.

**Java migration:** the three implementation classes move from
`ai.protomolt.proto.registry` to `ai.protomolt.proto.schema.registry.git`.
Consumers must add the new dependency, update imports and recompile. Storage
contracts retain their package. Existing persisted formats and protocol definitions
are unchanged by this extraction. `RegistrationSupport` and `DescriptorSetArtifacts`
are public shared helpers so backends reuse the core checks.

A shared test-fixtures variant exercises the same storage contract against both
in-memory and Git implementations. Test dependencies remain separate from the
production runtime. The Git module includes workflow version storage and the
gRPC workflow contracts. It does not depend on the workflow execution engine.
Moving the repository adapter by itself would not remove those contracts: the
store uses their validation and versioned message format for workflow documents.

Registry metadata parsing also rejects malformed stored compatibility policies and
invalid global counters instead of treating them as defaults. Absence of a
subject override still means inheritance; a malformed override does not. A listed
schema version missing from storage fails history assembly so the compatibility
gate cannot evaluate an incomplete history.

Optional `WorkflowDocumentStore` and `ConfigDocumentStore` capabilities extend
the schema store contract for document storage. HTTP routes select these
interfaces rather than checking for Git. Non-Git test backends exercise workflow
put/get/list and typed-config HTTP/action publication. Git implements both.
`PublishConfigAction` now accepts `ConfigDocumentStore`; source callers passing a
Git store still compile, but existing binaries must be recompiled for the changed
constructor signature. Publication errors retain their original cause.

The generic `protomolt-registry-service` no longer assembles Git federation,
workflow runtime or toolkit/index action providers. The optional
`protomolt-schema-registry-git-service` supplies `RegistryModule`,
`RegistryRemotesAction` and `RegistrySyncAction` in
`ai.protomolt.proto.schema.registry.git.service`; consumers of those classes
must add that artifact, update imports and recompile. The document-platform
assembly selects it explicitly. HTTP callers may supply a catalog containing
only the actions they intend to expose.

`protomolt-emit` no longer includes Git delivery. `GitSink` moves to
`protomolt-emit-git`, in `ai.protomolt.proto.emit.git`; callers must add that
artifact and update the import. This also removes JGit from Parquet emission
and the registry HTTP service's Parquet schema endpoint. Runtime gates reject
Git dependencies from emit core and the generic registry HTTP service.

Configuration type resolution reports corrupt stored schemas and missing listed
subjects as storage errors. It does not skip them and return a misleading
"type not found" result. Invalid JSON and protobuf config input retain their
parse causes in `InvalidConfigException`.

## Registry storage provider discovery

`SchemaRegistryStores.discover()` loads `SchemaRegistryStoreProvider` factories
through Java SPI. Discovery does not open stores. `open(id, options, writeGate)`
requires an explicit installed provider ID and returns a caller-owned store.
Duplicate/malformed IDs, missing providers, null results and provider failures
are errors; there is no fallback backend. The caller supplies the compatibility
write gate explicitly, including null when no gate is intended.

Registry core registers the `memory` provider, which accepts no options. Installing
`protomolt-schema-registry-git` also registers `git`. That provider requires all
three options: `directory`, `author-name` and `author-email`. Unknown, missing or
blank options fail before the repository is opened. Existing direct builders keep
their API; this factory path does not choose an implicit commit author.

The factory API supports application wiring through the generic registry role.
The separate Git service assembly also provides federation and workflow integration
for the full document-platform application.

`protomolt-schema-registry-composer` supplies the generic `registry` role through
`ServiceModule` SPI without adding Git or workflow dependencies to the HTTP
library. An embedding application includes this module and uses
`Composer.builder().environment(environment).build().bootFromEnvironment()`.
The existing document-platform continues to select its Git-specific assembly.

The generic role requires these environment settings:

- `PROTOMOLT_ROLES=registry`
- `PROTOMOLT_REGISTRY_STORE=memory` (or another installed provider ID)
- `PROTOMOLT_REGISTRY_HOST=127.0.0.1`
- `PROTOMOLT_REGISTRY_PORT=8081`
- `PROTOMOLT_REGISTRY_AUTH=token` and `PROTOMOLT_REGISTRY_TOKEN` set to the secret;
  `AUTH=none` is an explicit unauthenticated choice and rejects a supplied token.
- `PROTOMOLT_REGISTRY_COMPATIBILITY=wire` for the compatibility write gate;
  `none` explicitly disables that gate. Schema compilation still runs.

Provider settings use `PROTOMOLT_REGISTRY_OPTION_` followed by the uppercase
option name with hyphens replaced by underscores. Git therefore requires
`OPTION_DIRECTORY`, `OPTION_AUTHOR_NAME` and `OPTION_AUTHOR_EMAIL` under that
prefix. Unknown registry settings and unknown provider options fail. The HTTP
protocol uses `/health`, `/protomolt` and the documented 16 MiB request cap.

Wiring opens only the selected store and registers cleanup immediately. HTTP
starts after all selected roles have contributed actions. The catalog includes
those contributions rather than loading every installed action provider. A config
storage capability contributes `publish-config`; the generic role does not supply
Git federation or workflow execution. Multiple action contexts or caller resolvers
fail instead of selecting the first. Startup failure and shutdown release the
server and store through the composer's cleanup stack.

## Explicit registry HTTP configuration

`SchemaRegistryServerConfig` rejects null or blank hosts and endpoint paths.
Supplying an empty host no longer binds every interface, and missing paths no
longer select endpoints implicitly. `defaults()` remains an explicit factory for
the documented conventional configuration; callers can also supply each field.

A non-null authentication token must be nonblank. An empty secret therefore
fails configuration instead of starting an unauthenticated service. Java callers
must pass null deliberately to disable token authentication; the generic registry
role requires `AUTH=none` for that choice. Token values are neither trimmed nor
included in configuration errors. Existing code that supplied missing values to
request defaults must use `defaults()` or pass the intended values explicitly.

The serve and document-platform launchers also reject blank operator tokens.
For an open node, omit `PROTOMOLT_API_TOKEN` and `--api-token` rather than supplying
an empty secret. Java options accept null for that choice. The document-platform
environment parser preserves the exact token value instead of trimming it.
