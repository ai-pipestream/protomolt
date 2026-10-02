# Optional action providers

ProtoMolt's action catalog supports runtime composition through `ActionProvider`.
The catalog provides authorization, scope budgets, request-contract validation,
and dispatch. Optional providers contribute actions using the same path.

## Choose the catalog you need

`ActionCatalog.empty(context)` starts with no actions. Register the operations
required by your service. Use `empty(context, budgets)` when several transports
must share a caller's scope-budget ledger. An empty catalog does not bypass
request validation or authorization.

`ActionCatalog.defaults(context)` registers actions supplied by
providers visible to the thread context class loader. Provider classes are sorted
by fully qualified class name; each provider returns its actions in a stable
order. No provider means an empty catalog. Use `empty` when composing entirely
through explicit registration. Full-catalog ordering follows provider class order,
then each provider's action order; callers should identify tools by name.

Only installed providers appear in the catalog manifest. Duplicate names and
provider-loading errors fail construction; they do not silently override an
action or produce an apparently complete catalog.

## Toolkit and schema-source migration

`protomolt-actions` contains action types, dispatch, authorization, budgets and
descriptor-based contract validation. Install the toolkit actions explicitly:

```groovy
runtimeOnly "ai.pipestream:protomolt-actions-toolkit:${protomoltVersion}"
```

That module includes `protomolt-actions-schema` at runtime. Applications using
`SchemaResolver` or named `CatalogContract.request(String)` lookups without toolkit
actions can install `protomolt-actions-schema` alone. It supplies
`SchemaResolverProvider` and `ActionContractProvider`; missing or ambiguous schema
providers and missing or ambiguous named contracts fail explicitly. Existing public
facades and protobuf definitions retain their names. Descriptor-native custom actions
do not need either provider and can use the core alone.

Registration resolves an action's request and response descriptors before installing
it. Broken contract providers therefore fail during catalog construction. Provider
constructors must be stateless and must not acquire network or worker resources.

## Index-rendering migration

The standalone `protomolt-actions` dependency no longer brings in the Lucene,
OpenSearch, Solr and Qdrant implementation modules. To keep the
`render-index-mappings` action in a standalone catalog, explicitly include:

```groovy
runtimeOnly "ai.pipestream:protomolt-actions-index:${protomoltVersion}"
```

Use the same version as the other ProtoMolt modules or import the ProtoMolt BOM.
Maven consumers can add the same artifact with runtime scope.

The full gRPC service, MCP launcher and registry service explicitly include this
provider, so their existing index-rendering behavior is retained. The thin
`protomolt-grpc-invoke` module does not include it.

This is a dependency-composition change for standalone catalog consumers. Existing
public action types and protobuf definitions are unchanged. The artifact must be
published before outside consumers can use the new dependency; a source PR alone
is not a release.

## Add a provider

Implement `ai.protomolt.proto.actions.ActionProvider` with a public no-argument
constructor and return the actions from `actions()`. Add its fully qualified
class name to this resource in your JAR:

```text
META-INF/services/ai.protomolt.proto.actions.ActionProvider
```

Provider code executes inside the host process. Treat it as trusted application
code; ServiceLoader is not a sandbox. Discovery occurs when constructing a
catalog. Changing the classpath does not hot-reload an existing catalog. Explicit
`register` and `replace` remain available for controlled changes to a running
catalog.

## Dependency boundary checks

`:protomolt-grpc-invoke:checkRuntimeBoundaries` rejects concrete search adapters
and Lucene libraries from the resolved production runtime graph. Both `test` and
`check` run this gate. Index-provider tests use the full test runtime; the gate
inspects the production configuration so test-only dependencies cannot hide a
runtime dependency leak.

`:protomolt-actions:checkRuntimeBoundaries` rejects toolkit implementations,
compilation, indexing, workflow and named service-contract dependencies in the core
runtime. `:protomolt-actions:boundaryTest` runs a descriptor-native action in a separate
test JVM without any optional action, schema or contract providers. The broader action
suite exercises the installed toolkit and index providers.

A fully minimal gRPC/MCP/ACP distribution remains separate work.

## Separate response-validation follow-up

During this change, a probe confirmed that direct typed `ActionCatalog.execute`
calls validate the request but do not independently enforce the declared response
type. `executeStreaming` similarly forwards typed emissions. This behavior existed
before provider discovery. It must not be generalized to workflow or transport
validation boundaries, which have their own checks. Response-contract hardening
is tracked in [Forgejo #293](https://git.rokkon.com/ai-pipestream/protomolt/issues/293)
and requires unary and streaming regression tests and correct
server-error classification; this refactor does not silently change that behavior.
