# Optional action providers

ProtoMolt's action catalog supports runtime composition through `ActionProvider`.
The catalog provides authorization, scope budgets, request-contract validation,
and dispatch. Optional providers contribute actions using the same path.

## Choose the catalog you need

`ActionCatalog.empty(context)` starts with no actions. Register the operations
required by your service. Use `empty(context, budgets)` when several transports
must share a caller's scope-budget ledger. An empty catalog does not bypass
request validation or authorization.

`ActionCatalog.defaults(context)` registers the core toolkit actions plus
providers visible to the thread context class loader. Provider classes are sorted
by fully qualified class name; each provider returns its actions in a stable
order. Optional actions occupy the slot after `render-prompt` and before
`eval-cel`, retaining the previous position of `render-index-mappings` in the
full distribution.

Only installed providers appear in the catalog manifest. Duplicate names and
provider-loading errors fail construction; they do not silently override an
action or produce an apparently complete catalog.

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

The catalog still includes core toolkit implementations and schema-source helpers.
This first extraction removes the concrete indexing dependency; it does not claim
that a fully minimal gRPC/MCP/ACP distribution has already been built.

## Separate response-validation follow-up

During this change, a probe confirmed that direct typed `ActionCatalog.execute`
calls validate the request but do not independently enforce the declared response
type. `executeStreaming` similarly forwards typed emissions. This behavior existed
before provider discovery. It must not be generalized to workflow or transport
validation boundaries, which have their own checks. Response-contract hardening
is tracked in [Forgejo #293](https://git.rokkon.com/ai-pipestream/protomolt/issues/293)
and requires unary and streaming regression tests and correct
server-error classification; this refactor does not silently change that behavior.
