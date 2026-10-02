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

## Response validation

Typed requests also use the action's declared descriptor. A caller cannot omit
validation rules by providing a same-named descriptor with different annotations.
The catalog decodes distinct descriptor instances against the declared contract
before validation and execution; identical instances retain their representation.
Null requests fail with `invalid-input` before the handler runs.

Direct typed and JSON catalog calls validate successful responses against the
action's declared response descriptor before returning or rendering them.
Null responses, wrong types and validation failures report `invalid-response`.
Distinct descriptor instances are decoded against the declared descriptor before
validation, so same-named descriptors cannot remove its rules. Generated messages
using the declared descriptor retain their instance; other responses may return a
canonical dynamic message.

Every streaming emission passes the same check before transport delivery. The
first validation or transport ActionException is terminal: subsequent emissions
fail, and a provider cannot catch that failure and report successful completion.
Earlier valid emissions cannot be withdrawn. Emission after execution completes
is rejected. This validation checks protocol contracts, not semantic correctness,
and does not undo side effects already performed by a handler.

An invalid action response is a server failure: gRPC DATA_LOSS and registry HTTP
500. MCP/ACP preserve the action error through their existing error paths.
The catalog now rejects invalid delegation replies before the gRPC response gate;
their gRPC status remains DATA_LOSS, with the catalog's `invalid-response` error
code replacing `invalid-upstream-response` for these failures.
The work addresses [Forgejo #293](https://git.rokkon.com/ai-pipestream/protomolt/issues/293).
