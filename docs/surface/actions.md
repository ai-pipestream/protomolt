# Actions

`protomolt-actions` is a catalog of verbs over the toolkit: compile,
validate, diff, check, render, evaluate: with one JSON envelope in and one
out. It exists for the edges: the registry console drives it over HTTP, and
an MCP mount can expose the same catalog as tools for LLM-driven schema
work. JSON is deliberately confined to this layer; every action wraps a
descriptor-native library underneath, and machine-to-machine paths should
prefer the binary endpoints (see [The registry](../schema/registry.md)).

```java
var catalog = ActionCatalog.defaults(ActionContext.create());
ObjectNode result = catalog.execute("check-compat", input);
```

Each action declares a name, a description written for tool use, and a JSON
Schema for its input, so `list()` is a complete, self-describing tool
manifest. Failures are structured: `{error, message, details?}` with
stable kebab-case codes (`unknown-type`, `invalid-input`, `compile-failed`,
`invalid-expression`, `mapping-failed`, …): distinct codes for distinct
repair strategies, which matters when the caller is a model.

## The built-in catalog

`ActionCatalog.defaults(...)` registers the built-in actions: the ones that need
nothing beyond a descriptor and the toolkit's own libraries. The generated
[action inventory](../generated/action-inventory.json) is the authoritative list
for this and every assembled surface:

| Action | Does |
|---|---|
| `compile` | Compile inline `.proto` sources; returns file names and a base64 descriptor set |
| `list-types` | Enumerate messages, enums, and services with fields: the introspection/grounding verb |
| `validate-message` | Validate a JSON message against the rules on its schema; returns structured violations |
| `diff-schemas` | Typed change list between two schemas (rule, path, impacts) |
| `check-compat` | Compatibility verdict under a mode, with violations and the full change list |
| `render-json-schema` | JSON Schema (2020-12) for a message type |
| `render-prompt` | Render a descriptor-grounded LLM prompt for a message type |
| `render-index-mappings` | OpenSearch mappings / Solr schema / Lucene field specs from indexing hints |
| `eval-cel` | Evaluate a CEL expression against a message |
| `map-message` | Apply text and CEL mapping rules to a message |
| `synthesize-shape` | Derive a join/union output type (envelope, projection, or oneof union) from named sources; returns registrable proto source and implied rules |
| `join-messages` | Join named source messages into an authored target or a synthesized shape with scoped rules and CEL |
| `merge-schemas` | Merge two or more message types into one new type: clash report, caller-decided resolutions, then merged proto + join/union rulesets in one move |
| `check-rules` | Statically validate mapping rules and CEL (filters must be bool) against descriptors; sample messages upgrade the check to a dry run |
| `infer-schema` | Struct-to-proto: reverse-engineer a message type from JSON sample documents; returns registrable source + descriptor set |
| `mask-message` | Mask fields by their schema-declared sensitivity classes: remove, redact, or encrypt/decrypt (AES-GCM, field-bound versioned envelope), recursively including map values and `google.protobuf.Any` payloads whose type is resolvable; payloads that are not are reported, never quietly passed |
| `extract-metadata` | The declared metadata bag for a type |

## Verbs from other modules

Additional actions live in the modules that carry their dependencies, so a host
that only needs the built-ins does not pull them in. Each implements the same
`ProtoAction` interface and is registered onto a catalog by the host:

| Action | Module | Does |
|---|---|---|
| `reflect` | `protomolt-grpc-invoke` | Fetch a live gRPC server's schema over server reflection, given only its address; servers without reflection return `ok: false` |
| `grpc-invoke` | `protomolt-grpc-invoke` | Call a unary or server-streaming gRPC method with no generated stubs; requests and responses are proto3 JSON |
| `generate-stubs` | `protomolt-codegen` | Generate message code in java, kotlin, python, cpp, csharp, ruby, php, and objc, plus `grpc-java` service stubs, using protoc as WebAssembly |
| `gather-git` | `protomolt-acquire-gather-git` | Gather `.proto` sources from a git repository (branch, tag, or commit) and compile them; returns the source texts plus a descriptor set usable as a schema input |
| `run-workflow` | `protomolt-workflow` | Execute a workflow: serial unary gRPC calls, each request mapped from the workflow input and prior steps' responses, with gates, per-step validation, and nested deadlines |
| `check-workflow` | `protomolt-workflow` | Verify a workflow without running it: methods resolve and are unary, step names are valid scope variables, gates are boolean CEL, and every mapping type-checks |
| `compile-workflow` | `protomolt-workflow` | Compile a checked workflow into the deterministic descriptor-grounded workflow contract |
| `suggest-mappings` | `protomolt-workflow` | Propose conservative descriptor-compatible mapping candidates and pass each through the workflow rule checker |
| `record-workflow-run` | `protomolt-workflow` | Execute a live workflow and persist sensitivity-redacted, content-addressed fixtures plus immutable run evidence |
| `replay-workflow` | `protomolt-workflow` | Replay recorded fixtures offline and report workflow, schema, request, response, mapping, or ordering drift |
| `promote-workflow` | `protomolt-workflow` | Store validated workflow content as an immutable version in the mounted git registry |
| `export-work-record` | `protomolt-workflow` | Project a recorded run's evidence into a canonical signed work record that verifies offline |
| `verify-work-record` | `protomolt-workflow` | Verify a signed work record against a caller-supplied trust snapshot with zero network calls |
| `evaluate-work-record` | `protomolt-workflow` | Evaluate a signed record beside its stored evidence: verification, reprojection match, and offline replay under the predeclared policy |
| `emit-okf` | `protomolt-emit-okf` | Render a schema as an Open Knowledge Format (OKF v0.1) bundle: linked markdown concept documents for every message, enum, and service, inline plus zipped |
| `submit-workflow` | `protomolt-jobs-service` | Submit a workflow for durable asynchronous execution |
| `get-job` | `protomolt-jobs-service` | Read one workflow run and its step checkpoints |
| `list-jobs` | `protomolt-jobs-service` | List durable workflow runs |
| `complete-step` | `protomolt-jobs-service` | Resume a parked external-completion step with its result |
| `inference-generate` | `protomolt-inference-service` | Run generation through a configured inference provider |
| `inference-list-models` | `protomolt-inference-service` | List configured inference models without exposing provider credentials |
| `inference-describe-model` | `protomolt-inference-service` | Inspect one configured model and its capabilities |
| `service-register` | `protomolt-grpc-service-workspace` | Reflect a gRPC endpoint and persist its profile plus a content-addressed descriptor artifact |
| `service-list` | `protomolt-grpc-service-workspace` | List durable service identities and descriptor fingerprints |
| `service-inspect` | `protomolt-grpc-service-workspace` | Read a service's methods, streaming modes, and request/response field shapes without returning descriptor bytes |
| `service-refresh` | `protomolt-grpc-service-workspace` | Re-reflect a registered endpoint and update its schema identity when it changed |
| `service-invoke` | `protomolt-grpc-service-workspace` | Invoke a registered service while resolving its pinned descriptor inside ProtoMolt |

`ProtoMoltCatalog.full(...)` in `protomolt-grpc-service` assembles the full
catalog behind the gRPC service, its REST mount, and the console. The standalone
[MCP server](mcp.md) registers its host-independent subset. The generated
[action inventory](../generated/action-inventory.json) records both exact name sets.

```java
var catalog = ProtoMoltCatalog.full(ActionContext.create());
```

## Verbs contributed by a composed node

A role module can contribute a `ProtoAction` at wire time, and the host
registers it onto the catalog it built. These verbs therefore exist only on a
node that mounts the owning role: a registry-only node has none of the search
verbs, a node without the delegation runtime has none of the delegation verbs.
They are ordinary catalog entries in every other respect: same envelope, same
JSON Schema manifest, same scope check, same error codes.

### Delegation

Contributed by `protomolt-delegation` through `DelegationActions.register`,
against the node's one delegation coordinator. Every one requires
`worker-coordinate`.

Each verb's envelope is the canonical proto3 JSON of a request message
declared in `delegation_actions.proto`, and its published input schema is
derived from that message, so the bounds a caller reads are the bounds the
verb enforces. The verbs carry the stream contract's own payload types
(`TaskSpec`, `CompletionCandidate`, `CheckpointReference`, `WorkerCapability`)
rather than restating them.

| Action | Does |
|---|---|
| `delegation-offer` | Offers a bounded task to an admitted worker: objective, scope, constraints, acceptance checks, context |
| `delegation-accept` | The worker takes the open offer's attempt lease |
| `delegation-cancel` | Cancels a task's open attempt with a bounded reason; terminal the moment the coordinator emits it |
| `delegation-candidate` | Submits a completion candidate for review against an expected revision |
| `delegation-checkpoint` | Records one resumable checkpoint on the leased attempt: resume token, note, optional state artifact |
| `delegation-message` | Sends a non-transitioning task message in either direction, worker question or coordinator guidance |
| `delegation-progress` | Reports one bounded progress note on the leased attempt, in a strictly increasing sequence |
| `delegation-review` | Applies the verdict on an open candidate: accept with a verdict line, or revise with feedback |
| `delegation-transcript` | Reads the recorded transcript from a cursor, optionally for one task |
| `delegation-watch` | Long-polls the event feed: blocks until an event appears after a cursor, then returns a bounded batch |
| `delegation-worker-list` | Lists registered workers with identity, admission and connection state, and lease state |
| `delegation-worker-register` | Registers this agent as a worker: opens the worker stream and sends the hello |

### Mesh cluster

Contributed by `protomolt-mesh-cluster` through `ClusterActions.register`,
against the node's cluster directory. Every one requires `worker-coordinate`.

Each verb's envelope is the canonical proto3 JSON of a request message
declared in `cluster_directory_service.proto`, and its published input schema
is derived from that message. The same file declares
`ClusterDirectoryService`, so a caller may reach the directory either as a
catalog verb or as a typed RPC over that service, against one contract. Every
mutating answer carries a `DirectoryCommit` and an `ApplyOutcome`, which
distinguishes a change that was applied from one refused by a stale fencing
token.

| Action | Does |
|---|---|
| `mesh-node-register` | Registers or refreshes one fenced mesh node advertisement after durable validation |
| `mesh-node-heartbeat` | Extends one registered node's liveness window with a fenced heartbeat |
| `mesh-processor-register` | Registers or renews one health-gated processor lease on a registered node |
| `mesh-capacity-update` | Publishes a fenced point-in-time node or processor capacity snapshot |
| `mesh-snapshot` | Returns the deterministic cluster directory snapshot and eligibility state |
| `mesh-sweep` | Expires elapsed processor leases and node presence windows, cascading node loss |

### Metrics

Defined in `protomolt-metric-service` and contributed by the `metric` role
module in `protomolt-metric-lucene`. See
[metric mappings](../design/metric-mapping.md).

Each verb's envelope is the canonical proto3 JSON of a request message
declared in `metric_service.proto` (`QueryMetricsRequest`,
`DescribeMappingRequest`, `RebuildRollupRequest`), and its published input
schema is derived from that message. The bounds the schema advertises are the
declared ones: a query names at most 100 measures, 32 group-by dimensions, and
64 filters, and the same limits hold whether the request arrives on the
catalog route or as a `MetricService` RPC.

| Action | Scope | Does |
|---|---|---|
| `describe-mapping` | `metrics-query` | One subject's queryable surface: members, roles, aggregates, descriptions, sensitivity, mounted backends |
| `query-metrics` | `metrics-query` | One aggregate query over a subject: measures, group-by dimensions with grains, filters, bounded limit |
| `rebuild-rollup` | `metrics-rebuild` | Runs a complete aggregate and atomically replaces a declared lake rollup table |

### Registry

Contributed by the registry role in `protomolt-registry-service`. Every one
requires `schema-write`.

Each verb's envelope is the canonical proto3 JSON of a request message
declared in `registry_admin.proto`, and its published input schema is derived
from that message. `registry-remotes` names its operation with the
`RemoteOperation` enum rather than a bare string, and the message states in
its own rules which fields each operation requires, so an add without a URL or
a remove naming no remote is refused by the contract rather than by a check
inside the verb. `registry-sync` answers with per-subject `ImportedSubject`
detail, not a count.

| Action | Does |
|---|---|
| `publish-config` | Publishes one typed config document through the registry's config gate, parsed strictly as its declared message type |
| `registry-remotes` | Manages the git remotes this registry federates from: list, add, remove |
| `registry-sync` | Fetches a configured remote and imports its subjects as `<remote>:<subject>` with its descriptor artifacts |

### Search

Contributed by the search role in `protomolt-search-service`.

Each verb's envelope is the canonical proto3 JSON of a request message
declared in `search_service.proto` (`SearchRequest`,
`ReplayDocumentsRequest`), and its published input schema is derived from that
message. `search` names its lane with the `SearchLane` enum, the same
vocabulary the RPC surface uses. `ReplayDocuments` is also an RPC on
`SearchIndexService`; its contract states that a scoped replay sets a drive
while a replay that prunes covers the whole repository and sets neither drive
nor account, so the two modes cannot be combined by accident.

| Action | Scope | Does |
|---|---|---|
| `search` | `search-query` | Searches one mapping subject on the lexical, vector, or hybrid lane and returns hits with typed stored fields |
| `replay-documents` | `search-index` | Re-runs a stored workflow over every document a repository listing matches, one durable run each, to re-derive search state |

### Pull connectors

Contributed by the connector role modules. Both require `service-invoke`.

Each verb's envelope is the canonical proto3 JSON of a request message
declared in `pull.proto` (`PullFromS3Request`, `PullFromJdbcRequest`), and its
published input schema is derived from that message. Both answer with a
`PullReport`: `submitted`, `deduplicated`, `failed`, per-item `errors`, and the
`watermark` to hand back on the next pass.

| Action | Module | Does |
|---|---|---|
| `pull-s3` | `protomolt-acquire-s3` | Pulls objects new or changed past a watermark from an S3 bucket and feeds them through intake with stable identity |
| `pull-jdbc` | `protomolt-acquire-jdbc` | Runs a watermark query against a source database and feeds each row through intake with stable identity |

### How these are reached

`ProtoMoltRestMount` mounts the typed surface by iterating the method
descriptors of `ProtoMoltService`, the hand-maintained proto service contract.
A contributed verb has no RPC there, so it does not appear on the typed gRPC
service, the `/grpc-json` REST routes, the generated OpenAPI document, or
Swagger UI. It is reached instead through:

- the registry's actions route, `GET /protomolt/actions` and
  `POST /protomolt/actions/{name}`, which serves whatever catalog the host
  mounted on the node;
- any surface the host hands that same catalog to, which on a composed node
  is the MCP endpoint, and which is equally the ACP agent and the CLI when
  they are constructed over it rather than over `ProtoMoltCatalog.full`.

Note for anyone verifying this page: the generated
[action inventory](../generated/action-inventory.json) enumerates the static
catalogs only (defaults, standalone MCP, full). Contributed verbs are not in
it, so the test that pins this document against the inventory cannot notice a
contributed verb going undocumented. Adding one to a role module means adding
it here by hand.

Wherever an action takes a schema it accepts exactly one of three forms,
`{"type": "fully.qualified.Name"}` (resolved from the context's descriptor
registry), inline `{"sources": {...}, "root": ...}` (compiled per call), or
`{"descriptorSetBase64": ...}`. Inline and binary schemas are re-parsed
with the toolkit's option extensions registered, so validation rules,
metadata, and indexing hints behave identically however the schema arrived.

## Required scopes

Every action declares the authorization scope it requires (`requiredScope()`)
from the closed vocabulary in the
[authorization scopes design](../design/authorization-scopes.md). Dispatching
as a scoped caller (`ActionCatalog.execute(name, input, caller)`) refuses
before the action runs when the caller does not hold that scope, with the
stable error code `permission-denied` naming the caller, the scope, and the
action; `list(caller)` serves only the actions the caller may execute. The
caller-less forms dispatch with process authority (`Caller.operator()`),
which is what the CLI and the stdio MCP server run as. An action that keeps
the blank default declaration is served under process authority and refused
by name for every scoped caller, so an undeclared plugin never grants
silently.

## Streaming actions

An action that produces results incrementally implements `StreamingAction`:
`executeStreaming(input, context, emitter)` emits one document per result as
it arrives, while the unary `execute` contract stays unchanged for collecting
fronts (REST, MCP). `ActionCatalog.executeStreaming(name, input, emitter)` is
the dispatch point; unary actions emit their single result, so streaming
fronts (the ACP agent) get one contract for every verb. `grpc-invoke` is the
first streaming action: server-streaming methods emit per response message
and every run ends with a terminal status document.

## The HTTP mount

Constructing the registry server with a catalog mounts it under the native
prefix: `GET /protomolt/actions` lists the manifest, and
`POST /protomolt/actions/{name}` executes: `unknown-action` maps to 404,
`invalid-input` to 400, other action errors to 422.

```java
var server = new SchemaRegistryServer(config, store,
        ActionCatalog.defaults(ActionContext.create()));
```
