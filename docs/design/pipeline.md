# Pipeline: a type-checked composition language that is data, not syntax

Status: design proposal. No code yet — this document is the artifact to react to.

ProtoMolt composes gRPC calls and CEL today in two places that deliberately
stop short of being a language: the chain manager (typed gRPC composition,
"deliberately not a pipeline") and the mapping engines (CEL rules, no call
graph). The missing middle is a composition an agent can author, a machine can
type-check *before* it touches a live service, and the registry can version.

The proposal: the composition language is a **protobuf schema**, authored as
JSON/proto-text. No parser, no grammar, no syntax-error failure class.
Structural validation comes from the schema itself; semantic validation (the
"strongly typed" part) is a check phase that walks the flow's type state
through every step. The same pattern `protomolt-projection` proved: options in
descriptors, evaluation at runtime, zero codegen.

## The schema (pipeline.proto)

```proto
syntax = "proto3";

package ai.pipestream.proto.pipeline.v1;

option java_multiple_files = true;
option java_package = "ai.pipestream.proto.pipeline";
option java_outer_classname = "PipelineProto";

import "google/protobuf/struct.proto";

// A Pipeline is a type-checked composition of gRPC calls, CEL steps, and
// projections over a flow of protobuf messages.
message Pipeline {
  string name = 1;
  string description = 2;
  // Fully-qualified message type the flow starts from.
  string input_type = 3;
  // Constants available to every CEL step as `vars.<name>`.
  map<string, google.protobuf.Value> vars = 4;
  repeated Step steps = 5;
}

// Where a gRPC service lives; its schema comes from server reflection.
message Endpoint {
  string host = 1;
  int32 port = 2;
  bool plaintext = 3;
}

message Step {
  // Optional label; names errors and appears in run results.
  string name = 1;
  oneof kind {
    GrpcCall grpc_call = 2;
    CelFilter cel_filter = 3;
    CelSelect cel_select = 4;
    Project project = 5;
    Unnest unnest = 6;
    Collect collect = 7;
    Let let = 8;
  }
}

// Calls a method whose streaming shape comes from its own descriptor: unary
// over a stream flow fans out per element, client-streaming consumes the flow
// as its request stream, and server-streaming/bidi responses become the new
// flow.
message GrpcCall {
  Endpoint endpoint = 1;
  string service = 2;  // fully-qualified service name
  string method = 3;
  oneof request {
    google.protobuf.Value input = 4;  // constant request payload
    string input_cel = 5;             // built from the flow: `input`, `vars`
  }
}

// Keeps the flow messages where `condition` is true. Stream flows only.
message CelFilter {
  string condition = 1;  // bool CEL over `input`
}

// Replaces each flow message with the CEL result. The expression's inferred
// type becomes the new flow type.
message CelSelect {
  string expression = 1;
}

// Maps each flow message through a protomolt-projection target. The flow type
// must be one of the target's declared `(sources)`; the target type becomes
// the new flow type.
message Project {
  string target_type = 1;
}

// Turns a repeated field of the flow message into a stream of its elements.
message Unnest {
  string path = 1;  // must resolve to a repeated field
}

// Collapses a stream into a single list value (e.g. to hand a whole result
// set to a reranker in one call).
message Collect {
}

// Binds a CEL value from the flow message as `vars.<name>` for later steps;
// the flow is unchanged.
message Let {
  string name = 1;
  string cel = 2;
}
```

## The type system (what "strongly typed" buys)

The flow is a **stream of messages of one tracked type**; a single message is
a stream of one. Every step is a stream operator, and the checker threads the
flow's type through the steps, failing before anything runs:

1. The flow starts as a stream of one message of `input_type`, resolved
   against the registry.
2. `grpc_call`: service/method resolved via the endpoint's reflection (or the
   registry), and the call shape comes from the method descriptor's own
   streaming flags crossed with the flow state. `input_cel` compiles against
   the current flow type; its result must be assignable to the method's input
   type (per-element when the flow is a stream and the method is unary). The
   matrix:
   - stream of one + unary method → one call
   - stream + unary method → one call per element, ordered
   - stream + client-streaming method → the flow streams in as the request
     stream; the single response is the new flow
   - server-streaming or bidi method → the response stream is the new flow
3. `cel_filter`: the condition must compile to bool against the flow type.
4. `cel_select`: compiles against the flow type; the inferred result type is
   the new flow type (message types stay precise, anything else becomes
   `dyn`, and later steps that need a concrete type will fail loudly).
5. `project`: the target resolves and carries `(sources)`; the current flow
   type must be one of them (this is exactly `MessageProjection`'s eager
   validation, reused). New flow type = the projection target.
6. `unnest`: the path resolves to a repeated field; the flow becomes a stream
   of its element type.
7. `collect`: stream → a single list value, where an operator genuinely needs
   the whole set (rerank); the set boundary is explicit, not implicit.
8. `let`: the name is unique; the expression compiles against the flow type
   plus all prior `vars`.

Every CEL expression is compiled per flow type with the same
`CelEnvironmentFactory.addMessageVar("input", type)` pattern the projection
module uses — so a typo fails at check time with the step named, not mid-run
against a live service.

## Execution semantics (v1)

In-order, in-memory, fail-fast: a run executes steps sequentially and stops on
the first error, reporting the step name and index. The semantics are
stream-native, but the v1 executor **materializes** each stage (bounded by the
same payload caps grpc-invoke already enforces); a lazy, backpressured
executor is a later implementation concern that requires no schema change. No
retries, no branching, no deadlines — chains keep their gates/deadlines niche,
and anything durable is a chain concern, not a pipeline concern. Runs are
request-scoped and ephemeral by design.

## Surfaces

- **CLI**: `protomolt run pipeline.json --input '<json>'` — one new verb on
  the existing dispatcher.
- **MCP**: `run-pipeline` (and later `check-pipeline`, which type-checks
  without executing) — one `register()` line each; an agent authors the JSON
  and self-validates it before spending a run.
- **Registry**: pipelines store as subjects like any schema; `deploy-chain`
  later is the same artifact with a trigger. Experiment and deployment are
  one artifact at two lifecycle stages.

## The search story as a pipeline

```json
{
  "name": "search-rerank",
  "inputType": "ai.pipestream.search.v1.SearchRequest",
  "steps": [
    {"name": "search", "grpcCall": {
      "endpoint": {"host": "localhost", "port": 9090, "plaintext": true},
      "service": "ai.pipestream.search.KnnService", "method": "Search",
      "inputCel": "input"}},
    {"name": "hits", "unnest": {"path": "hits"}},
    {"name": "docs", "project": {"targetType": "acme.search.v1.SearchDoc"}},
    {"name": "set", "collect": {}},
    {"name": "rerank", "grpcCall": {
      "endpoint": {"host": "localhost", "port": 9091, "plaintext": true},
      "service": "acme.rerank.RerankService", "method": "Rerank",
      "inputCel": "{'query': vars.query, 'docs': input}"}}
  ],
  "vars": {"query": {"stringValue": "admissibility of prior acts"}}
}
```

Every step checks statically: `Search` accepts the request, `hits` is a
repeated field of the response, each hit is a declared source of `SearchDoc`
(via its projection options), the rerank request matches its method's input
type. A mismatch anywhere fails at check time with the step named.

## Honest edges and open questions

- **Relationship to chain**: chains compose gRPC calls with gates and
  deadlines and are already registry-stored. Pipelines add CEL/projection
  steps and stream cardinality but v1 has *less* operational machinery. If the
  two converge, the likely shape is pipeline-as-authoring-surface compiling to
  chain-as-runtime; for now they answer different questions (ephemeral typed
  experiments vs. durable compositions).
- **Endpoints by name**: inline host/port is v1-honest but a named-endpoint
  registry (or MCP resource) would keep secrets and topology out of scripts.
- **Streaming execution**: the flow semantics are stream-native (every step is
  a stream operator; client-streaming calls consume the flow directly), but
  the v1 executor materializes each stage. Backpressure, windowing, and
  unbounded sources are executor concerns for later — the schema does not
  change when they arrive.
- **Error handling**: fail-fast only. Conditional branches, catch/retry, and
  loops are deliberately absent — that is the DSL slope, and it stays
  un-climbed.
- **Map fields in `project`**: inherits the projection module's v1 limit.

## v1 scope

Schema + checker + executor + `run` verb + `run-pipeline` action + tests in
the fixture style (pipeline JSON + stubbed descriptor sources). Everything
else — named endpoints, deploy triggers, streaming, convergence with chain —
is later, and each is an extension of the schema rather than a change to it.
