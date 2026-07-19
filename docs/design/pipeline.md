# Pipeline: a dead-simple chain call, typed before it runs

Status: design proposal. No code yet.

A pipeline is a linear chain of steps that hands protobuf messages from one
step to the next. It runs in one process, typically against services on the
same machine, and saves nothing between steps. It is authored as JSON against
a pipeline schema, so there is no parser and no syntax to get wrong. The
schema validates structure. A check phase validates every step against the
descriptors it names, before anything runs.

This is not orchestration. No persistence, no scheduler, no cluster, no
provenance store. A NiFi-class system with flow management and a provenance
UI would be its own server built on top of this schema and executor. This is
not that, and it should not grow into that.

## The schema (pipeline.proto)

```proto
syntax = "proto3";

package ai.pipestream.proto.pipeline.v1;

option java_multiple_files = true;
option java_package = "ai.pipestream.proto.pipeline";
option java_outer_classname = "PipelineProto";

import "google/protobuf/struct.proto";

// A linear chain of steps over a stream of protobuf messages.
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
  // Optional label; names errors and appears in the run record.
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

// Call shape comes from the method descriptor crossed with the flow state:
// unary over a stream fans out per element, client-streaming consumes the
// flow as its request stream, server-streaming and bidi responses become the
// new flow.
message GrpcCall {
  Endpoint endpoint = 1;
  string service = 2;  // fully-qualified service name
  string method = 3;
  oneof request {
    google.protobuf.Value input = 4;  // constant request payload
    string input_cel = 5;             // built from the flow: `input`, `vars`
  }
}

// Keeps the flow messages where `condition` is true.
message CelFilter {
  string condition = 1;  // bool CEL over `input`
}

// Replaces each flow message with the CEL result.
message CelSelect {
  string expression = 1;
}

// Maps each flow message through a protomolt-projection target. The flow type
// must be one of the target's declared `(sources)`.
message Project {
  string target_type = 1;
}

// Turns a repeated field of the flow message into a stream of its elements.
message Unnest {
  string path = 1;
}

// Collapses a stream into a single list value, for steps that need the whole
// set at once (rerank).
message Collect {
}

// Binds a CEL value as `vars.<name>` for later steps; the flow is unchanged.
message Let {
  string name = 1;
  string cel = 2;
}
```

## Type checking

The flow is a stream of messages of one tracked type. A single message is a
stream of one. The checker walks the type through the steps and fails before
anything runs:

1. The flow starts as a stream of one message of `input_type`.
2. `grpc_call`: the service and method resolve via the endpoint's reflection.
   `input_cel` compiles against the current flow type, and its result must be
   assignable to the method's input type. The response becomes the new flow.
3. `cel_filter`: the condition compiles to bool against the flow type.
4. `cel_select`: the inferred result type becomes the new flow type. Message
   types stay precise; anything else becomes `dyn`, and later steps that need
   a concrete type fail loudly.
5. `project`: the target resolves, carries `(sources)`, and the flow type is
   one of them. This reuses `MessageProjection`'s eager validation.
6. `unnest`: the path resolves to a repeated field; the flow becomes its
   element type.
7. `collect`: stream becomes a single list value.
8. `let`: the name is unique, and the expression compiles against the flow
   type plus all prior `vars`.

Every CEL expression is compiled per flow type with
`CelEnvironmentFactory.addMessageVar("input", type)`, the same pattern the
projection module uses. A typo fails at check time with the step named, not
mid-run against a live service.

## Fan-out

V1 is linear. The only fan-out is per-element: a stream flowing into a unary
call runs that call once per element, in order.

Branching fan-out, where one message goes to several downstream steps, is a
DAG. That is the slope toward NiFi, and v1 does not climb it. If a real DAG
is needed, the chain manager already has keyed and zip joins. If v1 use
proves the gap, the extension is a `fan_out` step running named
sub-sequences, added to the schema without changing the linear core.

## Audit trail

Yes, runs need a record. Every run returns a `RunRecord`:

- pipeline name, input type, output type, success or failure
- per step: name, duration, messages in, messages out, error if any

The CLI prints it. MCP returns it. Payloads are not recorded by default; an
optional per-stage digest supports replay and debugging without storing every
message. When pipelines get stored or deployed later, the `RunRecord` is what
persists. That is the whole audit story. A queryable provenance system
belongs to the NiFi-class server, not to a chain call.

## Execution semantics

In-order, in-memory, fail-fast. A run executes steps sequentially and stops
on the first error, reporting the step name and index. The semantics are
stream-native, but the v1 executor materializes each stage, bounded by the
payload caps grpc-invoke already enforces. A lazy, backpressured executor is
a later implementation swap that requires no schema change. No retries, no
branching, no deadlines. Runs are request-scoped and ephemeral.

## Surfaces

- **CLI**: `protomolt run pipeline.json --input '<json>'`. One new verb.
- **MCP**: `run-pipeline` and `check-pipeline` actions. One `register()` line
  each. An agent authors the JSON and self-validates before spending a run.
- **Registry**: pipelines store as subjects like any schema. `deploy-chain`
  later is the same artifact with a trigger.

## Example: search, project, rerank

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
repeated field of the response, each hit is a declared source of `SearchDoc`,
the rerank request matches its method's input type. A mismatch fails at check
time with the step named.

## Not v1

- Named endpoints, so topology stays out of scripts.
- Branching fan-out and joins.
- Queryable provenance. The `RunRecord` is enough.
- Backpressure, windowing, unbounded sources. Executor concerns, later.
- Convergence with the chain manager. Pipelines answer ephemeral typed
  experiments; chains answer durable compositions with gates and deadlines.
