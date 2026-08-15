# Pipelines

A pipeline is the checked, streaming-aware execution form of a gRPC workflow.
Its protobuf contract is
[`pipeline.proto`](../../transform/pipeline/src/main/proto/ai/pipestream/proto/pipeline/v1/pipeline.proto).

Each step receives data through an explicit typed edge. The edge selects
workflow input or prior step outputs, applies mapping and CEL rules, optionally
projects the result, and validates it before delivery. The same edge contract
is used by workflows and pipelines.

## Step types

A pipeline supports:

- gRPC calls with unary, server-streaming, client-streaming, or bidirectional methods;
- structured generation into a validated protobuf message;
- `unnest`, which turns a repeated field into a stream; and
- `collect`, which writes a stream into a repeated field.

Steps run in declaration order. Their outputs become named bindings available
to later edges. A final output mapping may combine bindings into the declared
result type.

## Cardinality

Every binding is `ONE` or `MANY`.

| Method shape | Request | Response |
| --- | --- | --- |
| Unary | `ONE` | `ONE` |
| Server-streaming | `ONE` | `MANY` |
| Client-streaming | `MANY` | `ONE` |
| Bidirectional | `MANY` | `MANY` |

Cardinality changes only through explicit `unnest` and `collect` steps. A
`MANY` binding is linear: one streaming call or one collect step consumes it.
Collect a stream before reusing it.

`max_stream_messages` limits every materialized stream. The executor rejects
an unnest or live response that exceeds the limit.

## Checking

`PipelineChecker` validates a pipeline against its descriptor set before any
service call. It checks:

- the descriptor fingerprint and every service dependency;
- method names, request and response types, and streaming flags;
- edge sources, mappings, CEL expressions, projections, and validation types;
- declared input and output cardinality;
- fan-out item and collect shapes; and
- final output mappings.

`PipelineValidation` enforces the contract's structural rules. A caller should
run structural validation and static checking before execution.

## Execution

`PipelineExecutor` calls services through the host-owned `PipelineTransport`.
The transport supplies endpoint and credential policy; the pipeline carries
only logical service dependencies.

Fan-out is available for unary gRPC calls and structured generation. It has
explicit item and concurrency limits, stable branch order, and `FAIL_FAST` or
`CONTINUE` failure behavior. Branch work runs on virtual threads while a
semaphore enforces the declared concurrency cap.

Pipeline and step deadlines bound the run. External-completion steps require a
durable job coordinator and are rejected by the in-process executor.

## Workflows

`WorkflowPipelineCompiler` converts a `Workflow` into a pipeline, preserves its
typed edges and fan-out policy, derives method shapes from descriptors, and
records the workflow fingerprint. `PipelineChecker` then verifies the compiled
contract independently.

See [workflows and run evidence](workflows.md) for compilation, recording, replay,
and promotion.
