# Pipelines

A pipeline is the checked, streaming-aware execution form of a gRPC workflow.
Its protobuf contract is
[`pipeline.proto`](../../transform/pipeline/src/main/proto/ai/protomolt/proto/pipeline/v1/pipeline.proto).

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

Pipeline and step deadlines bound the run. Synchronous `PipelineExecutor.run`
evaluates each step's `when` gate first and rejects unskipped
`STEP_COMPLETION_EXTERNAL` steps with `FailureKind.EXTERNAL`.

## Segmented execution and durable job coordination

`PipelineExecutor.runSegment` executes a pipeline up to completion or the next
unskipped `STEP_COMPLETION_EXTERNAL` step, returning a sealed `Segment`
(`Segment.Completed` or `Segment.Parked`). Prior `Checkpoint` entries replay
completed steps in order without re-invoking upstream services after verifying:

- canonical SHA-256 fingerprints of the `Pipeline` + descriptor set and the
  binary input message;
- ordered step identity, output `Descriptor` full name and structural
  fingerprint, cardinality (`ONE` or `MANY`), and `max_stream_messages` bounds;
- `StepOutcome` request/response counts, fan-out `BranchOutcome` evidence,
  declared `validate_response` constraints, and linear stream consumption.

When an `ExecutionIdentity` (`jobId` + `attempt`) is supplied,
`DynamicPipelineTransport` stamps `x-protomolt-job-id`,
`x-protomolt-step-name`, `x-protomolt-branch-id`, and
`x-protomolt-operation-id` (`<jobId>/<stepName>` or `<jobId>/<stepName>#<index>`)
onto outbound gRPC calls so side-effecting services deduplicate retries across
worker reassignments.

`protomolt-jobs-service` (`PipelineJobCoordinator`,
`WorkflowRunSubmitter.submitPipeline`, `WorkflowRunWorker`, and `complete-step`)
persists pipeline definitions, transitive descriptor sets, inputs, and step
checkpoints in `workflow_run` with canonical protobuf wire bytes preserved in
base64 alongside JSON projections for `get-job` and `list-jobs`. External
completions validate against the parked step's descriptor and
`validate_response` rules, accept idempotent redeliveries, and refuse altered
intent or conflicting payloads without mutating committed outcomes.

## Workflows

`WorkflowPipelineCompiler` converts a `Workflow` into a pipeline, preserves its
typed edges and fan-out policy, derives method shapes from descriptors, and
records the workflow fingerprint. `PipelineChecker` then verifies the compiled
contract independently.

See [workflows and run evidence](workflows.md) for compilation, recording, replay,
and promotion.
