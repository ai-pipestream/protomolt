# Workflow manager

A workflow is a checked serial composition of gRPC and structured-generation
steps. It maps the workflow input and prior step outputs into each request, applies
gates and validation, and returns one typed result.

Workflows are bounded sidecar operations. Durable scheduling, checkpoint storage,
and external completion belong to the `jobs` modules. Streaming pipelines use
the [pipeline executor](pipeline.md).

## Definition

`CompiledWorkflow` contains:

- the workflow name, descriptor files, input type, and deadline;
- an ordered list of steps; and
- an optional output mapping.

Canonical proto3 JSON is accepted by the action, REST, MCP, and registry
surfaces.

Each completed step binds its response under the step name. Mapping rules and
CEL expressions can read `input` and any prior binding. A request can therefore
combine the original input with several earlier responses without a separate
join node.

## Step types

A gRPC step names a target, transport mode, unary method, optional CEL gate,
request mapping, response-validation flag, and nested deadline.

A structured-generation step names a target protobuf type, catalog model, and
attempt limit. It may receive grounding through a typed edge. The runner needs
a configured `StructuredGenerator` to execute this step.

An external-completion gRPC step parks a durable job until `complete-step`
supplies its response. Synchronous `run-workflow` rejects external steps because
it has no checkpoint store.

## Typed edges

A typed edge selects `input` or prior step bindings, maps them into a declared
type, optionally projects the result, and validates it before delivery. When an
edge is present, it owns the request mapping.

Fan-out runs one gRPC or structured branch for each item in a repeated field.
It requires explicit item and concurrency limits, stable branch identities,
ordered collection, and a `FAIL_FAST` or `CONTINUE` failure policy. Branches
run on virtual threads while the declared concurrency limit bounds in-flight
work.

## Static checking

`WorkflowVerifier` checks the full definition without contacting a service:

- every method and protobuf type resolves;
- gRPC workflow methods are unary;
- mapping paths and CEL expressions type-check against the scope;
- projections and validation rules match the delivered type;
- structured targets and fan-out collect shapes are valid; and
- deadlines, step names, gates, and completion modes satisfy the contract.

`check-workflow` exposes the verifier through the action catalog. Registry writes
use the same verification gate.

## Execution

`WorkflowRunner` executes steps in order and fails at the first unrecoverable
error. The whole-workflow deadline bounds every step; a step deadline is limited
by the time remaining in the workflow.

`runSegment` supports durable jobs by stopping at an external step and
returning a checkpoint. The job service resumes from that checkpoint after an
external result arrives.

Inline definitions execute directly. Named definitions resolve through
`WorkflowRepository`; the Git registry provides the standard repository
implementation.

## Recording and promotion

`WorkflowCompiler` converts a checked workflow to a portable `Workflow`.
The workflow workbench records redacted fixtures and run evidence, replays them
offline, and promotes immutable versions to the registry.

See [workflows and run evidence](workflows.md).

## Stream joins

`StreamJoiner` combines two live server streams by arrival order (`ZIP`) or a
matching key (`KEYED`). Both sides use flow control and bounded buffers. The
oldest unmatched entry is evicted when a buffer reaches its limit.

See [joins and derived shapes](join-shapes.md).
