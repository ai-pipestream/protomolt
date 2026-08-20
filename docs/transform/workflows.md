# Workflows and run evidence

A workflow turns a successful gRPC exploration into a checked, replayable
contract. It records the input type, service dependencies, ordered steps,
typed dataflow, output mapping, and deadline. The protobuf contract is
[`grpc_workflow.proto`](../../surface/grpc/workflow/src/main/proto/ai/pipestream/proto/grpc/workflow/v1/grpc_workflow.proto).

The normal lifecycle is:

1. register or reflect a service;
2. inspect and probe its methods;
3. check a workflow;
4. compile the workflow into a workflow;
5. record a live run;
6. replay the fixtures offline; and
7. promote the workflow to the Git registry.

Descriptors, validation rules, fingerprints, and recorded evidence determine
whether the workflow is valid. Model output and suggested mappings do not bypass
those checks.

## Workflow model

`Workflow` contains:

- a stable name, description, input type, and whole-run deadline;
- service-profile dependencies with endpoint and descriptor fingerprints;
- ordered gRPC or structured-generation steps; and
- an optional final output mapping.

A gRPC step names a fully qualified method and may apply a CEL gate, request
mapping, response validation, and a nested deadline. A structured step names a
target protobuf type, catalog model, and an attempt cap of at most three.

`WorkflowCompiler` converts an existing checked `CompiledWorkflow` without
changing its mapping or CEL semantics.

## Typed edges

An edge controls the data passed between steps:

1. select the workflow input or prior step outputs;
2. map those sources into a declared protobuf type;
3. optionally project to a consumer-visible type;
4. validate the produced value; and
5. deliver it to a gRPC call or structured-generation step.

Mapping, projection, and validation run before the consumer. A rejected edge
does not invoke the service or inference provider. Projection also provides a
privacy boundary: excluded fields do not enter structured-generation
grounding.

Fan-out selects a repeated field from the edge output and invokes one branch
per item. The contract requires item and concurrency limits, stable branch
identities, ordered collection, and either `FAIL_FAST` or `CONTINUE` failure
handling.

## Structured generation

`StructuredGenerator` resolves the target descriptor and checks the model's
structured-output capability before invoking a provider. It renders a strict
JSON Schema response format, parses protobuf JSON, validates the message, and
uses rendered validation feedback for a bounded repair attempt.

Provider failures stop the operation. Parse or validation failures may retry,
up to the request limit and never more than three attempts. Successful output
is returned as a typed protobuf message.

Model catalog entries may contain an opaque `credential_ref`, such as an
`env:` reference. The host resolves it immediately before transport. Workflows,
provider request bodies, evidence, logs, and errors do not contain credential
material.

## Run evidence

`WorkflowRunRecorder` stores the information needed to review and replay a run:

- workflow, service-profile, descriptor, prompt, and schema fingerprints;
- method and protobuf type names;
- sensitivity-masked request and response artifacts;
- step status, elapsed time, validation findings, and item counts;
- edge fingerprints, validation verdicts, and fan-out branch evidence; and
- structured-generation provider, model, token usage, finish reason, and
  per-attempt outcome.

Raw model output and repair feedback are not persisted. Structured response
evidence is the sensitivity-masked typed message.

`FileSystemArtifactRepository` stores large payloads by SHA-256 content
identity. `FileSystemRunEvidenceRepository` stores immutable run records. A
host chooses their workspace locations and returns references instead of
copying artifacts into MCP responses.

## Offline replay

`WorkflowReplay` performs no network or inference calls. It:

- verifies artifact hashes and descriptor dependencies;
- parses and validates recorded protobuf requests and responses;
- recomputes structured prompt and schema fingerprints;
- checks the recorded validation verdict and attempt history;
- verifies edge fingerprints, cardinality, and branch evidence; and
- replays mixed gRPC and structured-generation workflows from fixtures.

Tampered descriptors, payloads, fingerprints, or attempt histories produce a
failed replay finding.

## Promotion

`RegistryWorkflowVersionRepository` stores immutable workflow versions in the Git-backed
registry. Promotion rejects unresolved artifacts, dependencies, or
fingerprints. A stored version can be recovered with the same dependency
identities used during recording.

## MCP and action workflow

The workbench actions are available through the action catalog, typed gRPC,
REST, and MCP:

| Action | Purpose |
| --- | --- |
| `suggest-mappings` | Generate descriptor-compatible field mapping candidates |
| `check-workflow` | Verify methods, types, mappings, CEL, and deadlines |
| `compile-workflow` | Convert a checked workflow into a workflow |
| `record-workflow-run` | Execute and persist redacted fixtures and evidence |
| `replay-workflow` | Verify a recorded run offline |
| `promote-workflow` | Store an immutable workflow version in the registry |
| `export-work-record` | Project run evidence into a canonical signed work record |
| `verify-work-record` | Verify a signed record offline against a trust snapshot |

The MCP initialize response describes this workflow, and MCP resources expose
service profiles, method contracts, workflows, runs, and artifacts without
loading the full workspace into model context.

See [service workspaces](../surface/service-workspace.md) for endpoint profiles
and [pipelines](pipeline.md) for streaming-aware execution.
