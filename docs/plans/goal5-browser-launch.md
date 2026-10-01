# Accepted-workflow browser launch

Status: proposed contracts; no handlers or public mount. Goal 5 also requires
browser authoring, worker-crash recovery, Kafka, Compose, and platform qualification.

## Existing operations

Reuse GetAcceptedWorkflow and LaunchAcceptedWorkflow. WorkflowAuthoringLauncher
checks trusted acceptance, policy, evidence and input before promotion and job
submission. The launch UUID is the job UUID; changed intent conflicts.
Keep the Workflows editor's check/save/direct-run flow separate.

## Proposed input contracts

- GetWorkflowLaunchInputContract takes WorkflowAcceptedCandidate and returns
  that selector, pinned input message name, descriptor-set bytes and their
  ArtifactReference. Reuse author-context bounds. The handler checks descriptor
  hash, imports and workflow input type. Use the existing SchemaSource form
  for browser schema inspection.
- PrepareWorkflowLaunchInput takes the selector and UTF-8 protobuf JSON bytes
  (1 byte through 4 MiB). It returns the selector and a complete unredacted
  application/x-protobuf ArtifactReference of at most 4 MiB. Parse with the
  pinned descriptor, reject unknown fields, validate annotations and check
  serialized size before storing deterministic protobuf bytes.

Validate requests and successful responses. Unsupported rules fail closed.
Annotations cover presence, bounds and artifact metadata constraints. Trusted
acceptance, hash/import checks, schema selection, parsing, authority and storage
identity are handler obligations. Contract validity does not prove semantic
correctness or run success. These operations do not execute fixtures, authorize
launch, promote workflows or create jobs. A cancelled preparation may leave an
unreferenced artifact. Retrying the same serialized bytes returns the same
content-addressed reference; existing artifact retention applies.

## Authority and browser flow

Add workflow-launch permission for the browser bridge. Named console principals
need worker-coordinate plus workflow-launch. Default console and workflow-author
credentials cannot launch. Use the HttpOnly session, with no browser operator
credential. Existing catalog launch actions remain operator-only.

Display the server-verified contract and a JSON input editor for accepted authored
workflows. Preparation errors prevent advancement. Persist the complete launch
intent and UUID before sending the existing launch request. Response loss,
reloads and repeat clicks retain that intent. Edited input requires preparation
and a new launch identity. Display job ID and authorization reference; query
existing job operations for execution status.

## Delivery and proof

1. Root defines additive protobuf messages and native-validator fixtures; Sol
   reviews them. Compile imports, lint, check compatibility, and record OpenAPI
   coverage plus runtime-only constraints.
2. Implement input lookup/preparation. Test invalid UTF-8/JSON, unknown fields,
   bounds, unsupported rules, stale acceptance, corrupt metadata, storage replay,
   and rejection before launch effects.
3. Add scoped session routes. Test default console and author denial, configured
   dual-scope success, and preservation of catalog authorization.
4. Add the editor and persisted intent. Test response loss, reload, changed input
   under one UUID, generic accepted tasks and stale selection. Verify browser
   behavior with real mounted operations, in addition to mocked UI tests.
5. Extend the installed-process proof with public input preparation and job replay.
   Production must not depend on samples.

The browser authoring entry also needs a configured task template, worker choice
and reviewed contract/policy binding. This launch slice does not complete that entry.
