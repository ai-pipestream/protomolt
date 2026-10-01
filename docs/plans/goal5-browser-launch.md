# Accepted-workflow browser launch

Status: contracts, input handlers and opt-in protocol mount implemented on this
branch; browser session routes and editor remain pending. Goal 5 also requires
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
unreferenced artifact. Retrying bytes with compatible stored metadata returns
the same reference; existing artifact retention applies. Verify the returned
full reference and resolved bytes after storage. The filesystem repository
deduplicates by hash and can return older media/redaction metadata; incompatible
metadata causes FAILED_PRECONDITION without altering the stored reference.

## Authority and browser flow

Add workflow-launch permission for the browser bridge. Named console principals
need worker-coordinate plus workflow-launch. Default console and workflow-author
credentials cannot launch. Use the HttpOnly session, with no browser operator
credential. Catalog lookup, input preparation and launch actions require
workflow-launch across protocols; operator access remains available. Cookie
routes dispatch through the catalog with the session caller. The browser entry
also requires worker-coordinate. No default credential receives launch scope.

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
   and rejection before launch effects. Preseed empty and nonempty input bytes
   with text/plain or redacted metadata; require rejection of that stored reference.
   Contract responses permit 8 MiB, including up to 4 MiB of descriptors; do not
   apply WorkflowLaunchValidation's 4 MiB whole-message cap to that response.
   Configure transport/proxy limits accordingly. Decode UTF-8 with error reporting
   instead of replacement characters. Configure generated gRPC clients for an
   8 MiB inbound response limit: a
   4 MiB descriptor payload plus its envelope exceeds gRPC's default 4 MiB limit.
   Recheck acceptance at launch after preparation.
   The preparation JSON envelope base64-encodes 4 MiB of input, exceeding 5 MiB.
   Give the scoped cookie route an 8 MiB body limit instead of the task console's
   existing 20 KiB limit. Test near-limit requests and descriptors through HTTP,
   and reject larger bodies with 413 before parsing or storage.
3. Add scoped session routes. Test default console and author denial, configured
   dual-scope success, and preservation of catalog authorization.
4. Add the editor and persisted intent. Test response loss, reload, changed input
   under one UUID, generic accepted tasks and stale selection. Verify browser
   behavior with real mounted operations, in addition to mocked UI tests.
5. Extend the installed-process proof with public input preparation and job replay.
   Production must not depend on samples.

The browser authoring entry also needs a configured task template, worker choice
and reviewed contract/policy binding. This launch slice does not complete that entry.

## Translation boundary

Protobuf bytes use base64 strings in JSON Schema/OpenAPI. Raw byte bounds are
runtime constraints rather than base64 string-length checks. The existing
translation marks untranslated rules with x-protomolt-runtime-rules and CEL
with x-protomolt-cel. Descriptor-size equality and input artifact media/redaction
checks require ProtoMolt runtime validation; a generic schema validator does not
execute these expressions. Hash/import verification, trusted acceptance,
strict UTF-8/JSON parsing and input-schema validation remain handler checks.
No generator changes are included. Check the mounted OpenAPI document when
handlers are added; these descriptors do not establish available operations.
