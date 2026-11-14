# Repository Sink (`RepositorySinkService`)

`ai.protomolt.proto.repo.sink.v1.RepositorySinkService` is the explicit boundary for publishing selected protobuf processing state or an authorized repository-backed revision as an immutable digital asset or rendition in `DocumentArchive` over v2 `DocumentService`.

## Processing Persistence vs. Repository Publication

Workflow and job persistence (`WorkflowRunStore`) retains binary-faithful step checkpoints (`responseBase64`) strictly for operational replay, resume, and inspection. Running or checkpointing a workflow step never creates a repository document, archive entry, or revision on its own.

Durable repository publication happens only when a workflow step or an authenticated caller explicitly invokes `RepositorySinkService.PublishRepositorySink`:

- Deleting or purging workflow-run rows leaves published repository assets, retained `RepositorySchemaAsset` descriptors, and `RenditionProvenance` records intact.
- First publication from processing state (`ProcessingStateSource`) records `source_revision = 0` alongside `processing_run_id`, `processing_step_name`, and `processing_input_id` rather than fabricating a prior repository revision.

## Contract Surface (`repository_sink.proto`)

`RepositorySinkService` exposes five unary RPCs over gRPC and the repository Connect HTTP gateway (`ConsoleProcedures.ALL`):

1. `ListSaveAsCapabilities`: inspects an authorized `SinkSource` against the host's enabled `RendererRegistry` capabilities without mutating state or authorizing writes. Source payloads on repository revisions decode strictly from closed retained `FileDescriptorSet` bytes (`GetSchemaDescriptor`), never from a live schema registry.
2. `PublishRepositorySink`: admits, authorizes caller read and writer grants, freezes `current` source revisions once at admission, binds `operation_id` to the canonical `(source + conversion + destination)` SHA-256 intent digest, executes optional bounded conversion via `RendererRegistry` outside any catalog transaction, and streams output through `RenditionUploads` into v2 `DocumentService.UploadDocument`.
3. `GetRepositorySinkJob`: returns the durable `RepositorySinkJob` (`QUEUED`, `RUNNING`, `SUCCEEDED`, `FAILED`, `CANCELLED`, or `UNRESOLVED`), reconciling `UNRESOLVED` commit replies through `DocumentService.GetOperationOutcome` before returning.
4. `CancelRepositorySinkJob`: signals cooperative cancellation to an in-flight conversion or upload before the repository commit point.
5. `ListRenditionProvenance`: inspects current or historical archive entry versions and returns persisted `RenditionProvenance` records enriched with `current_source_revision` and `source_stale` currency metadata.

## Concurrency, Idempotency, and Authorization

- **Independent destination preconditions**: `SinkDestination` carries `create_only` and `expected_destination_version` independently of `source_revision`, refusing concurrent destination edits with `ABORTED` instead of overwriting newer revisions.
- **Retry binding**: repeating `PublishRepositorySink` with the same `operation_id` and identical canonical intent replays the committed outcome (`replayed = true`) without creating a duplicate version; reusing `operation_id` with altered source, conversion, or destination intent fails with `ALREADY_EXISTS`.
- **Live pre-commit writer verification**: `RepoSinkAuthorization` captures the admitting caller and re-evaluates both `RepoCallerBindings` and live `Catalog` writer grants immediately before staging and committing bytes (`callForCommit`). Revoking writer authority while an asynchronous conversion is running fails the job with `PERMISSION_DENIED` (`bytes_committed = 0`).
- **Explicit representation loss**: direct protobuf saves stamp `authoritative_protobuf = true`; converted renditions (Markdown, HTML, PDF, Parquet) always stamp `authoritative_protobuf = false` with the provider's `loss_description` and are refused if later selected as a protobuf `ArchiveRenditionSource`.
