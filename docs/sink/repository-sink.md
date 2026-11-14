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

## Parquet Rendition Provider (`protomolt-emit-parquet`)

`ai.protomolt.proto.emit.parquet.ParquetRendererProvider` qualifies `ParquetEmitter` behind `RendererProvider` so authorized protobuf processing outputs or repository-backed revisions can be saved as non-authoritative `.parquet` renditions (`application/vnd.apache.parquet`) without loading Hadoop runtime classes.

### Provider Profiles

The provider never guesses or flattens arbitrary nested repeated fields; every conversion uses an explicit profile:

1. **Single-document profile (`ai.protomolt.emit.parquet.single`, `source_profile = "protobuf.single_message"`)**: discovered via `ServiceLoader` and mounted by default in `RepoServices.defaultSaveAsRenderers()`. Renders one authorized protobuf message (an archive protobuf rendition, document main payload, named part, or unpacked `google.protobuf.Any` occurrence) into a 1-row Parquet artifact.
2. **Declared projection profiles (`ParquetRendererProvider.projected(...)` and `ParquetRendererProvider.projectedRepeated(...)`)**: bind a custom `provider_id` and `source_profile` to an explicit `sourceDescriptor` and compiled `MessageProjection`. Single-row projection maps one message to one target row; repeated-row projection extracts elements strictly from an explicitly declared repeated message `FieldDescriptor` on `sourceDescriptor` and projects every element into the target row schema, failing closed if any element fails projection or validation.

### Typed Options (`ParquetRendererOptions`)

`ai.protomolt.proto.emit.parquet.v1.ParquetRendererOptions` (`parquet_renderer.proto`) is advertised via `RendererCapability.options_type` with closed `FileDescriptorSet` bytes returned by `ListSaveAsCapabilities`:

- `columns`: optional top-level column names to project into the Parquet schema (empty retains all fields on the row descriptor; blank, dotted, duplicate, or unknown column names are rejected).
- `masked_sensitivity_classes` and `mask_strategy` (`PARQUET_MASK_STRATEGY_REDACT` default, or `PARQUET_MASK_STRATEGY_REMOVE`): applies `SensitivityMasker` to fields annotated with `[(ai.protomolt.proto.meta.v1.field).sensitivity = "..."]` after message validation and before Parquet encoding. Keyed encryption strategies (`ENCRYPT`/`DECRYPT`) and validation bypass (`skipValidation`) are deliberately excluded from `ParquetRendererOptions`.
- `max_output_bytes`: optional tighter ceiling (`1..provider.max_output_bytes`; `0` uses the provider's bounded ceiling, 32 MiB by default and at most 64 MiB).
- `mapping_id` and `policy_id`: optional governance identifiers echoed alongside `ConversionSpec.mapping_id` and `ConversionSpec.policy_id`.

### Fail-Closed Refusals, Bounds, and Boundary Separation

- **Refusal policy**: empty root or nested message schemas, cyclic/recursive message schemas (outside `google.protobuf.Struct`, `Value`, and `ListValue`), opaque `google.protobuf.Any` columns (unless unpacked via `occurrence_field_path`, projected out via `columns`, or mapped away via `MessageProjection`), unknown wire fields, unrecognized enum numbers, and `ProtoValidator` rule violations are deterministically refused before repository commit.
- **Bounded materialization and cooperative cancellation**: `ParquetEmitter` streams output through a bounded `OutputFile` that checks `Cancellation` before each row and on every `PositionOutputStream` write, aborting with `LIMIT` (`RESOURCE_EXHAUSTED`) before writing past `max_output_bytes` or `CANCELLED` without committing partial renditions.
- **Separate from object-store and table sinks**: `Save as...` Parquet writes a single immutable repository rendition artifact with `RenditionProvenance` (`authoritative_protobuf = false`). Direct object-store Parquet emission (`S3ParquetSink` in `protomolt-emit-s3`) and governed Apache Iceberg table sinks remain independent delivery paths.
