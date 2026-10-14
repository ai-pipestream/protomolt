# Repo service: contract and component mapping (draft for review)

For Argon review only. These two draft files are committed on
`reconcile/parts-based-repository`; no production contract or implementation is changed.
Requirements v2.5 remain normative; this is their proposed contract mapping.

- **Implements:** requirements v2.5 (`docs/design/parts-based-repository-requirements.md`, branch `reconcile/parts-based-repository`).
- **Contract:** [document_service_v2_draft.proto](document_service_v2_draft.proto). It compiles with protoc against the v1 protos.

> **Source caveat.** The original class inventory came from GitHub main and Javadoc
> summaries, not full call-path review. The 2026-10-14 correction checks specific
> components on reconciliation base `c46c0f582`; it does not qualify every row.
> All unverified keep/retire rows below are candidates, never a deletion manifest.
> Preserve required behavior and port its tests before any removal.

## 1. Decisions in the draft that need your call

| # | Decision | Draft default | Why |
|---|----------|---------------|-----|
| D1 | Final protobuf package/shared messages | **Open.** `repo.v2` isolates this review draft only. | One public DocumentService remains required. No authorization to deploy v1/v2 side by side or delete imported shared v1 messages. |
| D2 | Expected revisions and content dedupe | Every save names an expected revision or "none". | v1 `force_save` bypasses content deduplication, not concurrency. Removing it is a separate dedupe-policy decision; do not claim it was an unconditional-write switch. |
| D3 | Is a tombstone a revision? | **Yes.** It gets a revision, an operation ID and an expected revision. | History shows the delete. A later save over it is an ordinary CAS. |
| D4 | Purge events | **Two events:** `PurgeMarked` when references are marked, `PurgeCompleted` when bytes are deleted. | This matches v1's requested/purged pair and lets compliance see when bytes are actually gone. |
| D5 | Where segmentations live in `Document` | **New fields** `SearchMetadata.segmentations = 16` and `embedding_sets = 17`. Field 13 is reserved after the consumers move. | Additive, but it waits on the §16.2 consumer inventory. |
| D6 | Offset units allowed | UTF-8 byte, UTF-16 code unit, code point | These cover Java, Python and Rust. Unspecified is rejected. |
| D7 | Catalog persistence library | **Decided:** JDBC + HikariCP + Flyway. | User selected this stack; preserve transaction semantics when implementation is authorized. |
| D8 | v1 conditional-blob RPCs (`PutBlob`, `GetBlobForUpdate`, `CompareAndPutBlob`, `DeleteBlob`) | **Removed** | They contradict write-once stacks. If a caller depends on them, that has to be found before deletion. |
| D10 | Persisted request metadata in retry identity | **Decided:** persisted metadata is included and identical on retries. | Transport tracing stays outside durable intent. |
| D9 | Upload shape | One raw blob or encoded part per UploadDocument stream; the header names the same save operation. | Both use the canonical handler. This closes the structured-part transport gap without another storage API. General multi-payload streaming remains a review decision. |

## 2. What the contract does, in short

- **Writes**
  - `SaveDocument` and `SaveDocuments` (bidi; one response per request_id, independent of the durable operation_id).
  - `UploadDocument` (client stream for raw bytes or an encoded codec fragment; the HTTP route maps onto the same upload/save operation).
  - `DeleteDocument` (tombstone) and `PurgeDocument` (admin).
- **Outcome lookup:** `GetOperationOutcome` returns either `committed` or `none`. The third state, "lookup unavailable", is gRPC `UNAVAILABLE`.
- **Reads**
  - `GetDocument` (unary), `ReadDocument` (manifest, part bytes, completion), and `ReadDocuments` (correlated bulk streams).
  - `ReadBlob` (byte range, plus a SHA-256 check at the end of a full read).
  - `GetManifest` (no payloads), `ListRevisions` (newest first, paged) and `ListDocuments`.
- **Part selection.** A `PartSelector` is one of a fixed part (CORE/BLOBS/PARSED), a segmentation ID, or an embedding-set key. Duplicate or contradictory selectors are rejected.
- **Save semantics**
  - `writes` lists what changes: PUT, EMPTY or DELETE.
  - Content in `document` for any part not listed as PUT is rejected, never dropped.
  - `sources` takes parts from exact revisions of other documents.
  - Everything else carries forward from the expected revision.
- **Intent.** SHA-256 over versioned binary SaveIntent, including write actions and reasons, content identity, sources, metadata, provenance, drive selection, validation mode and owner/opaque attestations. The requirements §7 encoding rules are required; a list of fields alone is not a canonical encoding.
- **Public manifest.** It lists every part with its state, size, SHA-256, the revision that wrote it, `segmentation_digest`, `opaque` and `content_included`. Stack references are internal (`StoredManifest` and `StackRef`).

### Error table

| gRPC status | Detail message | When | Recorded as an outcome? |
|-------------|----------------|------|-------------------------|
| `ABORTED` | `RevisionConflict` (current revision, tombstone flag) | Stale expected revision, after the same-ID outcome re-read | No |
| `ALREADY_EXISTS` | `OperationReuse` | Same operation ID, different intent | No |
| `INVALID_ARGUMENT` | `SaveRejection` (`MALFORMED_REQUEST`, `VALIDATION_FAILED`, `INVALID_BYTES`, `UPLOAD_MISMATCH`) | Request structure or the composed candidate fails | Yes, for validation failures |
| `FAILED_PRECONDITION` | `SaveRejection` (`SCHEMA_MISSING`, `SOURCE_PURGED`, `SOURCE_NOT_FOUND`, `SEGMENTATION_MISMATCH`, `OPAQUE_UNOWNED`, `OWNER_UNKNOWN`) | The world isn't as the request assumes | Yes |
| `PERMISSION_DENIED` | `SaveRejection` (`OPAQUE_NOT_ALLOWED`) | Opaque mode requested, but host policy refuses it | Yes |
| `UNAVAILABLE` | none | Registry or catalog unreachable | Never |
| `NOT_FOUND` | none | Exact revision, blob or document doesn't exist | n/a |
| `DATA_LOSS` | none | Stored bytes fail their SHA-256 check | n/a |

In `SaveDocuments`, the same code and detail arrive per item as `ItemError`.

**Question.** Recording validation rejections as outcomes means a retry with fixed content must use a **new operation ID**. That follows from §7 ("a recorded failure such as a validation rejection"), but please confirm it's what you want.

## 3. Target components

```
repo/proto       canonical DocumentService (final package is open)
repo/codec       parts codec: CORE/BLOBS/PARSED + CHUNKS per segmentation + EMBEDDINGS per set
repo/stack       Stack interface (was blob/spi) + s3, redis, cache providers
repo/catalog     proposed catalog boundary; PostgreSQL first, JDBC + HikariCP + Flyway
repo/validation  composed-candidate validation, opaque handling, retained schemas (was admission)
repo/engine      DocumentSaves, DocumentReads, Purges: orchestrates codec + stack + catalog + validation
repo/service     DocumentService gRPC, HTTP upload route, DriveService, wiring/main
```

The module names above describe responsibilities, not an approved directory shuffle.
Prefer existing modules where the dependency boundary is already appropriate.

The **Stack** interface (§6):
- `put(InputStream) -> StackRef` (key, SHA-256, length);
- `get(stackId, key, range) -> owned read handle` with retained identity/length,
  close/cancel behavior and explicit full-object versus range verification;
- `head(key)`;
- `delete(key)`;
- `copy(src, dst)`, optional.

It has no version IDs, ETags or conditional writes.

The **Catalog** interface (§7):
- `commit(CommitRequest) -> Outcome`. One transaction covering:
  1. outcome lookup;
  2. CAS;
  3. accession insert;
  4. outcome insert;
  5. outbox insert;
  6. pointer move.
- `lookupOutcome(account, operationId)`.
- `manifest(address, revision)`.
- `revisions(address, page)`.
- `documents(scope, page)`.
- `markPurged(address, purgeId)`.
- `deletableObjects(purgeId)`.
- `isPurged(StackRef)`, also checked inside `commit` for carried-forward references.

**Catalog tables** (fresh baseline):
- `stacks`;
- `documents` (call number and current-revision pointer);
- `revisions`;
- `part_refs`;
- `objects` (stack ref plus purged flag);
- `operation_outcomes` (unique on account and operation ID);
- `outbox`;
- `schema_artifacts`.

The baseline requires a new empty catalog and refuses an existing V1–V120 database.
Absence of a Flyway history table alone is not proof that a database is empty.

## 4. Mapping of existing classes

Legend: **keep** = candidate reuse, **adapt** = known change needed, **replace** = proposed replacement of a role, **retire** = candidate deletion after acceptance and consumer review, **frozen** = untouched pending a decision. None of these labels alone authorizes deletion or proves fidelity.

### Codec (`repo/codec`, 9 classes): keep the core

| Class | Verdict | Change |
|-------|---------|--------|
| `DocumentPartCodec`, `PartLayout`, `PartLayouts` | adapt | Add EMBEDDINGS. CHUNKS is keyed by segmentation ID, EMBEDDINGS by `segmentation_id/embedding_set_id`. Fix the "byte-for-byte" claim to say semantic equality plus preserved unknown fields (§15.7). |
| `DocumentRevisionAssembly` | verify/adapt | Audit materialization before using it for large reads. |
| `PartObject` | adapt | Confirmed byte[] payload in this branch. Keep only for explicitly bounded materialization; streaming needs a source/reference abstraction without whole-part allocation. |
| `DocumentChunkSequence`, `ChunkRunNames` | adapt | Validate segmentations and embedding sets (digest binding, one vector per segment ID). |
| `DocumentFragmentConfinement` | keep (verify) | |
| `RepositoryNamespaces` | adapt | Becomes write-once stack key naming. |

### Stacks (`repo/blob/*`, 30 classes): reduce

| Class | Verdict | Change |
|-------|---------|--------|
| `BlobStore` | replace with `Stack` | Drop `WriteCondition`, ETags, `versionId`, `getForUpdate`, `conditionalPut` and byte[] `get`. Add a streamed get with range. |
| `BlobStoreProvider`, `BlobStores` | adapt | Provider discovery stays. |
| `S3BlobStore`, `S3BlobStoreProvider`, `S3ObjectReads` | adapt | Streamed ranged get, no versioning. |
| `RedisBlobStore` + its 7 classes | adapt (or retire) | Keep only if Redis is a real stack choice. Its value cap must not become a platform limit. **Question for you.** |
| `CachingBlobStore` | adapt | Confirmed complete GetResult/byte[] read interface. Check cache identity includes stack/namespace, digest verification, purge visibility, bounded cache admission and streaming bypass. Immutable keys alone do not qualify it. |
| `RemoteBlobStore` (blob/grpc) | verify, then keep or retire | The "dogfood" remote store. Keep only if something deploys it. |
| `BackendIdentity`, `S3BackendIdentity`, `RedisBackendIdentity` | replace | Become the `StackRecord` catalog row. |
| `PhysicalObjectIdentity`, `PhysicalObjectLocation` | replace | Become `StackRef`. |
| `PayloadBudget` | reuse with measurement | Tracks reservations, not JVM heap or SDK buffers. Bound active buffers and measure slow-consumer/cancellation paths; it does not turn byte[] APIs into streams. |
| `ExactVersionReclaimer`, `S3ExactVersionReclaimer`, `ExactVersionOutcome`, `ObjectReclaimer`, `S3ObjectReclaimer`, `ExpiringBlobStore` | retire | Provider-version tracking and automatic reclamation are out (§6). |
| `NamespaceProvisioner`, `S3NamespaceProvisioner` | keep | Bucket setup. |
| `OpenedBlobStore`, `BlobCapability`, `BlobStoreException` | adapt (verify) | |

### Catalog (`repo/container`, 233 classes): mostly retire

**Keep or adapt**

| Class | Verdict | Change |
|-------|---------|--------|
| `JdbcEventOutbox`, `EventRelay` | adapt | Preserve caller-transaction enqueue and binary events. Use the selected JDBC transaction boundary. |
| `DocumentEventFactory`, `DocumentEventRecord` | adapt | One event per accession, listing changed parts. |
| `DocumentIds` | keep | Deterministic call-number UUIDs. |
| `LedgerDatabase`, `LedgerConfig`, `SqlTimeouts` | adapt | Pool, then the fresh baseline with its refuse guard. |
| `Tx` | verify/adapt | Preserve rollback/isolation behavior; adapt to the selected JDBC transaction boundary. |
| `DocumentLedger` (578 lines) | replace | Becomes `PostgresCatalog`, with the commit transaction above. |
| `RepositorySchemaArtifacts` | adapt | Schema artifact IDs stored on the revision. |
| `DriveLedger`, `DriveRecord`, `DriveKeys` | keep | A drive resolves to the current stack ID. |
| `S3Purger` | adapt | The byte-deletion step of purge. |
| `JdbcPurgeQueue`, `KafkaPurgeQueue`, `PurgeQueue`, `PurgeRelayStore`, `JdbcPurgeRelayStore`, `PurgeOffsetTracker`, `PurgeSweeper`, `PurgeSnapshots`, `DocumentPurgeRecord`, `DocumentPurgeCommandFactory` | replace | Purge becomes one mark transaction, then an idempotent deletion pass keyed by purge ID. |
| `StorageReconciler` | adapt to report-only | It lists unreferenced objects and **deletes nothing** in slice 1. |

**Retire (§14)**

| Group | Classes |
|-------|---------|
| Coordinator, handoff and drains | all `RepositoryCoordinator*`, `RepositoryClaimRetirement`, `RepositoryExecutionClaimLedger`, `RepositoryShutdownClaim`, `RepositoryOperationLedger` |
| Successor generations | `RepositorySuccessor*`, `DocumentSuccessorFingerprint`, `RepositoryHistoricalSuccessorActivation` |
| Reader pins and quiescence | `DocumentReadPins`, `DocumentReadLedger`, `DocumentReadLifecycle`, `DocumentReadProtection`, `DocumentReadRecovery`, `ReaderHost*`, `ReaderRegistration`, `ReaderExternalQuiescence` |
| Capture coverage | `DocumentPreparation*` (12 classes) |
| Historical attempt owners | `RepositoryHistorical*`, `RepositoryInitialHistorical*`, `RepositoryInstalledHistoricalAttempts`, `DocumentHistorical*` (about 35 classes), `HistoricalDocumentRepository`, `HistoricalMaterializationRepository` |
| Attempts and staging ledgers | `DocumentAttempt*`, `DocumentPartAttemptLedger`, `DocumentSelectedAttemptLedger`, `DocumentCommitParts`, `DocumentCommitWriter`, `DocumentPartStager`, `DocumentPartPublication`, `DocumentPartTransfer`, `DocumentPartWorkers`, `DocumentKeyReservations`, `DocumentRevisionLocks`, `DocumentRevisionRetentionInventory`. The new commit replaces these; check `DocumentPartWorkers` for reusable parallel part IO. |
| Publication | all `DocumentPublication*`, `RemoteDocumentPublicationRepository`, `RepositoryPublicationCalls`, `Managed*Publication*` |
| Assessments | all `DocumentAssessment*`, `DocumentRejectedAssessmentReads`, `DocumentSchemaAssessment*` |
| Managed recovery and credentials machinery | `RepositoryManagedRecovery`, `RepositoryRecoveryAttempts`, `RepositoryReservedPreparation`, `ManagedBackendLedger`, `CoherenceProbe` (verify each) |
| Physical object ledger | `PhysicalObjectLedger`, `RawObjectLedger` (verify against the HTTP upload path) |

**Frozen (§16.1)**

| Group | Classes |
|-------|---------|
| Archive subsystem | all `Archive*` classes (about 35 across container, engine and service), `RepoBoundedArchiveMain`, `Bounded*` |

### Validation (`repo/admission`, 36 classes)

| Class | Verdict | Change |
|-------|---------|--------|
| `DocumentSchemaAdmission` | keep, adapt | Validates the composed candidate. Reads sibling parts only when a rule needs them, and counts those reads for §15.1. |
| `DocumentAnyMaterialization`, `DocumentAnyRootInventory` | verify/adapt | Reuse traversal. Existing resolved materialization is not proof of terminal opaque handling or new semantic-root coverage. |
| `DocumentAdmissionPolicy` | adapt | Host policy for opaque mode. |
| `DocumentSchemaArtifactCache`, `DocumentRetainedSchemaResolution`, `DocumentRetainedSchemaAssets`, `DocumentSchemaDefinitions`, `BuiltinDocumentSchema`, `DocumentCompositeSchemaResolution` | keep | Cache by artifact ID; retained schemas for historical reads. |
| `DocumentSchemaOccurrences`, `DocumentSchemaOccurrenceCodec`, `DocumentSchemaOccurrenceProjection` | adapt | Produce `OpaqueOccurrence` and `RuleResult` on the revision. |
| `DocumentRetained*Materialization`, `DocumentHistoricalResponseMaterialization`, `DocumentHistoricalResponseVerifier` | adapt (verify) | Exact-revision reads decode with retained schemas. Keep whatever does that. |
| `DocumentAdmissionReservations`, `DocumentAdmissionResources`, `DocumentPayloadAssessment`, `DocumentPayloadCheck`, `*Replay`, `*Evidence*`, `DocumentAssessmentManifestCodec`, `DocumentSchemaAssetBinding`, `DocumentSchemaBinding`, `DocumentSchemaMaterialization`, `DocumentSchemaPreparation`, `DocumentSchemaRoot*` | retire or fold | Assessment and evidence lifecycles. Fold anything the validation step needs into `DocumentSchemaAdmission`. |
| `RegistrySchemaResolver`, `RegistryDescriptorLoads` (schema/registry) | keep | |

### Engine (`repo/engine`, 32 classes)

| Class | Verdict | Change |
|-------|---------|--------|
| `DocumentOperations` (1,085 lines) | replace | Becomes `DocumentSaves` and `DocumentReads`, following §7 and §8 directly. |
| `SaveResolution` | keep | Call-number resolution from `DocumentTarget`. |
| `DocumentSaveCandidate` | adapt | The composed candidate: writes, sources and carried-forward parts. |
| `ManagedDocumentSave` | replace (verify) | Its role folds into `DocumentSaves`. |
| `DocumentPartReader`, `DocumentReadBatch`, `DocumentRequests` | adapt | Read by `StackRef`; one revision resolved per call. |
| `RepositoryErrors` | adapt | Implements the error table above. |
| `DriveOperations`, `DriveProvisioner` | keep | |
| `BlobOperations` | retire | The conditional-blob RPCs are gone. |
| `RawIngestionOperations`, `ManagedRawBindings`, `RawObjectRecovery` | adapt | Become the upload operation behind `UploadDocument` and the HTTP route. |
| `ControlledBlobStore`, `UnservedBackendGenerationException` | retire (verify) | |
| `DocumentHistorical*`, `HistoricalReadFailures` | retire | History is exact-revision reads. |
| `Archive*` | frozen | |

### Service (`repo/service`, 45 classes)

| Class | Verdict | Change |
|-------|---------|--------|
| `DocumentGrpcService` | replace | Implements v2 `DocumentService`. |
| `UploadHttpServer` | adapt | It calls the same upload operation as `UploadDocument`: same claim check, validation and accession. |
| `DriveGrpcService` | keep | |
| `GrpcErrors` | adapt | Maps errors to status plus detail message per the table. |
| `DocumentHistoryGrpcService`, `DocumentHistoryMaterializationGrpcService`, `DocumentPublicationGrpcService`, `HistoricalDocumentResponses`, `HistoricalOccurrenceResponses`, `HistoricalReadAccess`, `ReaderHostOptions`, `RepositoryDrainTimeoutException`, `ManagedPublication*` | retire | |
| `RepoServiceMain`, `RepoServiceModule`, `RepoServices`, `RepoServiceConfig`, `ManagedDocumentServices`, `ManagedStoragePolicy`, `ManagedSchemaAccess`, `ManagedSchemaOwnership`, `SelectedDriveBackend`, `SelectedBlobBacking` | adapt | Constructor wiring for stack plus catalog. |
| `GrpcServerLifetime`, `LifecycleShutdown`, `ExecutorShutdown`, `ShutdownBarrier`, `OwnedResources`, `UnaryRequestAdmission` | keep (verify) | |
| `RemoteRouting`, `RemoteBucketBindings`, `RemoteRepositoryCredential`, `RepositoryNetworkAuthentication`, `RepositoryEnvironment` | verify | Keep only what multi-host routing or auth actually needs. |
| `Archive*`, `Bounded*`, `RepoBoundedArchiveMain` | frozen | |

### SPI (`repo/spi`, 22 classes)

| Verdict | Classes |
|---------|---------|
| keep, adapt | `DocumentRepository` (to the v2 operations), `DriveRepository`, `RepositoryCaller`, `DocumentAccessPolicy`, `RepositoryException` |
| retire | `DocumentPublication*`, `HistoricalDocumentRepository`, `HistoricalMaterializationRepository`, `RepositoryOperationControl`, `RepositoryReadControl` (verify), `BlobRepository` |
| replace | `PhysicalObjectIdentity` and `PhysicalObjectLocation` become `StackRef` |
| frozen | `ArchiveRepository`, `ArchiveMutationRepository`, `RawIngestionRepository` (upload path, verify) |

### Rough scale

- The original 411-class inventory was counted on main, not qualified against this branch. No verified keep/delete counts or implementation estimate are asserted.
- The 120 migrations are replaced by the baseline (§14).
- There are about 570 test classes. Each retiring test is checked against §15 and the failure cases first.

## 5. Next steps after review (not started)

1. Settle D1–D9 and the Redis and remote-store questions.
2. Verify this mapping against the branch's class list.
3. Do the §16.2 consumer inventory before D5 lands.
4. Approve the contract and bounded implementation slices. Write acceptance assertions alongside each slice; do not defer tests until the end. Retirement follows replacement proof and a caller inventory.

## 6. Correction record and required handler behavior (2026-10-14)

This revision addresses the coordinator's seven review findings. Its initial revision changed only
the two draft files; the ownership revision also updates the sole requirements. It does not restore retired design documents, start an
implementation or introduce another normative requirements copy.

### Validation and schemas

- Buf-compatible annotations now express required alternatives/messages/IDs,
  allowed part selectors, revision/length bounds, digest syntax and enum values.
  CEL rules cover offset ordering, vector dimensions, deletion reasons and the
  incompatibility of typed-valid with opaque/unevaluated occurrences. Execute them
  through ProtoMolt's own ProtoValidator before calling the draft runtime-qualified.
- Imported v1 NodeAddress fields do not gain annotations by import. Handler checks
  must verify required address components, graph semantics, authenticated account,
  source/destination ownership and access, and replay authorization. Invalid identity
  is not permission to record a rejection against another account's operation ID.
- Duplicate selectors, valid source revisions, purge eligibility, CAS, actual part
  confinement, source text/offset consistency, finite vector values, unique/exact
  segment coverage and content-versus-write correspondence are mandatory handler
  checks. These are not proven by a successful protobuf parse.
- RevisionValidation now associates each resolved occurrence with an exact retained
  RepositorySchemaAssetReference and its Any payload digest/length. PartSchemaBinding
  also pins the containing fragment schema. Same type URL with different definitions
  in separate occurrences remains distinguishable. Flat artifact-ID membership alone
  was insufficient. Retain full closures/options, never substitute a live definition.
- OccurrenceLocator reuses RepositorySchemaOccurrenceStep (field number, index,
  typed map key and resolved intermediate Any). A terminal opaque envelope is selected
  without entering its bytes. Check every transition against pinned descriptors and
  actual content. Existing root discovery/codec rules need adaptation, not blind reuse.
- Validate response and stored result envelopes as well as requests. A rejected
  candidate never becomes typed-valid. Failed typed admission cannot retry internally
  as opaque. Registry outage and absent schema remain different failures.

### Handler rules

Requirements §§5–11 now own retry identity, ownership, opaque carry-forward,
stream framing, event deduplication and concurrency semantics. This mapping does
not maintain a second normative copy. Canonical intent fixtures and runtime
validation remain unexecuted design acceptance work.

### Source checks and remaining review boundaries

Checked directly on reconciliation base c46c0f582:

- `repo/codec/.../PartObject.java`: byte[] payload, not a streaming abstraction.
- `repo/blob/cache/.../CachingBlobStore.java`: complete-object read results and
  size-bounded cache population; immutable keys alone do not qualify purge or streams.
- `repo/blob/spi/.../PayloadBudget.java`: reservation accounting, not total memory.
- `repo/proto/.../document_service.proto`: force_save is a content-deduplication bypass.
- `repo/proto/.../schema_occurrence.proto` and `schema_asset_reference.proto`:
  existing typed occurrence steps and exact retained artifact identity.
- `repo/proto/.../document.proto`: semantic_results is field 13; fields 16/17 are
  available in this checkout, but consumer migration is still required.

The remainder of the original class matrix has NOT been individually qualified.
In particular, retirement wildcards must never become deletion commands. Removing
a lifecycle does not automatically remove reusable validation, authorization,
recovery or provider behavior in a class with a similar name.

Argon review still needs to settle package/shared-message placement (avoid circular
imports when v1 Document references new semantic types), semantic consumer mapping, exact canonical encoding fixtures, full annotation and
handler coverage, multi-payload streaming scope, and purge concurrency/progress
lookup details. Neither v2 package naming nor this draft authorizes two public
repository services. Retain the original aggregate/parts API concept.

### Verification of this documentation revision

The updated draft compiles with `libprotoc 25.1` and its complete imports on this
branch. Reproduce from the repository root (output stays outside the checkout):

```bash
proto_args=()
while IFS= read -r proto_root; do
  proto_args+=("-I$proto_root")
done < <(rg --files -g '*.proto' | sed -n 's@\(.*src/main/proto\)/.*@\1@p' | sort -u)
protoc "${proto_args[@]}" -Idocs/design/repo-v2-contract-draft \
  --include_imports --descriptor_set_out=/tmp/repo-v2-argon-review.pb \
  docs/design/repo-v2-contract-draft/document_service_v2_draft.proto
```

`git diff --check` also passes. No production module was built or changed. No
runtime validator fixtures, CEL execution, integration tests or performance tests
were run for this documentation revision. These are still required before approval
for implementation: valid/invalid annotation fixtures (especially oneof presence,
false/zero values, event revision kinds and CEL numeric types), same-type-URL
occurrence bindings, equivalent-upload replay, source-text changes under the same
field path, and distinct purge marked/completed deliveries. Compilation is not
proof of those behaviors.

### Review response: annotation and scope corrections

The three field-level required annotations inside oneofs were incorrect and are
removed; the oneofs remain required. Event operation IDs are required by CEL only
for accession/tombstone events. The reviewer exercised these patterns with upstream
Protovalidate; that is not a run of this draft through ProtoMolt’s validator.
Client schema bindings and StreamedPart.schema are removed. Retained server-selected
occurrence and fragment identities remain on RevisionValidation. Bulk errors now
use ItemError. D7 and D10 are now decided and recorded in requirements v2.5.
No push or implementation is authorized.

Purge conflict handling and streaming limitations are specified in requirements
§§7 and 11. Composite references remain a later proposal (§13), not a replacement
for StackRef in this draft.

### Supplemental class dispositions from review

This supplements §4 and overrides its broad retirement labels where they conflict.
Paths below are module roots; class names identify the source files under src/main.
These are design dispositions based on declarations, dependencies and selected bodies,
not end-to-end qualification or permission to delete. Each adaptation still needs
caller/test tracing before implementation.

| Module / classes | Disposition and behavior to preserve |
|---|---|
| container/ledger: `DocumentAdmissionAuthorization`, `DocumentCreationAuthorization` | Adapt. Preserve authenticated account, current policy, creation, source and replay authorization. Remove coupling to publication owners only after these checks exist on canonical save/read/outcome paths. |
| container/ledger: `DocumentUploadAuthority`, `RepositoryCreationGrants` | Replace lifecycle-specific representations. Current grants bind an exact publication command; do not blindly carry owner/attempt authority into the new catalog. Preserve explicit creation authority and deny-by-default provider access. |
| container/ledger: `RepositoryCredentialAuthorities`; spi: `RepositoryCredentialBinding` | Keep/adapt credential identity and revocation checks. Credential generation is a security identity, not automatically a removable operation-owner generation. |
| container/blob: `PartStorage` | Adapt descriptor-driven part fan-out and selection. Current split produces PartObject byte arrays, and methods take bucket/version-oriented parameters. This is not yet the streaming stack implementation; preserve parallelism and replace buffered payload ownership. |
| container/ledger: `DocumentUploadCoordinator`, `DocumentUploadPlan`, `DocumentUploadPayloads`, `DocumentOperationUploadAdmission` | Fold useful placement, content matching, cancellation and provider completion behavior into the canonical upload/save operation. Replace command-member leases and buffered payload snapshots; no second upload lifecycle. |
| container/ledger: `DocumentRecord`, `DocumentRowKind`, `DocumentStatus`, `RawObjectRecord` | Replace storage representations with catalog rows and protobuf manifests. Preserve address identity, ownership and raw claim checks. Existing DocumentRecord JSON manifest/security paths are not reusable persistence. |
| container/ledger: `ListDocumentsFilter`, `ListDocumentsResult` | Adapt filtering, pagination and result roles to the new revision/catalog model; do not remove listing behavior with its old row type. |
| container/ledger: `LedgerException` | Adapt typed failure translation and causes; check all callers before replacement. |
| container/ledger: `DocumentSchemaPolicies` | Adapt host-selected policy reads and stale-policy rejection into the commit boundary; no client schema-selection authority. |
| container/ledger: `DocumentSchemaRetention` | Adapt transactionally retained descriptor/provenance associations. Keep atomic linkage with the accession; remove command-member and attempt dependencies. |
| container/ledger: `DocumentSchemaManifest` | Replace JSON encoding of retention sets with typed protobuf/catalog relations. Preserve complete expected-set checks. |
| container/ledger: `DocumentSchemaBatch`, `DocumentSchemaUnion` | Adapt complete descriptor closure collection, identity conflict detection and resource accounting to one composed candidate. Do not inherit operation-wide constants as document size ceilings. |
| container/ledger: `DocumentSchemaAdmissionBinding` | Replace pre-write operation-owner binding. Preserve the link between accepted candidate, policy and exact retained schema evidence on the accession. |
| admission: `DocumentSchemaAssetCodec` | Keep/adapt binary provenance metadata encoding. Its checks do not authenticate compiler claims or establish source retention by themselves. |
| container/ledger: `DocumentAdmissionSnapshot`, `DocumentDriveSnapshot`, `DocumentSourceSnapshot` | Replace lifecycle snapshots with explicit candidate/reference/catalog inputs. Preserve exact source identity and authorization; DocumentSourceSnapshot currently parses JSON manifests and records provider versions. |
| container/ledger: `DocumentCommandContent`, `DocumentOperationCommands`, `DocumentOperationSelection` | Retire old restart-command and selection-attempt representations after canonical intent/outcome recovery is covered. DocumentOperationCommands already reads binary commands; retirement is about lifecycle, not a claim that every old helper used JSON. DocumentOperationSelection does encode internal Struct/JSON. |
| container/ledger: `DocumentObservationFlusher`, `DocumentRecoveryPayloads` | Retire attempt observation queue and recovery payload clone lifecycle. Preserve cancellation/resource cleanup tests; cloned PartObject arrays are not a large-object strategy. |
| container/ledger: `DocumentReuseAdmission` | Replace its JSON selection batching and publication fences. Preserve exact-reference eligibility, source authorization and atomic shared-reference publication in the catalog. |
| container/ledger: `DocumentRetainedReader`, `DocumentRetainedReadPlan` | Adapt exact source, digest/length verification and cancellation behavior. Current port takes a pinned plan and returns buffered PartObject batches; it cannot be kept unchanged as the new streaming reader. |
| container/ledger: `DocumentRetainedManifestEntries` | Replace JSON manifest extraction/batching with typed retained entries. Preserve complete requested-key matching and provenance by exact revision. |
| container/ledger: `DocumentRuntimeArtifact`, `DocumentRuntimeClasspath`, `DocumentRuntimeInventory` | Preserve local provenance verification as candidate helpers, adapting their use only if retained validation provenance requires it. Actual caller is DocumentAssessmentRuntimeObserver; they inspect local artifacts/classpaths, not remote executable loading. They are not automatically part of document admission. |
| history/grpc: `HistoricalOccurrenceClient` | Retire the separate history transport client after callers move. Preserve verified retained-schema decoding in canonical historical reads; history itself is not retired. |
| service: `ManagedArchiveServices` | Frozen with the archive subsystem. Dependencies used here cannot be deleted merely because document code no longer needs them. |
| blob/redis: `RedisBlobStore`, `RedisBlobStoreProvider`, `RedisBlobStoreConfig` | Provider decision remains open. If retained, adapt to stack capabilities and explicitly report provider limits, buffering, expiry and persistence requirements; none becomes a platform document ceiling. |
| blob/redis: `RedisObjectKeys`, `RedisWriteMetadata` | Candidate reuse for injective key encoding and bounded metadata encoding, after checking compatibility with stack identity. |
| blob/redis: `RedisObjectScripts`, `RedisWritePolicy`, `RedisBackendIdentity` | Adapt only if Redis is retained. Replace incompatible overwrite/version identity semantics; retain actual atomic operation guarantees without claiming script rollback or archival durability. |

This explicitly covers the review's named classes and expands its unnamed Redis
sub-classes. It does not certify the review's aggregate count of 48, or claim the
entire module inventory has now passed call-path qualification.

### Ownership and registry source inventory (2026-10-14)

Checked on reconciliation base c75756ea4; this is source inspection, not runtime proof.

- repo/proto/src/main/proto/ai/protomolt/proto/repo/v1/document.proto:
  OwnershipContext already carries account, datasource, optional connector, security
  and source owner. Reuse it for document ownership; SaveOwner describes the writer.
- repo/proto/src/main/proto/ai/protomolt/proto/repo/v1/manifest.proto:
  WriteProvenance already carries module_id, node_id, graph_id and int64 graph_version.
  Its existing documentation allows unknown attribution. The new handler must enforce
  complete provenance for a step owner; importing it does not add those constraints.
- repo/spi/src/main/java/ai/protomolt/proto/repo/spi/RepositoryCaller.java:
  trusted-host caller identity includes account IDs, ACL identities and optional
  credential binding. Request-supplied owner fields cannot manufacture that identity.
- repo/spi/src/main/java/ai/protomolt/proto/repo/spi/DocumentAccessPolicy.java:
  existing tenant/ACL checks distinguish READ/WRITE and reject unresolved inherited
  policy. Its administrative processAuthority is explicit, not proof of step authority.
- repo/container/src/main/java/ai/protomolt/proto/repo/container/ledger/DocumentAdmissionAuthorization.java:
  requireCaller checks account/principal; authorizeReplay checks current read policy
  separately from old write preconditions. Adapt these guarantees without importing
  the old operation-owner/attempt lifecycle.
- mesh/cluster/src/main/proto/ai/protomolt/proto/mesh/cluster/v1/cluster_directory_service.proto:
  ClusterDirectoryService registers/discovers cluster nodes. A cluster machine node
  is not established as a workflow step or authorized output producer by this API.
- surface/grpc/workflow/src/main/proto/ai/protomolt/proto/grpc/workflow/v1/grpc_workflow.proto:
  WorkflowStep names a dependency/method or structured generation target;
  VersionedWorkflow contains an immutable slug version and workflow_fingerprint.
  This is a useful definition/descriptor source, but its string version is not
  WriteProvenance's numeric graph_version. No coercion or invented correspondence.
- mesh/proto/src/main/proto/ai/protomolt/proto/mesh/v1/entity.proto:
  ProfileReference provides name/version/fingerprint, not caller delegation to a step.

**W1 decision:** graph-registry verification is removed from the first slice.
The inventory above records existing capabilities, not a requirement to invent a
registry. Graph versions remain unverified provenance. VersionedWorkflow may help
an administrator generate grants later; no correspondence is assumed today.

Reuse RepositoryCredentialBinding for trusted issuer/UUID/generation identity,
RepositoryCredentialAuthorities for existing revocation-check semantics (including `requireLive`'s current-generation check, run inside the commit transaction), and
DocumentAccessPolicy for tenant/ACL enforcement. RepositoryCreationGrants currently
binds publication commands and placement; it is not the proposed owner-scoped
WriterGrant. Adapt the reusable authority checks to the selected JDBC catalog,
without carrying forward the removed command-owner lifecycle. No runtime writer-grant
implementation or administrative provisioning path is claimed complete here.

WriterGrant and WriterGrantEvidence are draft catalog/evidence messages only.
Requirements §§5, 7 and 9 define matching, commit-time revocation, retained binary
snapshots, replay and carried-opaque semantics. SaveIntent binds the claimed owner
and attestations, not a server-selected grant ID or its current revocation state.

### Fable review checklist for the W1 revision

Please check these before recommending implementation or push:

1. Authority: account, issuer, credential UUID and owner scope match; the generation is checked live at commit;
   optional node/connector scope cannot widen account/module/datasource authority.
   Process authority does not implicitly become a producer grant. Provisioning is
   administrative and cannot be reached through SaveDocument payloads.
2. Race: specify a real JDBC transaction conflict between save and revocation; a
   second read alone is insufficient. Both commit orderings and credential revocation
   during upload need tests. No database transaction spans the streamed upload.
3. Evidence: retained binary grant bytes, ID and digest agree; snapshots survive
   revocation and are not replaced by current grants. Check where their schema
   descriptor is retained for archival decoding. No JSON serialization is involved.
4. Replay: changed owner/metadata/attestation conflicts; a fresh trace ID does not.
   Current read access is checked, but revocation of the original write grant alone
   does not destroy idempotent access to a committed outcome.
5. Opaque reuse: prior source bytes/type/occurrence and producer evidence are verified;
   only the new writer needs an active save grant. A revoked historical producer
   cannot create new opaque bytes through a carry-forward claim. Intake producing
   new opaque data needs a separate eligible step grant. Review this distinction.
6. Annotation dialect/runtime: run valid and invalid fixtures through ProtoMolt's
   own ProtoValidator, not only upstream Protovalidate. Exercise absent oneofs,
   optional empty node/connector, UUIDs, missing grant snapshots, forbidden intake
   opaque permissions, and save versus tombstone response envelopes. Existing
   native annotations and buf.validate coexist in imports; verify both execute.
7. Retained metadata: ensure all owner, lineage, metadata and evidence needed by the
   contract have durable protobuf fields/associations and survive read/restart.
8. Grant selection and errors: overlapping matching grants select deterministically;
   distinguish missing owner, revoked/mismatched authority, and unavailable catalog.
   Check failure details do not reveal other principals or poison operation IDs.
9. Keep prior unresolved items visible: semantic consumer migration, archive freeze,
   exact canonical encoding fixtures, purge/reference race proof and actual large
   payload memory measurements. Composite pieces stay out of slice 1.

The draft compiles with imports; this revision has no runtime validator qualification,
transaction tests or implementation. Review findings are not authority to push.
