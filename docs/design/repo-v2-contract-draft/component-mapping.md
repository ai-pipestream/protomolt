# Repo service: contract and component mapping (draft for review)

For review only. Nothing here is implemented, and nothing is on a branch.

- **Implements:** requirements v2.2 (`docs/design/parts-based-repository-requirements.md`, branch `reconcile/parts-based-repository`).
- **Contract:** [document_service_v2_draft.proto](document_service_v2_draft.proto). It compiles with protoc against the v1 protos.

> **Source caveat.** I mapped class names from GitHub `main` (411 main classes under `repo/`).
> The branch couldn't be read from here, so it may have more classes or different ones. Check
> each name on the branch before acting on it. Roles come from each class's own Javadoc first
> line, not from a full read.

## 1. Decisions in the draft that need your call

| # | Decision | Draft default | Why |
|---|----------|---------------|-----|
| D1 | New package `repo.v2`, or edit v1 in place | **v2 package.** v1 is deleted after §15 passes. | Retired shapes can't leak back in, and the old and new services can run side by side during sequencing. |
| D2 | Unconditional saves (v1 `force_save`) | **Removed.** Every save names an expected revision, or "none" for a new document. | §7 says CAS is the only coordination. A crawler that re-saves blindly does a `GetManifest` first, or retries on `ABORTED`. |
| D3 | Is a tombstone a revision? | **Yes.** It gets a revision, an operation ID and an expected revision. | History shows the delete. A later save over it is an ordinary CAS. |
| D4 | Purge events | **Two events:** `PurgeMarked` when references are marked, `PurgeCompleted` when bytes are deleted. | This matches v1's requested/purged pair and lets compliance see when bytes are actually gone. |
| D5 | Where segmentations live in `Document` | **New fields** `SearchMetadata.segmentations = 16` and `embedding_sets = 17`. Field 13 is reserved after the consumers move. | Additive, but it waits on the §16.2 consumer inventory. |
| D6 | Offset units allowed | UTF-8 byte, UTF-16 code unit, code point | These cover Java, Python and Rust. Unspecified is rejected. |
| D7 | Hibernate in the catalog | **Drop it** in the new catalog: plain JDBC plus HikariCP plus Flyway. | JDK-first, minimal dependencies. The new catalog is about six tables and a single transaction, so JPA adds nothing. |
| D8 | v1 conditional-blob RPCs (`PutBlob`, `GetBlobForUpdate`, `CompareAndPutBlob`, `DeleteBlob`) | **Removed** | They contradict write-once stacks. If a caller depends on them, that has to be found before deletion. |
| D9 | Upload shape | **One blob per `UploadDocument` stream.** The header carries the full save request. | Simplest form that gives HTTP and gRPC the same commit. Multi-blob uploads can come later. |

## 2. What the contract does, in short

- **Writes**
  - `SaveDocument` and `SaveDocuments` (bidi; one response per request, correlated by `operation_id`).
  - `UploadDocument` (client stream; the HTTP route maps onto it).
  - `DeleteDocument` (tombstone) and `PurgeDocument` (admin).
- **Outcome lookup:** `GetOperationOutcome` returns either `committed` or `none`. The third state, "lookup unavailable", is gRPC `UNAVAILABLE`.
- **Reads**
  - `GetDocument` (unary) and `ReadDocument` (the manifest frame first, then part bytes).
  - `ReadBlob` (byte range, plus a SHA-256 check at the end of a full read).
  - `GetManifest` (no payloads), `ListRevisions` (newest first, paged) and `ListDocuments`.
- **Part selection.** A `PartSelector` is one of a fixed part (CORE/BLOBS/PARSED), a segmentation ID, or an embedding-set key. Duplicate or contradictory selectors are rejected.
- **Save semantics**
  - `writes` lists what changes: PUT, EMPTY or DELETE.
  - Content in `document` for any part not listed as PUT is rejected, never dropped.
  - `sources` takes parts from exact revisions of other documents.
  - Everything else carries forward from the expected revision.
- **Intent.** Intent is a SHA-256 over the call number, expected revision, mode, each write (selector, action, content SHA-256) and each source. It is stored with the outcome and drives replay versus `ALREADY_EXISTS`.
- **Public manifest.** It lists every part with its state, size, SHA-256, the revision that wrote it, `segmentation_digest`, `opaque` and `content_included`. Stack references are internal (`StoredManifest` and `StackRef`).

### Error table

| gRPC status | Detail message | When | Recorded as an outcome? |
|-------------|----------------|------|-------------------------|
| `ABORTED` | `RevisionConflict` (current revision, tombstone flag) | Stale expected revision, after the same-ID outcome re-read | No |
| `ALREADY_EXISTS` | `OperationReuse` | Same operation ID, different intent | No |
| `INVALID_ARGUMENT` | `SaveRejection` (`MALFORMED_REQUEST`, `VALIDATION_FAILED`, `INVALID_BYTES`, `UPLOAD_MISMATCH`) | Request structure or the composed candidate fails | Yes, for validation failures |
| `FAILED_PRECONDITION` | `SaveRejection` (`SCHEMA_MISSING`, `SOURCE_PURGED`, `SOURCE_NOT_FOUND`, `SEGMENTATION_MISMATCH`) | The world isn't as the request assumes | Yes |
| `PERMISSION_DENIED` | `SaveRejection` (`OPAQUE_NOT_ALLOWED`) | Opaque mode requested, but host policy refuses it | Yes |
| `UNAVAILABLE` | none | Registry or catalog unreachable | Never |
| `NOT_FOUND` | none | Exact revision, blob or document doesn't exist | n/a |
| `DATA_LOSS` | none | Stored bytes fail their SHA-256 check | n/a |

In `SaveDocuments`, the same code and detail arrive per item as `SaveError`.

**Question.** Recording validation rejections as outcomes means a retry with fixed content must use a **new operation ID**. That follows from §7 ("a recorded failure such as a validation rejection"), but please confirm it's what you want.

## 3. Target components

```
repo/proto       v2 contract (this draft)
repo/codec       parts codec: CORE/BLOBS/PARSED + CHUNKS per segmentation + EMBEDDINGS per set
repo/stack       Stack interface (was blob/spi) + s3, redis, cache providers
repo/catalog     Catalog interface + postgres (JDBC) implementation; Mongo later
repo/validation  composed-candidate validation, opaque handling, retained schemas (was admission)
repo/engine      DocumentSaves, DocumentReads, Purges: orchestrates codec + stack + catalog + validation
repo/service     DocumentService gRPC, HTTP upload route, DriveService, wiring/main
```

The **Stack** interface (§6):
- `put(InputStream) -> StackRef` (key, SHA-256, length);
- `get(key, range) -> InputStream`;
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

The baseline checks for the `flyway_schema_history` of V1–V120 and refuses to run if it finds it.

## 4. Mapping of existing classes

Legend: **keep** = used as is, **adapt** = kept but changed, **replace** = role kept but new code, **retire** = deleted after §15 passes (port any test that covers a still-required failure case), **frozen** = untouched pending an open question.

### Codec (`repo/codec`, 9 classes): keep the core

| Class | Verdict | Change |
|-------|---------|--------|
| `DocumentPartCodec`, `PartLayout`, `PartLayouts` | adapt | Add EMBEDDINGS. CHUNKS is keyed by segmentation ID, EMBEDDINGS by `segmentation_id/embedding_set_id`. Fix the "byte-for-byte" claim to say semantic equality plus preserved unknown fields (§15.7). |
| `DocumentRevisionAssembly`, `PartObject` | keep | |
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
| `CachingBlobStore` | keep | Write-once keys make it trivially safe. |
| `RemoteBlobStore` (blob/grpc) | verify, then keep or retire | The "dogfood" remote store. Keep only if something deploys it. |
| `BackendIdentity`, `S3BackendIdentity`, `RedisBackendIdentity` | replace | Become the `StackRecord` catalog row. |
| `PhysicalObjectIdentity`, `PhysicalObjectLocation` | replace | Become `StackRef`. |
| `PayloadBudget` | keep | Bounded memory for streaming. |
| `ExactVersionReclaimer`, `S3ExactVersionReclaimer`, `ExactVersionOutcome`, `ObjectReclaimer`, `S3ObjectReclaimer`, `ExpiringBlobStore` | retire | Provider-version tracking and automatic reclamation are out (§6). |
| `NamespaceProvisioner`, `S3NamespaceProvisioner` | keep | Bucket setup. |
| `OpenedBlobStore`, `BlobCapability`, `BlobStoreException` | adapt (verify) | |

### Catalog (`repo/container`, 233 classes): mostly retire

**Keep or adapt**

| Class | Verdict | Change |
|-------|---------|--------|
| `JdbcEventOutbox`, `EventRelay` | keep, adapt | v2 `DocumentEvent`; plain JDBC instead of JPA. |
| `DocumentEventFactory`, `DocumentEventRecord` | adapt | One event per accession, listing changed parts. |
| `DocumentIds` | keep | Deterministic call-number UUIDs. |
| `LedgerDatabase`, `LedgerConfig`, `SqlTimeouts` | adapt | Pool, then the fresh baseline with its refuse guard. |
| `Tx` | replace | JDBC transaction helper; no `EntityManager`. |
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
| `DocumentAnyMaterialization`, `DocumentAnyRootInventory` | keep | Opaque handling (§9). |
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

- Of 411 main classes, about 60 survive as keep or adapt, about 15 are new or replacement code, about 35 archive classes are frozen, and the rest (about 300) retire after §15 passes.
- The 120 migrations are replaced by the baseline (§14).
- There are about 570 test classes. Each retiring test is checked against §15 and the failure cases first.

## 5. Next steps after review (not started)

1. Settle D1–D9 and the Redis and remote-store questions.
2. Verify this mapping against the branch's class list.
3. Do the §16.2 consumer inventory before D5 lands.
4. Then implementation, in this order: proto, stack interface, catalog and baseline, codec changes, engine, service, the §15 tests, and retirement last.
