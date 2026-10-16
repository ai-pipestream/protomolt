# Repo service: call paths and change map for the v2.5 contract (plan step 2)

> **Status (2026-10-15).** A dated plan-step-2 trace of main 974ea302. Where it differs from
> `parts-based-repository-requirements.md` or `component-mapping.md` §7-8, those documents win.
> Delete this file together with the legacy code it traces (plan step 5).

Kristian asked for this through the project coordinator on 2026-10-15 (plan step 2). It traces one save and
one read end to end through today's repo service, then maps each step of the v2.5 contract onto the code
that exists, method by method. Nothing was implemented, built or pushed, and nothing in the worktree, main
or the archive was edited.

**Source.** I traced GitHub main at 974ea302 (a read-only local copy). The krick worktree
(`reconcile/parts-based-repository`) matches it file for file, by size, in the engine, container ledger, blob
and lifecycle, admission, codec and schema-registry packages. Every difference is publication wiring: the
worktree lacks `DocumentPublicationGrpcService` and `document_publication_service.proto` (it has
`document_publication_execution.proto` instead), and `RepoServices`, `ManagedDocumentServices`,
`ManagedPublicationOptions`, `RepositoryDrainTimeoutException`, `DocumentPublicationRepository` and the
rehearsal fixture differ in size (§1.1). I couldn't compare bytes: the staging tool refuses files nested more
than seven folders below the connected folder, and every Java source is ten or more deep.

**Confidence.** Source reading only. Nothing was compiled or run for this map. Line numbers are on 974ea302.
Two read-only helper agents traced the managed publication path and the semantic consumers; I checked their
load-bearing claims against the source before using them.

Contents
- 0. What changes the plan
- 1. Today's save paths
- 2. Today's read paths
- 3. The v2.5 save, step by step, mapped to today's code
- 4. The v2.5 read, mapped to today's code
- 5. Methods that change, new pieces, and removable machinery
- 6. Consumers of SemanticProcessingResult (§16.2)
- 7. Where the v2 files go under repo/proto, and what the build needs
- 8. Contract gaps the trace found
- 9. A bounded first slice to approve

---

## 0. What changes the plan

Read this section first; the rest is the evidence.

1. **Production runs only the legacy save, and it validates nothing.** gRPC `SaveDocument` overwrites the one
   documents row per call number, with no request validation, owner, grant, `Any` check or operation outcome
   (§1.2).
2. **The path that validates is built but unreachable.** The managed publication (path B) has typed `Any`
   validation, operation receipts, retained schemas and verified reads. Both production entry points build the
   service without the schema access and options it needs, so none of its services is mounted (§1.1, §1.5).
   Only a verification fixture turns it on, so retiring it breaks no production caller in this repository.
3. **One save has three implementations**: path (A), a managed branch inside `DocumentOperations` that nothing
   constructs, and path (B). The duplication runs through authorization, outbox writes, reads and retry
   handling too (§5.3). v2.5 replaces all of it with one save handler and one read handler.
4. **What carries over is small and easy to name**: the admission engine (`DocumentSchemaAdmission`,
   `DocumentSchemaAssessment`, `DocumentPayloadCheck`), the codec's split and assemble, the size and SHA-256
   checks in `DocumentPartReader.readPart`, `DocumentAccessPolicy` and the caller binding, `requireLive` with its
   V99 `FOR SHARE` function, the hashed streaming upload, `EventRelay`, and path (B)'s revision tables as the
   model for the new catalog. The catalog itself is new code in a new module; Hibernate's `Tx` stays for the
   archive, drives and purge.
5. **Most of the ledger package becomes removable.** 166 of its 197 files, about 24,600 of its 28,700 lines,
   serve leases, claims, handoff, reader pins, upload attempts, publication journals, assessment slots and
   historical reuse, none of which v2.5 has (§5.3). Two classes component-mapping.md lists for retirement or
   folding, `DocumentSchemaAssessment` and `DocumentPayloadCheck`, are the validation engine itself and should
   stay.
6. **Validation can skip carried parts today.** `document.proto` carries no validation rules, so no rule spans
   parts, and the new entry can validate written parts only (S11 in §3). The draft still has to say where a
   carried part's validation entries come from (gap 9) and which policy `policy_sha256` names (gap 10).
7. **Open item 1, the `FOR SHARE` lock, is answered by existing code.** Credential rotation and revocation
   already work that way (V99), and writer-grant revocation can copy it (§5.1).
8. **§16.2:** in this repo only the Qdrant point mapper reads `semantic_results`; the real writers are pipeline
   modules elsewhere. Keep segmentations and embedding sets out of slice 1 (§6).
9. **Needed from Kristian before slice 1 (§9):** gaps 1 and 2 (reject a blank `doc_id`; pipeline saves send a
   full `NodeAddress`), the package name (D1), the validation dialect (the house dialect is my recommendation,
   §7.2), and approval of the slice itself.

---

## 1. Today's save paths

### 1.1 Wiring

- `RepoServices` builds the document engine at RepoServices.java:310-313:
  `new DocumentOperations(documentLedger, driveLedger, tx, blobStore, partStorage, purgeQueue, eventOutbox, bounded == null ? generation : null)`.
  That is the eight-argument constructor (DocumentOperations.java:203). It leaves `managedParts` and
  `managedWriter` null, so the managed branches inside `saveFull` (360) and `savePartial` (465) can't run
  from gRPC, and `assemble` refuses managed publications (678). `DocumentGrpcService` wraps this instance.
- `ManagedDocumentServices` is constructed separately (RepoServices.java:376-379) when managed storage has a
  retention-qualified generation and the host isn't a bounded archive host.
- **Path (B), the managed publication, is not reachable in production.** Both production entry points call
  `RepoServices.build(config)` (RepoServiceMain.java:30, RepoServiceModule.java:47), and that constructor chain
  passes no history access, no schema access and no publication options (RepoServices.java:136-156). Even when
  `ManagedDocumentServices` exists, it then has no publication repository, publication gRPC service, history
  service or materialization service (ManagedDocumentServices.java:97, 116-120, 147), and `services()` mounts
  none of them (RepoServices.java:612-621). Nothing in src/main implements `ManagedSchemaAccess` except the
  recovery-rehearsal fixture (RehearsalFixture.java:154, 170), which calls the publication as a library through
  `publicationRepository()` (RepoServices.java:531).
- **Branch difference.** Main has `DocumentPublicationGrpcService.java` and
  `document_publication_service.proto`. The worktree has neither, has `document_publication_execution.proto`
  instead, and differs in size in `RepoServices`, `ManagedDocumentServices`, `ManagedPublicationOptions`,
  `RepositoryDrainTimeoutException` and `DocumentPublicationRepository`. Main's copies are dated later, so the
  branch most likely predates main's publication gRPC service (inferred, not checked with git). On the branch,
  then, path (B) has no gRPC entry at all. None of these files is on the basic save or read path.

### 1.2 gRPC `SaveDocument`, full save (the path production runs)

| # | Step | Where | What it does |
|---|------|-------|--------------|
| 1 | Entry | DocumentGrpcService.java:80 → DocumentOperations.saveDocument (240) | `caller()` (DocumentGrpcService.java:60) binds the authenticated caller to a `RepositoryCaller`. `requireReadBinding` (737) needs process authority or at least one account binding. |
| 2 | Resolve the call number | SaveResolution.resolve (SaveResolution.java:53) | Requires document, ownership, account and drive. **A blank `doc_id` gets `UUID.randomUUID()` (line 59).** Intake arm: `graph_id` must equal `intake:<account>`, the datasource becomes `graph_address_id`, `cluster_id` is refused (89). Pipeline arm: `graph_location_id` and `graph_id` are required, `cluster_id` is optional (126). |
| 3 | Account, call number, current row | saveBlocking (267-273) | `requireReadAccount` (748); `DocumentIds.nodeId(address)` (DocumentIds.java:54); `findByNodeId`. |
| 4 | Write authorization | requireWrite (785) | **Creating a document requires process authority (786).** An update needs WRITE through `DocumentAccessPolicy.allows` (DocumentAccessPolicy.java:15) and AVAILABLE status. Without process authority a save can't change security policy, drive, datasource or source-deletion settings. |
| 5 | Drive | 282-286 | `drives.findByName`, then `SaveResolution.basePrefix`. |
| 6 | Split | saveFull (299), 307-308 | `DocumentPartCodec.split(doc, PartLayouts.document())` (DocumentPartCodec.java:72) returns byte[] `PartObject`s; `rootChecksum` (269) hashes them. |
| 7 | Dedupe under the row lock | 319-358, via DocumentLedger.withLockedReference (333) | Account writer lock plus `FOR UPDATE`; re-checks the sampled revision; an intake save with the same root checksum and no `force_save` bumps `reprocess_count` and returns `deduplicated` without writing. |
| 8 | Write objects | 371-379 | Random attempt prefix (`writeAttemptPrefix`, 1052); `reserveLegacyPartKeys` in its own transaction (DocumentLedger.java:53); `PartStorage.writePartObjects` (PartStorage.java:116) puts byte[] parts in parallel. The store checks SHA-256 only when `force_save` is set, and `written_by` is copied from the request unchecked. |
| 9 | Commit | upsertRow (1063) → DocumentSaveCandidate.build → DocumentLedger.saveIfRevision (101) → saveGuarded (136) | One transaction: `lock_document_schema_policy_account` (DocumentSchemaPolicies.java:27), then `lock_document_revision_keys` advisory locks and `FOR UPDATE` (lockRevisions, 162), `requireRevision` (211), `em.merge` **over the existing row**, flush and refresh. The callback publishes raw-object bindings (`ManagedRawBindings.Plan.publish`) and enqueues `DocumentEventFactory.saved` (DocumentEventFactory.java:48) through `JdbcEventOutbox.enqueue` (JdbcEventOutbox.java:50). |
| 10 | Response | SaveResolution.saveResponse (160) | Node ID, drive, prefix, size, checksum. |

Never done on this path: request validation with ProtoValidator, owner checks, writer grants, the credential
liveness check, `Any` resolution, composed-candidate validation, an operation outcome, a revision row, part
references, retained schema evidence. `DocumentOperations` has no dependency on the admission module at all.

### 1.3 Partial save

`savePartial` (DocumentOperations.java:396-560):
- The copy source must be an AVAILABLE row the caller can read, in the same account unless the caller has
  process authority (406-421).
- Content is split, then filtered to `parts_written` and `chunk_sets_written` (440-444). **Content for any
  other part is dropped silently.** v2.5 rejects it.
- Every source-PRESENT part not written is carried (449-454). **Carried parts are copied** into the new
  attempt prefix (`PartStorage.copyParts` at 512; PartStorage.java:216), and a carried CORE is read back in
  full and re-hashed (533-541). v2.5 carries by reference and copies nothing.
- `combineManifests` (562) restores codec order; `ManagedRawBindings.verifyCopy` checks claim checks; the
  commit is the same `upsertRow`, with the source's sampled revision held under lock (sourceRevisions).

### 1.4 HTTP upload

UploadHttpServer.handle (179) → upload (222) → `RawIngestionOperations.upload` (RawIngestionOperations.java:52)
as `new RepositoryCaller("http-upload", true)`: process authority, no credential. Then `ingest` (76):
1. leases a raw-object row (`RawObjectLedger.begin`), streams the body with `BlobStore.put(InputStream)` (88)
   while hashing it, checks length and the declared SHA-256, and calls `raw.verify`;
2. defaults `doc_id` to a name-based UUID of the content SHA-256 (110), not a random one;
3. builds an intake `Document` carrying the blob claim check and calls `DocumentOperations.saveIngested` (142),
   which requires process authority and a full save and runs `saveBlocking` with an `IngestionAdmission`;
4. on a revision conflict re-reads the current row and tries again, up to three times
   (`PUBLICATION_ATTEMPTS`, 25), so the last writer wins.

This already matches decision 27: intake owner, process authority, no credential. In v2.5 the route also needs
an operation ID and an expected revision (§8, gap 6).

### 1.5 Managed publication save (path B)

Traced on main. Nothing in production reaches it (§1.1). On the branch everything from the facade down
matches main by size; only the gRPC entry is missing. One `PublishDocument` call in journaled mode, for a
fresh operation whose members are all accepted:

| # | Step | Where | What it does |
|---|------|-------|--------------|
| 1 | Entry (main only) | DocumentPublicationGrpcService.publishDocument (41-80) | Binds the caller through the transport bindings (107-120) and runs ProtoValidator on the envelope only (65-66). The command's SHA-256 covers its sorted members but not the operation ID (DocumentPublicationCommand.java:41-73). |
| 2 | Facade | DocumentPublicationFacade.publishDocument (41-68) | Requires a caller, takes permits and budgets, and checks each upload's SHA-256 against its declaration (DocumentPublicationInput.java:88-92). |
| 3 | Replay | DocumentPublicationRuntime.publishValidated (422-467) → DocumentPublicationReplay.observe (58-172) | Returns the terminal receipt if one exists: `repository_operation_success` or `repository_operation_rejection`, keyed by account, principal and operation ID. |
| 4 | Account and drives | ManagedPublicationSelection.select (29-45) | Account membership unless process authority; one placement per drive. |
| 5 | Session | DocumentPublicationSessions.execute (228-284) → DocumentPublicationSession (105-125) | An in-memory session per operation, with a random owner nonce, attempt IDs and upload tokens. Object keys are `…/attempts/{attemptId}/part-{n}` (DocumentUploadPlan.java:133-141). |
| 6 | Registration | DocumentPublicationRegistration.admitInitialAccepted (145-223) | One transaction inserts an execution claim with lease and fence tokens, a coordinator binding, the preparation and mode journals, and the operation with its owner (lease, generation 1). |
| 7 | Authorization | DocumentAdmissionAuthorization.lockAndAuthorize (201-269); it runs again in steps 8, 9 and 12 | READ on sources and WRITE on the destination through `DocumentAccessPolicy`. Creating a document without process authority needs a creation grant and a credential binding (224-227, 250-252). A credential-bound caller must pass `RepositoryCredentialAuthorities.requireLive` (256). Changing security policy, drive, datasource or deletion settings needs process authority (237-248), the same rule `requireWrite` enforces in path (A). |
| 8 | Policy and pins | DocumentPublicationExecution.executeAssessed (162-189) | Requires an active account schema policy (`StalePolicy` otherwise, DocumentSchemaPolicies.java:127) and inserts reader pins. |
| 9 | Upload | DocumentUploadCoordinator.execute (205-310) → DocumentPartTransfer.upload (31-54) | Attempt rows move through PLANNING, STAGING and VERIFIED, with key reservations and lease heartbeats. **This is the only object write:** a byte[] put, then a whole-object read-back that checks SHA-256, content type, version ID and ETag. Uploads finish before validation starts. |
| 10 | Validation | DocumentPublicationAssessment.prepareInternal (354-426) | Reads every reused part back (DocumentPublicationInputs.java:85) and runs typed admission on each typed member (S11 in §3). Opaque members get only `DocumentCommandContent.check`. Failures are recorded, not thrown. |
| 11 | Schema staging | DocumentSchemaBatch.stage → RepositorySchemaArtifacts (89) | A separate transaction writes artifacts, payloads and claims. |
| 12 | Commit | DocumentPublicationCommit.commitInternal (79-205) → DocumentCommitWriter.write (129-168) | One transaction: owner fence, policy lock, authorization again, retention locks and `document_revision_schema_admissions`. Then, per member, an `em.merge` over the documents row (134), `document_revision_commits`, `_publications` and `_parts`, schema retention rows, the `document_revision_current` pointer, and the outbox row through `em.persist` (165) rather than `JdbcEventOutbox.enqueue`. Last, `repository_operation_success`. |

A rejected member writes assessment-slot rows and a `repository_operation_rejection` receipt instead
(DocumentPublicationAssessmentExecution.java:49-94). Its uploads have already happened, so it leaves staged
objects and attempt rows behind. I didn't trace how they are reclaimed.

What path (B) means for the repair:
- **It holds all the validation and write authorization that exists today**: typed `Any` validation, creation
  grants and the credential liveness check (steps 7 and 10).
- **Its revision tables are the nearest model of the v2.5 catalog**: immutable revision rows carrying operation
  identity and a TYPED or OPAQUE decision, part references by object ID, and a current pointer. It still
  overwrites the documents row, keeps a JSON body column, and wraps everything in leases, fences, claims and
  pins that v2.5 drops.
- **It can't share an account with path (A).** It requires an active schema policy, and the trigger
  `a0_document_body_schema_policy` (V59:75; its function is replaced at V61:127-146) rejects any documents body
  write in such an account unless the same transaction wrote a matching `document_revision_schema_admissions`
  row. Path (A) never writes one, and policy pointers can't be deleted (V59:22). Nothing in production
  activates a policy: `DocumentSchemaPolicies.activate` has only test callers, and the rehearsal fixture inserts
  the rows itself because "no policy API exists" (RehearsalFixture.java:42-43, 188-206). So this is latent.
- **Nothing in src/main provisions grants or credentials.** `RepositoryCreationGrants.install` and `revoke`, and
  `RepositoryCredentialAuthorities.register`, `revoke` and `rotate`, are package-private and called only from
  tests. Outside tests, creating a document through (B) needs process authority, and a credential-bound caller
  can't write at all.

---

## 2. Today's read paths

### 2.1 `GetDocument`

DocumentGrpcService.java:89 → `DocumentOperations.getDocument` (631):
1. `requireReadBinding`, parse `node_id`, `findByNodeId`;
2. `requireVisibleRead` (758): ACL through `DocumentAccessPolicy.allows` plus AVAILABLE status;
3. `assemble` (668): reads the JSON manifest from the row (`row.readManifest()`), then
   - with a managed publication (`partPublications().findForRead`), reads through `DocumentPartReader`. That
     reader is null in production wiring, so this throws FAILED_PRECONDITION "Managed document reader is not
     configured" (678);
   - otherwise `PartStorage.readParts` (PartStorage.java:250): byte[] gets with **no size or SHA-256 check**,
     and null on a parse failure, which `assemble` turns into UNAVAILABLE.

`GetDocumentByReference` (646) does the same by address; `GetDocumentManifest` (703) returns the row's JSON
manifest or the publication's manifest (722-724). So for a document last written by path (B), v1 returns the
manifest but can't return the content. No read takes a revision, because only the current row exists.

### 2.2 Managed reads and history

`DocumentPartReader.read` (DocumentPartReader.java:121) → `readFragments` → `readBoundParts` (315) →
`readPart` (556): a bounded byte[] read up to the recorded size, then size, SHA-256, content type, provider
version and ETag must all match or the read fails with DATA_LOSS. A 32-slot semaphore bounds concurrent reads.
**This is the only read path that verifies bytes.**

Exact past revisions are read through `DocumentHistoryGrpcService.readRevision` and, for one recorded `Any`
occurrence, `DocumentHistoryMaterializationGrpcService.readHistoricalOccurrence`. Neither is mounted in
production (§1.1). One revision read:
1. `DocumentReadLedger.captureHistorical` (81-100) runs one transaction: the reader must be active, history
   access is authorized, and a reader pin goes into `document_read_pins` (released later).
2. `DocumentPartReader.readHistorical` → `readPart` (254-262, 556-584) is the verified read above, on the
   backend that wrote the revision.
3. `DocumentHistoricalSchemas.check` (28-66) loads the revision's retained policy, artifacts, assets and evidence,
   and re-runs `DocumentSchemaAdmission.check` (291-330) against retained artifacts only, never the registry.
   Any mismatch is DATA_LOSS.
4. Authorization runs again before the response leaves (DocumentHistoryGrpcService.java:83-86).

The occurrence read decodes the selected occurrence with retained descriptors (`DocumentSchemaMaterialization.read`,
114-256). History writes nothing except its pins. v2.5 covers revision reads with `GetDocument` at an exact
revision and has no RPC for a single occurrence. Outside repo/service, only verification code calls the history
and publication services: `ContentChecks` in verification/repository-recovery, and the client modules
repo/history/grpc and repo/publication/grpc, which only verification modules (and a test-runtime configuration
in repo/container) depend on.

---

## 3. The v2.5 save, step by step, mapped to today's code

"Keep" means the method carries over as is; "adapt" means the named change; "new" means nothing exists.

| # | v2.5 step (requirements) | Today | Change |
|---|--------------------------|-------|--------|
| S1 | Validate request structure: buf.validate, CEL and native rules (§9) | Hand-written checks in SaveResolution; BlobOperations and the archive code already call `ProtoValidator.create()` (BlobOperations.java:40) | **New** entry check on `SaveDocumentRequest`. The fixtures in reviews/contract-draft/fixtures already run the draft through ProtoMolt's ProtoValidator. |
| S2 | Caller binding | requireReadBinding (DocumentOperations.java:737) | Keep. |
| S3 | Call number from `DocumentTarget` (§5) | SaveResolution.resolve (53); DocumentIds.nodeId (DocumentIds.java:54) | **Adapt:** reject a blank `doc_id` instead of minting one (59); map the three target arms; settle `graph_id` and `cluster_id` (§8). Keep DocumentIds. |
| S4 | Account scope | requireReadAccount (748) | Keep. |
| S5 | Owner (§5): required; intake must match ownership; process authority is intake-only; a step needs `module_id` | Nothing. `written_by` is copied unchecked (PartStorage.java:116) | **New** owner check. |
| S6 | Early outcome lookup; a replay needs only current read access (§7) | Nothing | **New:** look up (account, operation_id) and compare the intent digest before any upload. |
| S7 | Canonical `SaveIntent` and its SHA-256 (§7) | Nothing | **New.** It needs each PUT's SHA-256 and length, so it runs after the split, or after the upload stream ends. |
| S8 | Writer grant selection, `allow_create`, opaque type allowances (§5) | Nothing on this path. `RepositoryCreationGrants` binds an exact publication command and can't be reused as is | **New** `writer_grants` table and selection by unsigned UTF-8 grant ID. |
| S9 | Document write policy (§5) | requireWrite (785); DocumentAccessPolicy.allows (DocumentAccessPolicy.java:15) | **Adapt:** creation no longer needs process authority (786); a grant with `allow_create` allows it, and `derived_from` is lineage only (decision 31). After creation the account, datasource and connector are fixed, and any authorized writer may change security (decision 30). |
| S10 | Compose the candidate: the expected revision's parts + writes + sources + carried parts; reject content outside PUTs (§7, §9) | savePartial (396-560); combineManifests (562); DocumentPartCodec.split (72) | **Adapt:** compose from catalog part references instead of the row's JSON manifest; carry by reference (no `copyParts`); reject, don't drop, unlisted content (440-444); `PartLayout` gains segmentations and embedding sets (§7). |
| S11 | Validate the composed candidate; typed by default; opaque rules; read carried parts only when a rule needs them (§9) | Not on this path. Only path (B) validates: DocumentAdmissionPolicy.assess (DocumentAdmissionPolicy.java:133-140) → DocumentSchemaAssessment.prepare (50-150). That first runs DocumentSchemaAdmission.checkMember (435-450): assemble the whole document from every fragment, match ownership, allow `Any` only at `structured_data` and the parser shape (490-511), and run ProtoValidator with both rule sources (23-26). Then, for the CORE and PARSED parts, it inventories `Any` roots, selects each root's schema from the registry, and decodes and validates each payload, nested `Any` included, in `DocumentPayloadCheck`. | **Adapt, keeping that engine.** Change its entry from a `DocumentPublicationMember` plus every fragment to the composed candidate. `document.proto` has no validation annotations at all, so no rule spans parts today: validate the written parts, take ownership from the catalog, and read no carried part (§9 counts such reads). Return `RevisionValidation` instead of assessment slots. Carried parts' validation entries are gap 9 (§8). |
| S12 | Write new part bytes to the stack, write-once, verify SHA-256 and length (§6) | writePartObjects (PartStorage.java:116); reserveLegacyPartKeys (DocumentLedger.java:53); writeAttemptPrefix (1052) | **Adapt:** keep the parallel fan-out; stream through `BlobStore.put(InputStream)` (BlobStore.java:173) with the server computing SHA-256 and length; drop reservations and attempt prefixes. |
| S13 | Commit (§7): outcome lookup, CAS, revision + part refs, outcome, outbox, pointer; plus grant and credential recheck and the purge conflict point | saveIfRevision → saveGuarded (DocumentLedger.java:101-154); RepositoryCredentialAuthorities.requireLive (62); JdbcEventOutbox.enqueue (50); DocumentEventFactory.saved (48) | **Replace** with JDBC inserts and a conditional pointer update. A validation failure records only a terminal outcome, as (B) does with `repository_operation_rejection`; an outage records nothing (requirements §7). Keep requireLive and copy its `FOR SHARE` pattern for grants (§5.2). Drop the schema-policy account lock unless the policy survives (gap 10). |
| S14 | Conflict: fresh-transaction outcome re-read; ABORTED with `RevisionConflict`; ALREADY_EXISTS with `OperationReuse` (§7) | RevisionConflictException → CONFLICT → ABORTED (GrpcErrors.java:68), with no detail message | **Adapt** GrpcErrors to attach detail messages (`io.grpc.protobuf.StatusProto`, already on the classpath through grpc-protobuf). Nothing in the codebase attaches status details today. |
| S15 | Response: accession and `replayed` | SaveResolution.saveResponse (160) | **Replace.** |

---

## 4. The v2.5 read, mapped to today's code

| # | v2.5 step | Today | Change |
|---|-----------|-------|--------|
| R1 | Validate the request, reject duplicate selectors | DocumentRequests.partsOrThrow (DocumentRequests.java:26) | **Adapt** to ProtoValidator plus a duplicate-selector check. |
| R2 | Access | validateAddress (42); requireReadAccount; requireVisibleRead (758); DocumentAccessPolicy.allows | Keep the checks. The policy comes from the revision's stored security (binary, not the JSON column). |
| R3 | Resolve the revision once: current or exact (§8) | Only the current row exists | **New:** catalog manifest lookup by (call number, revision). |
| R4 | Select parts by part, segmentation ID or embedding-set key | partsOrThrow; chunk set names | **Adapt.** |
| R5 | Read bytes and verify size and SHA-256; stream large parts (§11) | PartStorage.readParts (no check); DocumentPartReader.readPart (checks, byte[]) | **Adapt:** keep readPart's checks; read through a new ranged streaming get, since `BlobStore` has a streaming put but no streaming get. |
| R6 | Assemble with retained schemas; refuse opaque content unless `allow_opaque` (§9) | DocumentPartCodec.assemble (220) | **Adapt:** typed reads refuse opaque parts. Assembling needs only the container schema retained with the revision (see §6.3, item 3). Payload schemas matter only where the server decodes an `Any`, such as a JSON edge or a re-verification, through `DocumentRetainedSchemaResolution`. |
| R7 | Respond with document, manifest (`content_included`) and `RevisionValidation` | assemble builds the v1 response | **Replace.** |

---

## 5. Methods that change, new pieces, and removable machinery

"After §15" means the requirements' rule: nothing is deleted until its replacement passes the §15 acceptance
tests, and a test is ported before it is removed if it covers a failure case the requirements still need.

### 5.1 Methods on the traced paths

| Class | Method (line) | Disposition |
|-------|---------------|-------------|
| DocumentGrpcService | `caller` (60) | Keep. |
| | `saveDocument` (80), `getDocument` (89), `getDocumentByReference` (94), `getDocumentManifest` (110), `deleteDocument` (114), `listDocuments` (118) | Replace with the v2 RPC handlers. |
| | `getBlob`, `putBlob`, `deleteBlob`, `getBlobForUpdate`, `compareAndPutBlob` (122-138) | Remove with D8, after the four calling modules move (§8, gap 5). |
| DocumentOperations | `saveDocument` (240), `saveIngested` (252), `saveBlocking` (267), `saveFull` (299), `savePartial` (396) | Replace with one save handler (S1-S15). The full/partial split disappears: v2.5 has one composition rule. |
| | `combineManifests` (562) | Adapt its codec-order merge to the v2 manifest. |
| | `getDocument` (631), `getDocumentByReference` (646), `assemble` (668), `getDocumentManifest` (703) | Replace with the read handler (R1-R7). |
| | `requireReadBinding` (737), `requireReadAccount` (748), `requireRead` (754), `requireVisibleRead` (758), `canRead` (763), `canAccess` (777) | Keep; move into the new handlers. `storedSecurity` (767) switches from the JSON column to the revision's binary policy. |
| | `requireWrite` (785) | Adapt the creation rule (decisions 24-27); keep the policy-change restrictions. |
| | `requireLegacyWriteTarget` (623), `writeAttemptPrefix` (1052), `upsertRow` (1063) | Remove after §15. |
| | `deleteDocument` (813) and its helpers; `listDocuments` (988) | Replace in a later slice (tombstone accession per D3; catalog listing). |
| SaveResolution | `resolve` (53) with its intake and pipeline arms | Adapt (§8, gaps 1-3). |
| | `intakeGraphId` (136) | Keep. |
| | `saveResponse` (160) | Replace. |
| | `requireLegacyStoragePrefix` (175), `basePrefix` (186), `s3Metadata` (191), `manifestVersion` (202) | Remove after §15: stack keys and catalog revisions replace them. |
| DocumentSaveCandidate | `build` | Replace with catalog row construction. Its rule that only process authority takes security policy from the request body is replaced by decision 30. |
| DocumentRequests | `partsOrThrow` (26) | Adapt to selectors. |
| | `validateAddress` (42), `hasNotFoundCause` | Keep. |
| PartStorage | `writePartObjects` (116) | Adapt: streaming put, `StackRef`, server-computed SHA-256 and length. |
| | `copyParts` (216) | Remove from the save path: carry is by reference. |
| | `readParts` (250) | Replace with the verified read. |
| DocumentPartCodec | `split` (72), `assemble` (220), `sha256Hex` | Keep; the layout gains segmentations and embedding sets after D5. |
| | `objectKey` (237) | Replace with stack key naming. |
| | `rootChecksum` (269), `rootChecksumFromManifest` (286) | Remove after §15, unless D2 keeps content dedupe by root. |
| | `manifestToJson` (302), `manifestFromJson` (316) | Remove after §15: manifests become binary. |
| PartLayouts | `document` | Adapt for segmentations (CHUNKS by segmentation ID) and embedding sets (a two-part key) after D5. |
| DocumentPartReader | `readPart` (556) | Keep the size and SHA-256 checks; drop the provider version, ETag and content-type checks that belong to exact-version tracking; make it a stream. |
| DocumentLedger | `saveIfRevision` (91, 101), `saveGuarded` (136), `requireRevision` (211), `withLockedReference` (333, 339), `reserveLegacyPartKeys` (53), `hasPartPublication` (131), `findByNodeId` (223), `findByReference` (294), `list` (399-413) | Replace with the JDBC catalog. |
| | `lockRevisions` (162) | Its idea (advisory locks on sorted keys for rows that may not exist, then row locks in a fixed order) is reusable if a save ever locks several documents. Slice 1 needs one document plus the outcome unique constraint. |
| DocumentSchemaPolicies | `lockAccountWriter` (27) | Off the save path, unless the account schema policy survives as the mapping's supplement proposes (gap 10); then it belongs inside the commit transaction. Its SQL fence, the `a0_document_body_schema_policy` trigger, doesn't carry into the new baseline (§1.5). |
| RepositoryCredentialAuthorities | `requireLive` (62) | Adapt to JDBC. Keep the SQL function `lock_repository_credential_authority` (V99), which already reads with `FOR SHARE`: rotation and revocation are UPDATEs, so a commit holding the share lock finishes first, and one that arrives later sees the new state. Writer-grant revocation can copy this exactly (open item 1 in §3 of the 5f1954c2f review). |
| JdbcEventOutbox | `enqueue` (50) | Adapt from `EntityManager` to a JDBC connection; despite the name it is JPA today. `EventRelay` stays. |
| DocumentEventFactory | `saved` (48) | Replace with the v2 `DocumentEvent` builder (Accessioned, listing changed parts). |
| GrpcErrors, RepositoryErrors | status mapping | Adapt: attach detail messages per the draft's error table. |
| Tx, DocumentRecord, persistence.xml | Hibernate unit of work and entity | Replace for documents (D7). `Tx` also serves the archive, purge, the outbox and drives, so it stays until they move; the new catalog gets its own JDBC transaction boundary. |
| UploadHttpServer `upload` (222), RawIngestionOperations `ingest` (76) | | Adapt in the HTTP slice: same streaming put and digest check, then the v2 save. |
| ManagedRawBindings | `writing` (43), `admitting` (47), `copying` (53, 58), `verifyCopy` (82), `Plan.publish` (179) | Replace with catalog object references, keeping its rule (§5.4). |

Path (B) and history (none of it mounted in production):

| Class | Method (line) | Disposition |
|-------|---------------|-------------|
| DocumentSchemaAdmission | `checkMember` (435-450), `check` (291-330), `requireKnownDocument` (490-511), `VALIDATOR` (23-26) | Keep, with a new entry over the composed candidate (S11). The two `Any` locations `requireKnownDocument` allows become the starting point for v2.5's occurrence rules. |
| DocumentSchemaAssessment | `prepare` (50-150) | Keep: it is the typed validation loop. Its output becomes `RevisionValidation` (resolved occurrences, part schemas) instead of assessment evidence. |
| DocumentPayloadCheck | `assessContextualAssets` (115), `scan` (214) | Keep: it decodes and validates each `Any` payload, nested ones included. |
| DocumentAdmissionPolicy | `assess` (133-140) | Adapt: host policy for opaque mode (as the component mapping says), with the new entry. |
| DocumentAdmissionAuthorization | `lockAndAuthorize` (201-269), `authorizeHistory` (180) | Replace with one authorization step (S5, S8, S9). Keep its rules: creation needs a grant plus a credential binding (the shape of v2.5's `allow_create`), and a credential-bound writer passes `requireLive`. Its rule that policy and datasource changes need process authority, implemented here and in `requireWrite`, is replaced by decision 30. |
| RepositoryCreationGrants | `requireLive` (105), `authorizeObservation` (121), `install` (63), `revoke` (151) | Replace with `writer_grants`. A creation grant binds one exact publication command (`repository_execution_scopes`); a v2.5 writer grant doesn't. |
| DocumentPublicationCommit, DocumentCommitWriter | `commitInternal` (79-205); `prepare` (28-123), `write` (129-168) | Replace with the JDBC commit. Borrow the revision-row shape and the order (locks, authorization, revision rows, outcome, outbox, one transaction); drop the documents overwrite and the JSON body. |
| DocumentPartTransfer | `upload` (31-54) | Replace with the streaming put; drop the whole-object read-back and the version and ETag checks. |
| DocumentUploadCoordinator, DocumentOperationUploadAdmission | `execute` (205-310), `stage` (184-216) | Remove after §15 (attempt rows, leases, key reservations). |
| DocumentPublicationReplay | `observe` (58-172) | Replace with the outcome lookup (S6, S13, S14). |
| DocumentPublicationRuntime, DocumentPublicationSessions, DocumentPublicationRegistration, DocumentPublicationExecution | whole classes | Remove after §15. |
| DocumentHistoricalSchemas | `check` (28-66) | Keep the idea (re-check against retained artifacts only, never the registry) if v2.5 reads re-verify; its SQL row readers go with the old tables. |
| DocumentSchemaMaterialization | `read` (114-256) | Keep only if a v2 surface decodes `Any` payloads with retained schemas, such as a JSON edge. Binary gRPC reads return the bytes and don't need it. |
| DocumentHistoricalOperations, DocumentHistoricalReader, DocumentReadLedger | `captureHistorical` (81) and the rest | Remove after §15: exact-revision reads replace them, without pins. |
| DocumentPublicationLedger | `findForRead` (60-105) | Remove with the v1 reads (§2.1). |
| DocumentAttemptWriter, ManagedDocumentSave, DocumentPublicationBatch, DocumentSourceSnapshot; DocumentOperations' 10-argument constructor (217) | | Dead in production. Nothing in src/main constructs them, and production builds `DocumentOperations` with the managed reader and writer null (RepoServices.java:311-312). Remove with the v1 engine. |

### 5.2 New pieces

1. Request validation at the service entry (ProtoValidator over the v2 messages).
2. Canonical `SaveIntent` encoding and digest, with the cross-language fixtures §7 requires.
3. Owner resolution: SaveOwner rules, including process authority as intake-only (decision 27).
4. Writer grants: the table, administrator provisioning outside DocumentService, deterministic selection, and the
   commit-time recheck with a `FOR SHARE` read. Provisioning is new work too: nothing in src/main provisions
   today's creation grants or credential authorities (§1.5).
5. The JDBC catalog: `stacks`, `documents` (call number and current revision), `revisions`, `part_refs`,
   `objects`, `operation_outcomes`, `outbox`, `schema_artifacts`, `writer_grants`, with a fresh Flyway baseline
   that refuses an existing V1-V120 database. Path (B)'s revision tables are the nearest model (§1.5). The
   baseline must also re-create `lock_repository_credential_authority` (V99) for `requireLive`.
6. The stack interface: streaming put returning a `StackRef`, a ranged streaming get with verification, head,
   delete.
7. The composed-candidate builder: the expected revision's part references, plus writes, plus sources, plus
   carried parts.
8. `RevisionValidation` recording, and gRPC status details.

### 5.3 Removable machinery, by role

Today one save has three implementations (path A, a dead managed branch inside `DocumentOperations`, and path
B), two authorization stacks, two outbox writers, three read paths and two retry stories. v2.5 needs one of
each. Sizes are `wc -l` on main for the container ledger package (197 files, 28,721 lines), grouped by class
name and role and checked against each class's javadoc.

| Role | Today | In v2.5 | Keep from it | Ledger size |
|------|-------|---------|--------------|-------------|
| Publication journal, retry identity, receipts | (A) dedupe by root checksum. (B) operation and owner rows with tokens, generations and leases; success and rejection receipts; abandonments; an in-memory session map | `operation_outcomes` keyed by account and operation ID, with the intent digest. It records successes and terminal failures such as validation rejections, never outages (requirements §7) | The rule that the command digest leaves out the operation ID; recording rejections as terminal outcomes | 28 files, 5,449 lines |
| Leases, claims, handoff, recovery, reader pins | (B) only | None: Kristian decided on no leases, epochs, handoff or reader pins | Nothing | 32 files, 3,366 lines |
| Upload staging | (A) attempt prefixes and key reservations. (B) attempt rows, selections, the observation flusher, heartbeats, seeds, key reservations | Write-once streaming put; references recorded at commit; no orphan sweep in slice 1 | Parallel part I/O; hashing while streaming | 16 files, 2,411 lines |
| Validation records | (B) assessment slots, starts, rejection retention, runtime observers | `RevisionValidation` on the revision; a rejection is a recorded outcome, not a retained assessment | The admission engine (§5.1) | 28 files, 3,958 lines |
| Historical reuse, restore and retention | (B) historical execution, successor activation, capture coverage, retained reads, reuse admission, `DocumentReadLedger` | Carry by reference inside the catalog; exact-revision reads | The retained-only schema re-check, as an idea | 51 files, 7,708 lines |
| Commit and revision model | (A) one row overwritten, JSON manifest, part-publication rows; the dead managed branch. (B) revision tables plus the same row overwrite | Revision, part references, pointer CAS, outcome and outbox in one JDBC transaction | (B)'s revision-table shape and commit order | 11 files, 1,710 lines |
| Schema policy and retention | (B) policy catalog with an SQL fence (lock function, triggers); staged artifacts with claims and leases; per-revision retention rows | `schema_artifacts` referenced by revision; the policy depends on gap 10 | Artifacts by digest; per-revision evidence | 7 files, 859 lines |
| Write authorization | (A) `requireWrite`. (B) `lockAndAuthorize` four times per call; creation grants bound to one command | One authorization step plus writer grants | `DocumentAccessPolicy`; `requireLive` and the V99 function | 4 files, 626 lines (`RepositoryCredentialAuthorities` stays) |
| Exact-version tracking | Version IDs and ETags checked on read-back and read; the backend capability gate | None: no reliance on provider versioning | Size and SHA-256 checks | Inside the classes above |
| Outbox writers | (A) `JdbcEventOutbox.enqueue`. (B) `em.persist` in `DocumentCommitWriter` | One enqueue inside the commit | `EventRelay` and its batch claim | Lifecycle package |
| Reads | (A) unverified `PartStorage.readParts`. (B) verified byte[] `DocumentPartReader`. History with pins and schema re-checks | One verified streaming read by revision | `readPart`'s size and SHA-256 checks | Engine package |
| Harness | verification/repository-recovery (13 files, 2,303 lines) and the two client modules | Retire with (B) | Nothing | Outside the ledger |
| Hibernate and V1-V120 | `Tx`, `DocumentRecord`, persistence.xml; 120 migrations, 9,639 lines of SQL | A new catalog module with its own baseline | Everything, for now: the archive (frozen), drives, raw objects and purge still use them | Outside this count |

The first six groups are 166 of the ledger package's 197 files and about 24,600 of its 28,700 lines. All of it
becomes removable once v2 passes §15 and the v1 service retires. Nothing in production calls path (B), so
retiring it breaks no production caller; its users are verification/repository-recovery,
verification/repository-consumer and the two client modules published in the BOM.

**Where this trace differs from component-mapping.md §4** (the branch copy; an older draft copy
lists the same retirements). It agrees with the mapping's retire list for the
lease, handoff, pin, attempt, publication, historical and assessment-slot groups. It differs here:
- **`DocumentSchemaAssessment` and `DocumentPayloadCheck` are the validation engine.** The mapping retires the
  first (under "all `DocumentSchemaAssessment*`") and lists the second as "retire or fold". Together they do the
  typed `Any` validation S11 needs (§3), so keep both. The assessment-slot persistence in the ledger
  (`DocumentAssessment*`, `DocumentPublicationAssessment*`) is what retires.
- **`SaveResolution` changes.** The mapping says keep; gaps 1-3 change its intake and pipeline arms.
- **`ManagedBackendLedger` stays while the archive does.** The mapping retires it (verify); `ManagedArchiveServices`,
  `ArchiveObjectRecovery` and `RawObjectRecovery` use it, and the archive is frozen.
- **`RawObjectLedger` is on the HTTP upload path** (`RawIngestionOperations.ingest`), so it stays until the route moves.
- **`ManagedDocumentSave` has nothing to fold.** The mapping folds its role into the new save; it is dead code.
- **`Tx` can't be adapted in place.** The archive, purge, the outbox and drives share it, so the new catalog
  needs its own transaction boundary.
- **`RepositoryCredentialAuthorities` is missing from the mapping.** Decision 26 keeps `requireLive`, and the new
  baseline has to re-create its V99 lock function.

### 5.4 Behavior to keep while removing

- **Claim checks can't be guessed.** `ManagedRawBindings` lets a save keep its own raw-object references and add
  one only through a verified upload (raw ID plus lease token); naming storage coordinates acquires nothing
  (ManagedRawBindings.java:42-45). In v2.5 the server fills claim checks after the upload, so a client-supplied
  storage reference must be rejected or matched against objects the document already references.
- **The account, datasource and connector stay fixed** after creation. Today's rule that only process authority
  may change security policy (`requireWrite`, 798-810; `DocumentSaveCandidate.build`) is replaced by decision 30:
  any authorized writer may change security, and each change is a recorded revision.
- **Reads verify bytes** (`DocumentPartReader.readPart`); today's basic read doesn't.
- **Uploads hash while streaming** and check a declared digest (RawIngestionOperations.java:88-104).
- **Part I/O fans out in parallel** (PartStorage's virtual-thread pool).
- **Address rules:** the intake graph is `intake:<account>` and the datasource is the intake address
  (SaveResolution intake arm).

---

## 6. Consumers of SemanticProcessingResult (§16.2)

Source: a read-only sweep of Java, protos, SQL, Gradle, YAML/JSON, docs, the TypeScript/Vue console and
scripts. There is no Rust or Go in the repo. I spot-checked the load-bearing claims below against the source.

### 6.1 Who reads and writes `SearchMetadata.semantic_results` (field 13)

| Requirement §16.2 names | What the codebase has |
|---|---|
| Qdrant point mapper | **The only production reader.** QdrantPointMapper.java:125-171 reads `result_id`, each chunk's `chunk_id`, `chunk_number` and `text`, and each embedding's `vector` and `model` (one named vector per model). Point IDs are name-based UUIDs of (doc, `result_id`, `chunk_id`) (167-171), so collections must be re-indexed if identity moves to segmentation and embedding-set IDs. It also hard-codes the path `search_metadata.semantic_results` (81, 239-296). It never reads offsets, `chunker_config_id`, `embedding_config_id` or `ChunkEmbedding.dimensions`. |
| Chunker | No in-repo chunker writes field 13. search/chunk (`SentencePackedChunker`) builds the search service's own chunk lane from body text. |
| Embedders | Same. search/embedding (`MappingEmbedder`) embeds that chunk lane only. |
| Search indexers | No direct reference. The default repo mapping skips `semantic_results` by name (RepoDocumentMapping.java:107-110), and the system test copies that skip list (GoldenPathSystemTest.java:403-406). OpenSearch, Lucene, Solr and NDJSON follow the mapping. |
| Kafka Connect | Generic; no Document references (sink/kafka/connect and its OpenSearch and Iceberg connectors, the serde). MCP, REST, OpenAPI and JSON Schema surfaces are generic too. |

The only writer outside tests is the court sample (samples CourtDocuments.java:91-114). **The real CHUNKS writers
are pipeline modules outside this repo**; RepoServiceIT models one as a "chunker" hop (1204-1222). They need
their own inventory where they live; this one can't see them.

### 6.2 The coupling is mostly structural, inside the repo

These depend on the field name, the field path or part number 3, not on the Java type:
- the codec layout (PartLayouts.java:22-30), the rule that a CHUNKS parent may hold only `semantic_results`
  (DocumentFragmentConfinement.java:47-54), run naming (ChunkRunNames) and run validation
  (DocumentChunkSequence), all pinned to the policy `protomolt-document-parts/v1`;
- the publication slot CEL rule (document_publication.proto:133-137) and SQL CHECKs on part 3 (V21, V22, V52);
- the `schema_root` layout pattern (schema_root.proto:39);
- the admission `Any` inventory and its Document fingerprint pin.

Today's keying: one CHUNKS object per run of consecutive elements sharing a `result_id`; a blank ID becomes
`set-<element index>`; a repeated ID gets `#2`, `#3`. v2.5 segmentation IDs are unique per document, so the run
rules go away.

### 6.3 What D5 (new fields 16 and 17 on SearchMetadata) would run into

1. **Anything left out of the layout falls into CORE** (PartLayouts javadoc: "CORE is implicit"). Vectors would
   then ride in CORE, which the search fetcher (CORE + PARSED, GrpcDocumentFetcher.java:73-74) and the parse
   coordinator (CORE + BLOBS, and it rewrites CORE in its partial save) both load. The search mapping would infer
   the new repeated messages as NESTED unless they get skip entries.
2. **Storing them as CHUNKS and EMBEDDINGS needs a v2 layout policy**: layout, confinement, sequence check,
   `schema_root` pattern. `PartLayout` handles only a `parent.repeated` path with one key field
   (PartLayout.java:130-150), so EMBEDDINGS keyed by (`segmentation_id`, `embedding_set_id`) needs a composite key.
3. **The admission pins the generated Document's descriptor fingerprint.** `DocumentAnyRootInventory` rejects a
   container whose fingerprint differs from the compiled `Document` (DocumentAnyRootInventory.java:24-25, 51-53).
   Any change to document.proto makes revisions stored under the old schema fail that check, and the deployed
   stores already hold v1 CHUNKS revisions. v2.5's "decode with the schema retained at save time" needs
   validation against the retained container schema, not a pin to the compiled one.
4. **The new messages must not contain an `Any`**, or `DocumentAnyRootInventoryTest` (exact `Any` paths) and
   `requireKnownDocument` (DocumentSchemaAdmission.java:490-511) fail.
5. **Limits count `semantic_results` entries**: 100,000 in the service (ManagedDocumentServices.java:127),
   1,000,000 in admission (DocumentSchemaAdmission.java:452-482). Both need redefining.
6. **buf breaking**: deleting field 13 or the old messages needs `FIELD_NO_DELETE` / `MESSAGE_NO_DELETE` excepts
   in the same PR. Reserve 13 and the name. Field 12 is an unreserved gap today.

**Conclusion for D5.** Consumer-side cost is small: the Qdrant mapper, its tests, the court sample and two skip
lists. The real cost is inside the repo (layout policy, admission) and in the external pipeline modules. That
supports keeping the semantic shape out of slice 1, as §9 proposes.

---

## 7. Where the v2 files go under repo/proto, and what the build needs

Source: a read-only survey of the build files, buf.yaml, CI and the vendored protos. Nothing was built.

### 7.1 Where the files go

`repo/proto` is the Gradle project `:protomolt-repo-proto` (settings.gradle:362-363), published as
`ai.pipestream:protomolt-repo-proto`. buf's `PACKAGE_DIRECTORY_MATCH` rule is on (buf.yaml:116-119), so the
directory must match the package. With the draft's working package:

```
repo/proto/src/main/proto/ai/protomolt/proto/repo/v2/
  document_service.proto   DocumentService, its requests and responses, selectors, SaveIntent
  manifest.proto           PartState, PartManifestEntry, DocumentManifest, Accession, RevisionValidation and its parts
  writer_grant.proto       WriterGrant, WriterCredential, StepWriterScope, WriterGrantEvidence, ProcessAuthorityEvidence
  events.proto             DocumentEvent, Accessioned, Tombstoned, PurgeMarked, PurgeCompleted, PartChange
  errors.proto             RevisionConflict, OperationReuse, SaveRejection, FieldViolation, ItemError
```

Two groups should live elsewhere:
- **Semantic types** (`Segmentation`, `EmbeddingSet`, `Lineage` and their enums) need their own package that
  imports nothing from `repo.v1` or `repo.v2`, for example `ai/protomolt/proto/repo/semantic/v1/`. D5 has v1
  `document.proto` import them; if they sat in `repo.v2`, which imports v1, the two packages would import each
  other. buf has a `PACKAGE_NO_IMPORT_CYCLE` rule; I haven't checked whether buf 1.70.0 enforces it under
  STANDARD, but the cycle is worth avoiding either way.
- **Catalog-internal types** (`StackRecord`, `StackRef`, `StoredPartEntry`, `StoredManifest`) belong in the
  catalog module's own `src/main/proto`, not in the published contract jar, since stack references are internal
  (requirements §5).

File options, following v1: `syntax = "proto3"`, `java_multiple_files = true`, `java_package` equal to the
package. Set `java_outer_classname` explicitly (for example `DocumentServiceProto`); otherwise protoc derives
`DocumentServiceOuterClass` because the file declares `DocumentService`.

### 7.2 The validation dialect decides most of the build work

The draft annotates with `buf.validate`. Today that import fails in three places: Gradle protoc for
`:protomolt-repo-proto`, `buf lint`, and the `buf build` behind the PR breaking check
(scripts/check-proto-compatibility.sh). No first-party contract proto uses that dialect; every repo proto
uses the house dialect `ai.protomolt.proto.validate.v1`, and the branch's AGENTS.md lists `buf.validate` as a
named exception (AGENTS.md:226).

**Option A, the house dialect (recommended).** No Gradle or buf.yaml change. The house dialect has every rule
the draft uses (string `min_len`, `pattern`, `uuid`, even `sha256_hex`; int64 `gt`/`gte`; uint32 and bool
`const`; bytes `min_len`; enum `defined_only`/`in`/`not_in`; repeated `unique` with `items`; `required`; message
CEL). It has no oneof option, so a required oneof becomes message CEL, as v1 does
(schema_policy.proto:16-20). The draft also imports `schema_occurrence.proto` and
`schema_asset_reference.proto`, which already carry house rules, so option B would mix both dialects in one
import closure anyway. The translation is mechanical, but the fixture harness must be re-run with the house
rule source only, because presence rules inside oneofs may not behave identically in the two dialects.

**Option B, keep `buf.validate`.** It needs all of the following:
- `api project(':protomolt-protobuf-validation-protovalidate')` on repo-proto, because protoc needs the import
  and the generated code references `build.buf.validate.ValidateProto`. That dependency also registers
  `ProtovalidateRuleSource` through ServiceLoader in about 25 runtimes that depend on repo-proto, changing every
  `ProtoValidator.create()` and JSON Schema generator there;
- a buf module for `protobuf/validation-protovalidate/src/main/proto` with a top-level lint ignore for the
  vendored file (a module-level lint block breaks the breaking-check image, buf.yaml:67-72), or
  `deps: buf.build/bufbuild/protovalidate` with a buf.lock;
- validation of v2 messages only through validators that load both dialects. Five admission validators pin the
  house dialect and would silently skip `buf.validate` rules (`DocumentSchemaEvidenceCodec`,
  `DocumentSchemaAssetBinding`, `DocumentSchemaBinding`, `DocumentSchemaRootProjection`,
  `DocumentSchemaOccurrenceProjection`);
- a decision on AGENTS.md's dialect rule.

### 7.3 Other build facts either way

- **Package naming.** The branch's AGENTS.md:184 says packages are `ai.protomolt.proto.<domain>[.<sub>].v1`, and
  none is anything but v1. `repo.v2` would be the first, so D1 needs that rule decided. The in-convention alternative is
  a sub-package, as `repo.archive.v1` does.
- **Lint.** `COMMENTS` requires a leading comment on every message, field, enum value, oneof, service and RPC;
  the draft lacks many (for example `IntentMetadata`'s fields at line 340). `RPC_REQUEST_RESPONSE_UNIQUE` holds
  because v2 defines its own messages; the v1 waivers (buf.yaml:136-139, 151-154) name only the v1 file.
- **Breaking check.** New files are additive under the FILE rules. Changing v1 `document.proto` for D5 is
  additive too (new fields 16 and 17).
- **Downstream compiles.** search/proto, parse/proto and intake/proto feed repo-proto's jar to their own protoc
  through the `protobuf` configuration, which plugin 0.10.0 makes transitive and compiles. A v2 file would most
  likely be compiled there as well, so its imports must resolve in those modules. Confirm with a build.
- **Java names.** The generated `DocumentServiceGrpc` simple name repeats in both packages, and the v1
  implementation already owns `DocumentGrpcService`, so the v2 implementation class needs another name until v1
  goes.
- **Exposure.** Services are mounted from explicit lists (RepoServices.java:361-366, 612-623), so a v2 service
  stays unexposed until someone adds it. Nothing enumerates proto packages: descriptor users start from a v1
  root (`BuiltinDocumentSchema` from `Document`, `DocumentEventFactory.descriptorSetBase64` at line 151 from the
  v1 events). A v2 `DocumentEvent` therefore needs its own descriptor set for the Kafka serde.
- **Tests.** The 20 tests under repo/proto/src/test/java are per-message fixtures (validation of generated and
  dynamic messages, JSON Schema projection, selected wire facts). v2 contract tests fit the same pattern.
  Under option B they would also need protovalidate on that test classpath, which it lacks today.

---

## 8. Contract gaps the trace found

Each item names what the trace found, the options, and my recommendation. Items 1 and 2 block slice 1.
Items 9 and 10 shape it; the slice can follow the recommendations unless Kristian decides otherwise. The rest
can be settled when the affected caller moves.

1. **A blank `doc_id` gets a random UUID (blocks).** SaveResolution.java:59. Under v2.5 the call number is
   part of the SaveIntent, so a retry would mint a different `doc_id`, its intent would differ, and the client
   would get `OperationReuse` for a save that actually committed.
   - Options: reject a blank `doc_id`; or derive it from (account, operation_id).
   - **Recommend rejecting it.** Every producer in the codebase already sets one deterministically: the HTTP
     route hashes the content (RawIngestionOperations.java:110), intake hashes its input
     (intake RawIngests.java:74), Confluence hashes the entity ID (RepoChangeSink.java:136).
2. **A pipeline target has nowhere to put `graph_id` (blocks pipeline saves).** v1 requires `graph_id` next to
   `graph_location_id` (SaveResolution pipeline arm, 112-130). The draft's `DocumentTarget` arms are a full
   `NodeAddress`, `use_datasource_id` (where `graph_id` is derivable as `intake:<account>`) and
   `graph_location_id` alone.
   - Options: drop the `graph_location_id` arm and send pipeline saves as a full `NodeAddress`; or add
     `graph_id` to the target.
   - **Recommend dropping the arm.** The call number is the `NodeAddress`; one fewer derivation rule.
3. **`cluster_id` has no home in v2.5.** v1 stores it on pipeline rows (SaveResolution.java:126,
   DocumentSaveCandidate.java:28). Nothing reads `documents.cluster_id` outside the write paths.
   **Recommend dropping it.**
4. **`crawl_id` and persisted metadata.** v1 stores `crawl_id` (DocumentSaveCandidate.build), filters
   ListDocuments by it (ListDocumentsFilter) and echoes it (DocumentOperations.java:1024-1025). Producers:
   Confluence (RepoChangeSink.java:262, the change cursor) and the HTTP route (RawIngestionOperations.java:136).
   Nothing in the codebase filters on it. v1 also accepts `metadata` (field 99) but never stores it: there is
   no column and no read of it. v2.5 stores metadata and makes it retry identity (D10), yet no response in the
   draft returns it (`Accession` and `RevisionSummary` have no metadata field).
   - **Recommend:** carry `crawl_id` as a metadata key, drop the list filter, and add `metadata` to
     `Accession` so persisted metadata can be read back. The alternative is to state that it is write-only audit data.
5. **D8 has callers.** Four modules call the blob RPCs the draft removes:
   - transform/delegation `RepositoryServiceTranscriptRepository`: `GetBlobForUpdate` + `CompareAndPutBlob`
     (encrypted transcript state, compare-and-set);
   - mesh/cluster `RepositoryServiceClusterEventRepository`: `GetBlob` + `PutBlob` on one fixed key, rewritten
     in place (encrypted cluster event log);
   - acquire/confluence `RepoChangeSink`: `PutBlob` for attachments, then `SaveDocument` with the claim check;
   - intake `IntakeGrpcService` and parse `ParseCoordinatorGrpcService`: `GetBlob`.
   - Options for the two state stores: keep each state as a document and use `ExpectedRevision` as the
     compare-and-set; or keep a small state-blob service outside DocumentService.
   - **Recommend documents.** The compare-and-set maps directly, and it keeps one public service. This
     doesn't block slice 1, but it blocks retiring v1.
6. **The HTTP route needs an operation ID and an expected revision.** Today it has neither: it samples the
   current row and retries conflicts three times, so the last writer wins (§1.4). Since conflicts aren't
   recorded outcomes, the route can keep its retry loop with one operation ID and a fresh expected revision
   each time. Where the operation ID comes from (a client header, or derived from datasource, `doc_id` and
   content SHA-256) is a call for when the route is ported, not slice 1.
7. **D2 (content dedupe) is still open, and intake depends on it.** Today an identical intake re-save writes
   nothing and returns `deduplicated` (DocumentOperations.java:319-358). With no dedupe rule, every identical
   re-crawl becomes a new accession with new objects. **Recommend:** no dedupe in slice 1; decide D2 before the
   HTTP route or Confluence moves.
8. **ListDocuments loses its drive filter.** Search's `ReplayAction` lists by drive and account
   (ReplayAction.java:139-145). In v2.5 a drive only selects storage for new objects, so one document's parts can
   sit on several drives. ReplayAction would list by account and optional graph scope instead.
9. **Validation entries for carried parts.** §9 reads a carried part only when a rule needs its content and
   says earlier validation results aren't reused. Yet `RevisionValidation.resolved_occurrences` and
   `part_schemas` must name the schemas for every part so historical reads can decode them, and the draft
   doesn't say where a carried part's entries come from.
   - Options: copy them from the expected revision's record, since the bytes are identical by SHA-256; or
     re-read and re-resolve the carried part on every save.
   - **Recommend copying**, and stating that this carries retained schema identity, not a verdict. Re-reading
     would break §9's read rule on every partial save.
10. **Which policy `policy_sha256` names.** The draft makes `RevisionValidation.policy_sha256` required, and the
    component mapping adapts `DocumentSchemaPolicies` into the commit. But the requirements mention only an
    opaque host policy, and nothing in production can activate today's per-account schema policy (§1.5).
    - Options: keep a per-account schema policy in the new catalog, provisioned by an administrator like
      grants; or drop the field and keep only the host's opaque policy.
    - **Recommend deciding before the slice's commit code.** If the policy stays, slice 1 provisions one in test
      setup, as it does grants.

Also found, not a gap: `delete_source_blobs_on_settle` and `source_blob_delete_reason`, which v2.5 removes, are
stored and copied, but nothing acts on them. No lifecycle worker reads them.

---

## 9. A bounded first slice to approve

This is a proposal for plan step 3, for Kristian to approve or cut. Nothing here has started.

**Goal.** Prove the storage semantics end to end on one path (CAS, idempotent replay, carry by reference,
typed validation, one event per accession) without touching production wiring or deleting anything.

**In:**
- the v2 protos for `SaveDocument`, `GetOperationOutcome`, `GetDocument`, `GetManifest` and `ListRevisions`,
  compiled under repo/proto (§7);
- a JDBC catalog (HikariCP, Flyway) with the fresh baseline and its refuse guard: documents, revisions,
  part_refs, objects, operation_outcomes, outbox, writer_grants, schema_artifacts. I'd put it in a new module
  so Hibernate stays out of it;
- a stack interface with a streaming put that returns a `StackRef` and a verified get, backed by the existing
  S3 store;
- the save handler S1-S15 (§3) and the read handler R1-R7 (§4) over today's four parts (CORE, BLOBS, CHUNKS by
  `result_id`, PARSED), with typed validation through the admission engine (`DocumentSchemaAdmission`, `DocumentSchemaAssessment`,
  `DocumentPayloadCheck`) on written parts only (S11);
- writer grants and process-authority intake (decisions 25-27), provisioned by test setup, since there is no
  grant RPC in this slice (an account schema policy too, if gap 10 keeps it);
- tests on the existing docker compose infrastructure (Postgres, LocalStack), exercised through the engine and
  an in-process gRPC server. Nothing is registered in `RepoServices`, so no second public service is deployed (D1).

**Acceptance from §15:** 1 (on PARSED instead of a segmentation), 3, 4, 5, 7, 8, 9, 11, 12, 13, 17, 18
(missing owner, unknown owner, credential mismatch, revoked grant), 20 and 24.

**Out, for step 4 or later:**
- segmentations and embedding sets (2, 15), which wait on D5 and §6;
- opaque content (6, 19);
- 600 MB streaming and the HTTP route (10);
- retained-schema outage (14);
- purge (16, 21);
- grant snapshot digests and overlapping grants (22, 23);
- `SaveDocuments`, `UploadDocument`, `ReadDocument`, `ReadBlob`, delete, `ListDocuments`;
- any deletion of old code.

**Needed before it starts:** gaps 1 and 2 in §8; the package name (D1), or agreement to build under `repo.v2`
as a working name; the validation dialect (§7.2, where I recommend the house dialect); and the module placement
above. Gaps 9 and 10 follow my recommendations unless Kristian decides otherwise.
