package ai.protomolt.proto.repo.engine;

import ai.protomolt.proto.repo.spi.DocumentAccessPolicy;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryOperationControl;

import ai.protomolt.proto.repo.v1.DeleteDocumentOutcome;
import ai.protomolt.proto.repo.v1.DeleteDocumentRequest;
import ai.protomolt.proto.repo.v1.DeleteDocumentResponse;
import ai.protomolt.proto.repo.v1.DeleteBlobRequest;
import ai.protomolt.proto.repo.v1.DeleteBlobResponse;
import ai.protomolt.proto.repo.v1.CompareAndPutBlobRequest;
import ai.protomolt.proto.repo.v1.CompareAndPutBlobResponse;
import ai.protomolt.proto.repo.v1.DeleteLogicalDocumentCommand;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.DocumentManifest;
import ai.protomolt.proto.repo.v1.DocumentMetadata;
import ai.protomolt.proto.repo.v1.DocumentPart;
import ai.protomolt.proto.repo.v1.GetBlobRequest;
import ai.protomolt.proto.repo.v1.GetBlobResponse;
import ai.protomolt.proto.repo.v1.GetBlobForUpdateRequest;
import ai.protomolt.proto.repo.v1.GetBlobForUpdateResponse;
import ai.protomolt.proto.repo.v1.GetDocumentByReferenceRequest;
import ai.protomolt.proto.repo.v1.GetDocumentManifestRequest;
import ai.protomolt.proto.repo.v1.GetDocumentManifestResponse;
import ai.protomolt.proto.repo.v1.GetDocumentRequest;
import ai.protomolt.proto.repo.v1.GetDocumentResponse;
import ai.protomolt.proto.repo.v1.ListDocumentsRequest;
import ai.protomolt.proto.repo.v1.ListDocumentsResponse;
import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v1.OwnershipContext;
import ai.protomolt.proto.repo.v1.PartManifestEntry;
import ai.protomolt.proto.repo.v1.PartState;
import ai.protomolt.proto.repo.v1.PutBlobRequest;
import ai.protomolt.proto.repo.v1.PutBlobResponse;
import ai.protomolt.proto.repo.v1.RemovedDocumentNode;
import ai.protomolt.proto.repo.v1.SaveDocumentRequest;
import ai.protomolt.proto.repo.v1.SaveDocumentResponse;
import ai.protomolt.proto.repo.blob.spi.BlobStore;
import ai.protomolt.proto.repo.container.blob.DocumentIds;
import ai.protomolt.proto.repo.container.blob.PartStorage;
import ai.protomolt.proto.repo.codec.DocumentPartCodec;
import ai.protomolt.proto.repo.codec.PartLayout;
import ai.protomolt.proto.repo.codec.PartLayouts;
import ai.protomolt.proto.repo.codec.PartObject;
import ai.protomolt.proto.repo.container.ledger.DocumentLedger;
import ai.protomolt.proto.repo.container.ledger.DocumentRecord;
import ai.protomolt.proto.repo.container.ledger.DocumentRowKind;
import ai.protomolt.proto.repo.container.ledger.DocumentStatus;
import ai.protomolt.proto.repo.container.ledger.DriveLedger;
import ai.protomolt.proto.repo.container.ledger.DriveRecord;
import ai.protomolt.proto.repo.container.ledger.ListDocumentsFilter;
import ai.protomolt.proto.repo.container.ledger.ListDocumentsResult;
import ai.protomolt.proto.repo.container.ledger.Tx;
import ai.protomolt.proto.repo.container.ledger.DocumentPurgeRecord;
import ai.protomolt.proto.repo.container.lifecycle.DocumentEventFactory;
import ai.protomolt.proto.repo.container.lifecycle.JdbcEventOutbox;
import ai.protomolt.proto.repo.container.lifecycle.PurgeQueue;
import ai.protomolt.proto.repo.container.lifecycle.PurgeSnapshots;
import com.google.protobuf.Timestamp;
import jakarta.persistence.EntityManager;
import jakarta.persistence.LockModeType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.Set;
import java.util.UUID;

import static ai.protomolt.proto.repo.engine.RepositoryErrors.failedPrecondition;
import static ai.protomolt.proto.repo.engine.RepositoryErrors.invalidArgument;
import static ai.protomolt.proto.repo.engine.RepositoryErrors.notFound;


/**
 * The claim-check orchestration behind {@code DocumentService}: splits each
 * {@link Document} into its four addressable parts, writes the parts to the
 * resolved drive's bucket, and records the machine-readable map (manifest +
 * identity + lifecycle) as one ledger row. Payload bytes live only in object
 * storage; the database row is the claim check.
 *
 * <p><b>Storage identity.</b> Every row is addressed by the canonical
 * {@link NodeAddress} — the four segments
 * {@code doc_id | graph_address_id | account_id | graph_id}, hashed into a
 * deterministic {@code node_id} by {@link DocumentIds#nodeId}. The save
 * request's {@code graph_address} oneof arm is the EXPLICIT origin
 * discriminator — nothing is inferred from blank fields:
 * <ul>
 *   <li>{@code use_datasource_id}: an INTAKE save. The address is the
 *   document's {@code ownership.datasource_id}; {@code graph_id} must equal the
 *   account's intake graph {@code "intake:<accountId>"}; {@code cluster_id}
 *   must be absent.</li>
 *   <li>{@code graph_location_id}: a PIPELINE save at the named graph node;
 *   {@code graph_id} is the owning graph and is required.</li>
 * </ul>
 *
 * <p><b>Intake dedupe.</b> A re-crawl re-saves the same bytes at the same
 * intake address. When the locked row is AVAILABLE, its root checksum matches
 * the incoming split's root, and {@code force_save} is false, the object PUT
 * is skipped entirely: the row is marked re-processed (a bookkeeping update
 * that deliberately does NOT move {@code updated_at} — see the staleness
 * guard on {@link DocumentRecord}) and the existing coordinates come back
 * with {@code deduplicated=true}.
 *
 * <p><b>Revive.</b> Re-saving a row tombstoned to PENDING_PURGE is a body
 * rewrite, not an error: the upsert flips the status back to AVAILABLE and
 * bumps {@code updated_at}, so a purge queued against the earlier body is
 * voided by the staleness guard instead of deleting live bytes.
 *
 * <p><b>Partial save.</b> When {@code parts_written} is non-empty, only the
 * listed parts are written from the supplied document; every other PRESENT
 * part is carried forward from the {@code copy_unwritten_parts_from} row's
 * manifest via object-store server-side copy — the bytes never transit this
 * service, and the carried entries keep their original
 * sha256/size/updated_at/written_by stamps (only the object key changes). A
 * gone copy source is FAILED_PRECONDITION so the caller can retry as a full
 * save. The new root checksum is derived from the merged manifest via
 * {@link DocumentPartCodec#rootChecksumFromManifest} without reading the
 * carried bytes.
 *
 * <p>All methods are plain blocking code: handlers run on the server's
 * virtual-thread executor, so a blocked JDBC or S3 round trip parks the
 * virtual thread instead of a carrier.
 */
public final class DocumentOperations implements ai.protomolt.proto.repo.spi.DocumentRepository {

    private static final Logger LOG = LoggerFactory.getLogger(DocumentOperations.class);

    /** Content type stamped on every part object: parts are serialized protobuf fragments. */
    static final String PART_CONTENT_TYPE = "application/x-protobuf";
    /** Default page size for ListDocuments. */
    static final int DEFAULT_LIST_LIMIT = 100;
    /** Hard page-size cap for ListDocuments. */
    static final int MAX_LIST_LIMIT = 1000;

    private static final List<DocumentPart> CANONICAL_ORDER = List.of(
            DocumentPart.DOCUMENT_PART_CORE, DocumentPart.DOCUMENT_PART_BLOBS,
            DocumentPart.DOCUMENT_PART_CHUNKS, DocumentPart.DOCUMENT_PART_PARSED);

    private final DocumentLedger documents;
    private final DriveLedger drives;
    private final Tx tx;
    private final BlobStore blobStore;
    private final PartStorage partStorage;
    private final PartLayout layout;
    private final PurgeQueue purgeQueue;
    private final JdbcEventOutbox events;
    private final ManagedRawBindings rawBindings;
    private final DocumentPartReader managedParts;
    private final ai.protomolt.proto.repo.container.ledger.DocumentAttemptWriter managedWriter;
    private final String managedGeneration;

    /**
     * @param documents the document-row ledger
     * @param drives the drive-row ledger (drive name → bucket/prefix)
     * @param tx the shared transaction wrapper, used directly for the two
     *        ad-hoc reads the ledgers deliberately do not expose (logical-row
     *        enumeration, account-less drive lookup) and for the tombstone +
     *        purge-enqueue transaction
     * @param blobStore the object-storage port every part IO goes through
     * @param partStorage the part fan-out IO layer
     * @param purgeQueue the purge queue the tombstone path enqueues onto
     *        (Phase A of the two-phase delete)
     */
    public DocumentOperations(DocumentLedger documents, DriveLedger drives, Tx tx,
            BlobStore blobStore, PartStorage partStorage, PurgeQueue purgeQueue) {
        this(documents, drives, tx, blobStore, partStorage, purgeQueue, null);
    }

    /**
     * @param documents the document-row ledger
     * @param drives the drive-row ledger (drive name → bucket/prefix)
     * @param tx the shared transaction wrapper, used directly for the two
     *        ad-hoc reads the ledgers deliberately do not expose (logical-row
     *        enumeration, account-less drive lookup) and for the tombstone +
     *        purge-enqueue transaction
     * @param blobStore the object-storage port every part IO goes through
     * @param partStorage the part fan-out IO layer
     * @param purgeQueue the purge queue the tombstone path enqueues onto
     *        (Phase A of the two-phase delete)
     * @param events the document-event outbox the commit points write
     *        (DocumentSaved / DocumentDeleted / PurgeRequested) into, IN THE
     *        SAME TRANSACTION as the ledger mutation; null when Kafka is not
     *        configured - no outbox writes then, zero overhead
     */
    public DocumentOperations(DocumentLedger documents, DriveLedger drives, Tx tx,
            BlobStore blobStore, PartStorage partStorage, PurgeQueue purgeQueue,
            JdbcEventOutbox events) {
        this(documents, drives, tx, blobStore, partStorage, purgeQueue, events, null);
    }

    /**
     * The host may supply a qualified, non-secret identity for the selected physical
     * backend. Without it, unmanaged documents work as before but managed references
     * are rejected. This does not by itself enable managed ingestion or qualify TTLs.
     */
    public DocumentOperations(DocumentLedger documents, DriveLedger drives, Tx tx,
            BlobStore blobStore, PartStorage partStorage, PurgeQueue purgeQueue,
            JdbcEventOutbox events, String managedBackendIdentity) {
        this(documents, drives, tx, blobStore, partStorage, purgeQueue, events, managedBackendIdentity, null);
    }

    /** Original-backend reader is required for bound documents; this does not enable managed writes. */
    public DocumentOperations(DocumentLedger documents, DriveLedger drives, Tx tx,
            BlobStore blobStore, PartStorage partStorage, PurgeQueue purgeQueue,
            JdbcEventOutbox events, String managedBackendIdentity, DocumentPartReader managedParts) {
        this(documents, drives, tx, blobStore, partStorage, purgeQueue, events, managedBackendIdentity, managedParts, null);
    }

    /** Explicit library composition; host must qualify and drain the borrowed reader/writer. */
    public DocumentOperations(DocumentLedger documents, DriveLedger drives, Tx tx,
            BlobStore blobStore, PartStorage partStorage, PurgeQueue purgeQueue,
            JdbcEventOutbox events, String managedBackendIdentity, DocumentPartReader managedParts,
            ai.protomolt.proto.repo.container.ledger.DocumentAttemptWriter managedWriter) {
        if (managedWriter != null && (managedParts == null || managedBackendIdentity == null || managedBackendIdentity.isBlank()))
            throw new IllegalArgumentException("Managed writer requires a reader and retained generation");
        this.documents = documents;
        this.drives = drives;
        this.tx = tx;
        this.blobStore = blobStore;
        this.partStorage = partStorage;
        this.layout = PartLayouts.document();
        this.purgeQueue = purgeQueue;
        this.events = events;
        this.rawBindings = new ManagedRawBindings(documents, drives, managedBackendIdentity);
        this.managedParts = managedParts;
        this.managedWriter = managedWriter;
        this.managedGeneration = managedBackendIdentity;
    }

    // ------------------------------------------------------------------ save

    @Override
    public SaveDocumentResponse saveDocument(RepositoryCaller caller, SaveDocumentRequest request, RepositoryOperationControl control) {
        control.check();
        requireReadBinding(caller);
        return RepositoryErrors.call(() -> {
            return saveBlocking(caller, request, null, control);
        });
    }

    private record IngestionAdmission(UUID rawId, UUID token, Long expectedRevision,
            UUID driveId, ManagedRawBindings.DriveState driveState) {}

    /** Only the shared ingestion implementation may supply a freshly verified upload lease. */
    SaveDocumentResponse saveIngested(RepositoryCaller caller, SaveDocumentRequest request,
            UUID rawId, UUID token, Long expectedRevision, UUID driveId, ManagedRawBindings.DriveState driveState) {
        RepositoryErrors.requireProcessAuthority(caller);
        if (!request.getPartsWrittenList().isEmpty())
            throw invalidArgument("Raw ingestion publishes a full intake document");
        return RepositoryErrors.call(() -> saveBlocking(caller, request,
                new IngestionAdmission(rawId, token, expectedRevision, driveId, driveState), RepositoryOperationControl.NONE));
    }

    /**
     * The save implementation shared by library, gRPC and HTTP callers.
     *
     * @param request the save request (validated exactly as on the wire)
     * @return the save response
     */
    private SaveDocumentResponse saveBlocking(RepositoryCaller caller, SaveDocumentRequest request, IngestionAdmission admission,
            RepositoryOperationControl control) {
        control.check();
        SaveResolution.Resolved r = SaveResolution.resolve(request);
        requireReadAccount(caller, r.address().getAccountId());
        UUID nodeId = DocumentIds.nodeId(r.address());
        DocumentRecord destination = documents.findByNodeId(nodeId).orElse(null);
        if (admission != null && (admission.expectedRevision() == null ? destination != null
                : destination == null || destination.mutationRevision != admission.expectedRevision()))
            throw RepositoryErrors.revisionConflict();
        boolean writesCore = request.getPartsWrittenList().isEmpty()
                || request.getPartsWrittenList().contains(DocumentPart.DOCUMENT_PART_CORE);
        requireWrite(caller, destination, r.doc(), request, writesCore);
        SaveResolution.requireLegacyStoragePrefix(destination);
        if (managedWriter == null) requireLegacyWriteTarget(destination);
        DriveRecord drive = drives.findByName(r.address().getAccountId(), request.getDrive())
                .orElseThrow(() -> readMissing(caller, "drive '" + request.getDrive() + "' not found for account '"
                        + r.address().getAccountId() + "'"));
        String basePrefix = SaveResolution.basePrefix(drive, r.address().getAccountId(), nodeId);

        if (request.getPartsWrittenList().isEmpty()) {
            return saveFull(caller, r, request, drive, nodeId, basePrefix, destination, admission, control);
        }
        return savePartial(caller, r, request, drive, nodeId, basePrefix, destination, control);
    }

    /**
     * Full save: split → dedupe check under the row lock → (maybe) write all
     * parts → upsert the row. The split happens BEFORE the dedupe decision
     * because the Merkle root of the split is the dedupe key; identical
     * documents split into identical parts and therefore identical roots.
     */
    private SaveDocumentResponse saveFull(RepositoryCaller caller, SaveResolution.Resolved r, SaveDocumentRequest request,
            DriveRecord drive, UUID nodeId, String basePrefix, DocumentRecord destination, IngestionAdmission admission,
            RepositoryOperationControl control) {
        control.check();
        var bindings = admission == null
                ? rawBindings.writing(r.doc().getBlobBag(), r.address().getAccountId(), destination)
                : rawBindings.admitting(r.doc().getBlobBag(), r.address().getAccountId(), destination,
                        admission.rawId(), admission.token(), admission.driveId(), admission.driveState());
        List<PartObject> split = DocumentPartCodec.split(r.doc(), layout);
        String rootChecksum = DocumentPartCodec.rootChecksum(split);

        // Dedupe decision under the row lock (FOR UPDATE): the checksum
        // comparison and the reprocess bookkeeping must be one serialized unit
        // so two racing re-crawls of the same bytes don't both write. The row
        // returned by withLockedReference is the tx's MANAGED entity, so the
        // reprocess bump below is flushed on commit — and deliberately does
        // not touch updated_at (the body was not rewritten).
        record Decision(boolean deduplicated, DocumentRecord existing, long nextDocVersion) {
        }
        boolean intake = DocumentRowKind.INTAKE.equals(r.rowKind());
        Decision decision = documents.withLockedReference(r.address(), (em, existing) -> {
            control.check();
            // Authorization covered this exact revision. Check before dedupe,
            // which itself mutates bookkeeping even without writing object bytes.
            if (destination == null ? existing.isPresent()
                    : existing.isEmpty() || existing.get().mutationRevision != destination.mutationRevision)
                throw RepositoryErrors.revisionConflict();
            if (existing.isEmpty()) {
                return new Decision(false, null, 1L);
            }
            DocumentRecord row = existing.get();
            SaveResolution.requireLegacyStoragePrefix(row);
            long nextVersion = SaveResolution.manifestVersion(row) + 1;
            if (intake && !request.getForceSave()
                    && DocumentStatus.AVAILABLE.equals(row.status)
                    && rootChecksum.equals(row.checksum)) {
                bindings.publish(em, row);
                row.reprocessCount = row.reprocessCount + 1;
                row.lastReprocessedAt = Instant.now();
                control.check();
                return new Decision(true, row, nextVersion);
            }
            return new Decision(false, row, nextVersion);
        });

        if (decision.deduplicated()) {
            DocumentRecord row = decision.existing();
            LOG.debug("Dedupe hit for {} (node_id={}, reprocess_count={})",
                    r.address().getDocId(), nodeId, row.reprocessCount);
            return SaveDocumentResponse.newBuilder()
                    .setNodeId(row.nodeId.toString())
                    .setDrive(row.driveName)
                    .setStoragePrefix(row.objectKey)
                    .setSizeBytes(row.sizeBytes)
                    .setChecksum(row.checksum)
                    .setCreatedAtEpochMs(row.createdAt.toEpochMilli())
                    .setDeduplicated(true)
                    .setAddress(r.address())
                    .build();
        }

        if (managedWriter != null) {
            var row = ManagedDocumentSave.full(managedWriter, managedGeneration, caller, r, request, drive, nodeId,
                    basePrefix, split, decision.existing(), decision.nextDocVersion(), bindings, events, control);
            return SaveResolution.saveResponse(row, row.checksum);
        }

        // A re-saved PENDING_PURGE row needs no special case here: the upsert
        // below writes status AVAILABLE and bumps updated_at, which IS the
        // revive — the staleness guard then voids any purge queued against
        // the earlier body. force_save flips verifyChecksums on so the store
        // rejects a PUT whose landed bytes mismatch the part hash.
        String writePrefix = writeAttemptPrefix(basePrefix);
        control.check();
        BlobStore blobStore = new ControlledBlobStore(this.blobStore, control);
        documents.reserveLegacyPartKeys(split.stream()
                .map(part -> DocumentPartCodec.objectKey(writePrefix, part.part(), part.subKey())).toList());
        PartStorage.WriteResult written = partStorage.writePartObjects(blobStore, drive.bucket, writePrefix,
                split, r.address(),
                request.hasWrittenBy() ? request.getWrittenBy() : null,
                PART_CONTENT_TYPE, SaveResolution.s3Metadata(r), request.getForceSave(), decision.nextDocVersion());

        DocumentRecord row = upsertRow(caller, r, request, drive, nodeId, basePrefix, written.manifest(),
                written.rootChecksum(), written.totalSizeBytes(), written.coreEtag(),
                written.coreVersionId(), decision.existing(), Map.of(), bindings, control);
        LOG.debug("Saved {} at {} (node_id={}, version={}, bytes={})",
                r.address().getDocId(), r.address().getGraphAddressId(), nodeId,
                decision.nextDocVersion(), written.totalSizeBytes());
        return SaveResolution.saveResponse(row, written.rootChecksum());
    }

    /**
     * Partial save: write ONLY {@code parts_written} (and, within CHUNKS, only
     * {@code chunk_sets_written} when non-empty) from the supplied document;
     * carry every other PRESENT part forward from the copy source's manifest.
     * No dedupe on partial saves — they are pipeline restages, not re-crawls.
     */
    private SaveDocumentResponse savePartial(RepositoryCaller caller, SaveResolution.Resolved r, SaveDocumentRequest request,
            DriveRecord drive, UUID nodeId, String basePrefix, DocumentRecord destExisting, RepositoryOperationControl control) {
        control.check();
        BlobStore blobStore = new ControlledBlobStore(this.blobStore, control);
        Set<DocumentPart> partsWritten = DocumentRequests.partsOrThrow(request.getPartsWrittenList(), "parts_written");
        if (!request.hasCopyUnwrittenPartsFrom()) {
            throw invalidArgument("copy_unwritten_parts_from is required when parts_written is non-empty");
        }
        NodeAddress srcRef = DocumentRequests.validateAddress(request.getCopyUnwrittenPartsFrom(),
                "copy_unwritten_parts_from");
        requireReadAccount(caller, srcRef.getAccountId());
        if (!caller.processAuthority() && !srcRef.getAccountId().equals(r.address().getAccountId()))
            throw notFound("Document is unavailable");

        // The copy source must be a live row with a manifest; a gone source is
        // FAILED_PRECONDITION (not NOT_FOUND) so the caller's
        // retry-as-full-save policy engages.
        DocumentRecord srcRow = documents.findByReference(srcRef)
                .orElseThrow(() -> caller.processAuthority()
                        ? failedPrecondition("partial-save copy source row not found: " + DocumentRequests.describe(srcRef))
                        : notFound("Document is unavailable"));
        requireRead(caller, srcRow);
        if (!DocumentStatus.AVAILABLE.equals(srcRow.status)) {
            throw failedPrecondition("partial-save copy source row is " + srcRow.status
                    + " (need AVAILABLE): " + DocumentRequests.describe(srcRef));
        }
        if (managedWriter == null) requireLegacyWriteTarget(srcRow);
        DocumentManifest srcManifest = srcRow.readManifest();
        if (srcManifest == null) {
            throw failedPrecondition("partial-save copy source row has no manifest: " + DocumentRequests.describe(srcRef));
        }
        boolean boundSource = managedWriter != null && documents.hasPartPublication(srcRow.nodeId);
        DriveRecord srcDrive = boundSource ? null : drives.findByName(srcRow.accountId, srcRow.driveName)
                .orElseThrow(() -> failedPrecondition("partial-save copy source drive '"
                        + srcRow.driveName + "' not found for account '" + srcRow.accountId + "'"));

        var sourceSnapshot = managedWriter == null ? null : boundSource
                ? ai.protomolt.proto.repo.container.ledger.DocumentSourceSnapshot.bound(tx, srcRow)
                : ai.protomolt.proto.repo.container.ledger.DocumentSourceSnapshot.legacy(tx, drives, srcRow, srcDrive);
        if (sourceSnapshot != null) srcManifest = sourceSnapshot.manifest();

        long docVersion = SaveResolution.manifestVersion(destExisting) + 1;

        Set<String> chunkSetsWritten = Set.copyOf(request.getChunkSetsWrittenList());
        List<PartObject> toWrite = DocumentPartCodec.split(r.doc(), layout).stream()
                .filter(p -> partsWritten.contains(p.part()))
                .filter(p -> p.part() != DocumentPart.DOCUMENT_PART_CHUNKS
                        || chunkSetsWritten.isEmpty() || chunkSetsWritten.contains(p.subKey()))
                .toList();

        // Carried-forward entries: every source-PRESENT part this save does
        // NOT write. When only specific chunk sets are written, the sibling
        // chunk sets carry forward like any unwritten part.
        List<PartManifestEntry> carried = srcManifest.getPartsList().stream()
                .filter(e -> e.getState() == PartState.PART_STATE_PRESENT)
                .filter(e -> !partsWritten.contains(e.getPart())
                        || (e.getPart() == DocumentPart.DOCUMENT_PART_CHUNKS
                                && !chunkSetsWritten.isEmpty() && !chunkSetsWritten.contains(e.getSubKey())))
                .toList();
        for (PartManifestEntry e : carried) {
            if (e.getObjectKey().isBlank()) {
                // A lying source manifest would send a malformed copy request
                // to the store; treat it exactly like a gone source.
                throw failedPrecondition("partial-save copy source entry " + e.getPart()
                        + (e.getSubKey().isEmpty() ? "" : "/" + e.getSubKey())
                        + " is PRESENT but carries a blank object_key: " + DocumentRequests.describe(srcRef));
            }
        }

        if (managedWriter != null) {
            var readControl = new ai.protomolt.proto.repo.spi.RepositoryReadControl() {
                @Override public boolean isCancelled() { return control.isCancelled(); }
                @Override public long remainingNanos() { return control.remainingNanos(); }
            };
            var slots = carried.stream().map(part -> new DocumentPartReader.PartSlot(part.getPart(),part.getSubKey()))
                    .collect(java.util.stream.Collectors.toSet());
            try (var batch = managedParts.readSourceSlots(sourceSnapshot, this.blobStore, slots, readControl)) {
                var managedBindings = partsWritten.contains(DocumentPart.DOCUMENT_PART_BLOBS)
                        ? rawBindings.writing(r.doc().getBlobBag(), r.address().getAccountId(), destExisting)
                        : rawBindings.copying(batch, srcRow, r.address().getAccountId());
                var saved = ManagedDocumentSave.partial(managedWriter, managedGeneration, caller, r, request, drive,
                        nodeId, basePrefix, toWrite, batch.parts(), carried, destExisting, sourceSnapshot,
                        docVersion, managedBindings, events, control);
                return SaveResolution.saveResponse(saved, saved.checksum);
            }
        }

        var bindings = partsWritten.contains(DocumentPart.DOCUMENT_PART_BLOBS)
                ? rawBindings.writing(r.doc().getBlobBag(), r.address().getAccountId(), destExisting)
                : rawBindings.copying(blobStore, srcDrive, srcRow, r.address().getAccountId());
        String writePrefix = writeAttemptPrefix(basePrefix);
        var destinationKeys = new ArrayList<String>();
        for (var part : toWrite) destinationKeys.add(DocumentPartCodec.objectKey(writePrefix, part.part(), part.subKey()));
        for (var part : carried) destinationKeys.add(DocumentPartCodec.objectKey(writePrefix, part.getPart(), part.getSubKey()));
        documents.reserveLegacyPartKeys(destinationKeys);
        PartStorage.WriteResult written = partStorage.writePartObjects(blobStore, drive.bucket, writePrefix,
                toWrite, r.address(),
                request.hasWrittenBy() ? request.getWrittenBy() : null,
                PART_CONTENT_TYPE, SaveResolution.s3Metadata(r), request.getForceSave(), docVersion);

        // Copy-forward: same BlobStore on both ends (one storage backend per
        // service), so this is always a server-side copy — the bytes never
        // transit this service. Carried entries keep their original
        // sha256/size/updated_at/written_by stamps; only the object key moves.
        // Even in-place partial saves copy into this attempt's namespace, so
        // writes and copies cannot alter objects referenced by the old manifest.
        List<PartStorage.CopySpec> copies = new ArrayList<>(carried.size());
        List<PartManifestEntry> carriedAtDest = new ArrayList<>(carried.size());
        for (PartManifestEntry e : carried) {
            String destKey = DocumentPartCodec.objectKey(writePrefix, e.getPart(), e.getSubKey());
            if (!(srcDrive.bucket.equals(drive.bucket) && destKey.equals(e.getObjectKey()))) {
                copies.add(new PartStorage.CopySpec(e, destKey));
            }
            carriedAtDest.add(e.toBuilder().setObjectKey(destKey).build());
        }
        try {
            partStorage.copyParts(blobStore, srcDrive.bucket, blobStore, drive.bucket, true, copies);
        } catch (RuntimeException e) {
            if (DocumentRequests.hasNotFoundCause(e)) {
                throw failedPrecondition("partial-save copy source object already reclaimed: "
                        + e.getMessage());
            }
            throw e;
        }

        DocumentManifest combined = combineManifests(written.manifest(), carriedAtDest, srcManifest, docVersion);
        if (!partsWritten.contains(DocumentPart.DOCUMENT_PART_BLOBS))
            ManagedRawBindings.verifyCopy(blobStore, drive, combined);
        String rootChecksum = DocumentPartCodec.rootChecksumFromManifest(combined);
        long totalSize = combined.getPartsList().stream()
                .filter(e -> e.getState() == PartState.PART_STATE_PRESENT)
                .mapToLong(PartManifestEntry::getSizeBytes).sum();

        // A carried CORE has a new object identity. Resolve the destination's
        // metadata rather than retaining the source object's version ID.
        String coreEtag = written.coreEtag();
        String coreVersionId = written.coreVersionId();
        var carriedCore = carriedAtDest.stream()
                .filter(part -> part.getPart() == DocumentPart.DOCUMENT_PART_CORE).findFirst();
        if (carriedCore.isPresent()) {
            var core = carriedCore.get();
            var copied = blobStore.get(drive.bucket, core.getObjectKey());
            if (copied.data().length != core.getSizeBytes()
                    || !DocumentPartCodec.sha256Hex(copied.data()).equals(core.getSha256()))
                throw failedPrecondition("Copied CORE does not match the source manifest");
            coreEtag = copied.eTag() == null ? "" : copied.eTag();
            coreVersionId = copied.versionId();
        }

        DocumentRecord row = upsertRow(caller, r, request, drive, nodeId, basePrefix, combined,
                rootChecksum, totalSize, coreEtag, coreVersionId, destExisting,
                Map.of(srcRow.nodeId, srcRow.mutationRevision), bindings, control);
        LOG.debug("Partial save {} at {} (node_id={}, version={}, parts={}, copied={})",
                r.address().getDocId(), r.address().getGraphAddressId(), nodeId, docVersion,
                partsWritten, carried.size());
        return SaveResolution.saveResponse(row, rootChecksum);
    }

    /**
     * Merges written and carried entries back into canonical assembly order
     * (CORE, BLOBS, CHUNKS, PARSED) — a byte-fidelity requirement: assembly is
     * a field-level merge in manifest order, and CHUNKS sub-object ordering
     * carries the original repeated-field sequence. A replaced chunk set keeps
     * its ORIGINAL position (source order rules the zone); brand-new sets
     * append after it. Parts neither written nor carried are recorded EMPTY.
     */
    static DocumentManifest combineManifests(DocumentManifest written,
            List<PartManifestEntry> carried, DocumentManifest source, long docVersion) {
        List<PartManifestEntry> writtenPresent = written.getPartsList().stream()
                .filter(e -> e.getState() == PartState.PART_STATE_PRESENT)
                .toList();
        List<PartManifestEntry> ordered = new ArrayList<>();
        Timestamp now = DocumentRequests.timestampNow();
        for (DocumentPart part : CANONICAL_ORDER) {
            if (part == DocumentPart.DOCUMENT_PART_CHUNKS) {
                List<String> srcChunkOrder = source.getPartsList().stream()
                        .filter(e -> e.getPart() == part && e.getState() == PartState.PART_STATE_PRESENT)
                        .map(PartManifestEntry::getSubKey)
                        .toList();
                Set<String> placed = new HashSet<>();
                for (String subKey : srcChunkOrder) {
                    // A rewritten set wins its slot; otherwise the carried one holds it.
                    findEntry(writtenPresent, part, subKey)
                            .or(() -> findEntry(carried, part, subKey))
                            .ifPresent(entry -> {
                                ordered.add(entry);
                                placed.add(subKey);
                            });
                }
                for (PartManifestEntry e : writtenPresent) {
                    if (e.getPart() == part && !placed.contains(e.getSubKey())) {
                        ordered.add(e);
                    }
                }
                if (ordered.stream().noneMatch(e -> e.getPart() == part)) {
                    ordered.add(emptyEntry(part, now));
                }
            } else {
                // Same precedence, one expression: written, then carried, then EMPTY.
                ordered.add(findEntry(writtenPresent, part, null)
                        .or(() -> findEntry(carried, part, null))
                        .orElseGet(() -> emptyEntry(part, now)));
            }
        }
        return DocumentManifest.newBuilder()
                .setAddress(written.getAddress())
                .setDocVersion(docVersion)
                .addAllParts(ordered)
                .build();
    }

    private static Optional<PartManifestEntry> findEntry(List<PartManifestEntry> entries,
            DocumentPart part, String subKey) {
        return entries.stream()
                .filter(e -> e.getPart() == part && (subKey == null || e.getSubKey().equals(subKey)))
                .findFirst();
    }

    private static PartManifestEntry emptyEntry(DocumentPart part, Timestamp now) {
        return PartManifestEntry.newBuilder()
                .setPart(part)
                .setState(PartState.PART_STATE_EMPTY)
                .setUpdatedAt(now)
                .build();
    }

    /** Legacy PUT/COPY cannot replace or borrow a generation-bound publication. */
    private void requireLegacyWriteTarget(DocumentRecord row) {
        if (row != null && documents.hasPartPublication(row.nodeId))
            throw failedPrecondition("Managed document writes are not configured");
    }

    // ------------------------------------------------------------------ reads

    @Override
    public GetDocumentResponse getDocument(RepositoryCaller caller, GetDocumentRequest request,
            ai.protomolt.proto.repo.spi.RepositoryReadControl control) {
        control.check();
        requireReadBinding(caller);
        return RepositoryErrors.call(() -> {
            UUID nodeId = DocumentRequests.parseUuid(request.getNodeId(), "node_id");
            DocumentRecord row = documents.findByNodeId(nodeId)
                    .orElseThrow(() -> readMissing(caller, "no document row for node_id " + nodeId));
            requireVisibleRead(caller, row);
            return assemble(row, DocumentRequests.partsOrThrow(request.getPartsList(), "parts"),
                    Set.copyOf(request.getChunkSetsList()), control);
        });
    }

    @Override
    public GetDocumentResponse getDocumentByReference(RepositoryCaller caller, GetDocumentByReferenceRequest request,
            ai.protomolt.proto.repo.spi.RepositoryReadControl control) {
        control.check();
        requireReadBinding(caller);
        return RepositoryErrors.call(() -> {
            NodeAddress address = DocumentRequests.validateAddress(request.getAddress(), "address");
            requireReadAccount(caller, address.getAccountId());
            DocumentRecord row = documents.findByReference(address)
                    .orElseThrow(() -> readMissing(caller, "no document row for " + DocumentRequests.describe(address)));
            requireVisibleRead(caller, row);
            return assemble(row, DocumentRequests.partsOrThrow(request.getPartsList(), "parts"),
                    Set.copyOf(request.getChunkSetsList()), control);
        });
    }

    /**
     * Assembles the requested parts (empty mask = all) from the row's drive.
     * A manifest-PRESENT object gone from storage is FAILED_PRECONDITION
     * naming the missing parts — v1 deliberately does NOT silently reconcile
     * the manifest here; a {@code null} (transient) read is UNAVAILABLE so the
     * caller retries.
     */
    private GetDocumentResponse assemble(DocumentRecord row, Set<DocumentPart> parts, Set<String> chunkSets,
            ai.protomolt.proto.repo.spi.RepositoryReadControl control) {
        control.check();
        DocumentManifest manifest = row.readManifest();
        if (manifest == null) {
            throw failedPrecondition("document row " + row.nodeId + " carries no part manifest");
        }
        var publication = documents.partPublications().findForRead(row);
        Document assembled;
        if (publication.isPresent()) {
            if (managedParts == null) throw failedPrecondition("Managed document reader is not configured");
            assembled = managedParts.read(publication.orElseThrow(), parts, chunkSets, Document.getDefaultInstance(), control);
        } else {
            DriveRecord drive = drives.findByName(row.accountId, row.driveName)
                    .orElseThrow(() -> notFound("drive '" + row.driveName + "' of document row "
                            + row.nodeId + " not found for account '" + row.accountId + "'"));
            assembled = partStorage.readParts(blobStore, drive.bucket, manifest, parts, chunkSets,
                    Document.getDefaultInstance());
        }
        control.check();
        if (assembled == null) {
            throw RepositoryErrors.unavailable(
                    "transient part read failure for node_id " + row.nodeId + " — retry");
        }
        return GetDocumentResponse.newBuilder()
                .setDocument(assembled)
                .setNodeId(row.nodeId.toString())
                .setDrive(row.driveName)
                .setSizeBytes(assembled.getSerializedSize())
                .setRetrievedAtEpochMs(System.currentTimeMillis())
                .setManifest(manifest)
                .build();
    }

    @Override
    public GetDocumentManifestResponse getDocumentManifest(RepositoryCaller caller, GetDocumentManifestRequest request) {
        requireReadBinding(caller);
        return RepositoryErrors.call(() -> {
            DocumentRecord row = switch (request.getCoordinateCase()) {
                case NODE_ID -> {
                    UUID nodeId = DocumentRequests.parseUuid(request.getNodeId(), "node_id");
                    yield documents.findByNodeId(nodeId)
                            .orElseThrow(() -> readMissing(caller, "no document row for node_id " + nodeId));
                }
                case ADDRESS -> {
                    NodeAddress address = DocumentRequests.validateAddress(request.getAddress(), "address");
                    requireReadAccount(caller, address.getAccountId());
                    yield documents.findByReference(address)
                            .orElseThrow(() -> readMissing(caller, "no document row for " + DocumentRequests.describe(address)));
                }
                default -> throw invalidArgument(
                        "exactly one coordinate (node_id or address) must be set");
            };
            requireVisibleRead(caller, row);
            DocumentManifest manifest = documents.partPublications().findForRead(row)
                    .map(ai.protomolt.proto.repo.container.ledger.DocumentPublicationLedger.Publication::manifest)
                    .orElseGet(row::readManifest);
            if (manifest == null) {
                throw notFound("document row " + row.nodeId + " carries no part manifest");
            }
            return GetDocumentManifestResponse.newBuilder()
                    .setManifest(manifest)
                    .setDrive(row.driveName)
                    .build();
        });
    }

    // ------------------------------------------------------------------ delete

    private static void requireReadBinding(RepositoryCaller caller) {
        if (caller == null || (!caller.processAuthority() && caller.accountIds().isEmpty()))
            throw new ai.protomolt.proto.repo.spi.RepositoryException(
                    ai.protomolt.proto.repo.spi.RepositoryException.Code.PERMISSION_DENIED,
                    "Repository account bindings are required");
    }

    private static ai.protomolt.proto.repo.spi.RepositoryException readMissing(RepositoryCaller caller, String detail) {
        return notFound(caller.processAuthority() ? detail : "Document is unavailable");
    }

    private static void requireReadAccount(RepositoryCaller caller, String account) {
        if (!caller.processAuthority() && !caller.accountIds().contains(account))
            throw notFound("Document is unavailable");
    }

    /** The loaded row is the policy snapshot for this read; body ACLs never grant access. */
    private static void requireRead(RepositoryCaller caller, DocumentRecord row) {
        if (!canRead(caller, row)) throw notFound("Document is unavailable");
    }

    private static void requireVisibleRead(RepositoryCaller caller, DocumentRecord row) {
        requireRead(caller, row);
        if (!DocumentStatus.AVAILABLE.equals(row.status)) throw notFound("Document is unavailable");
    }

    private static boolean canRead(RepositoryCaller caller, DocumentRecord row) {
        return canAccess(caller, row, ai.protomolt.proto.repo.v1.Access.ACCESS_READ);
    }

    private static ai.protomolt.proto.repo.v1.DocumentSecurity storedSecurity(DocumentRecord row) {
        try {
            return row.readSecurity();
        } catch (ai.protomolt.proto.repo.container.ledger.LedgerException failure) {
            throw new ai.protomolt.proto.repo.spi.RepositoryException(
                    ai.protomolt.proto.repo.spi.RepositoryException.Code.FAILED_PRECONDITION,
                    "Stored document policy is malformed", failure);
        }
    }

    private static boolean canAccess(RepositoryCaller caller, DocumentRecord row, ai.protomolt.proto.repo.v1.Access access) {
        if (!caller.processAuthority() && !caller.accountIds().contains(row.accountId)) return false;
        // An operator does not need inherited grants; scoped callers fail closed
        // until the host supplies a resolved inherited-policy snapshot.
        return DocumentAccessPolicy.allows(caller, row.accountId, storedSecurity(row),
                caller.processAuthority() ? List.of() : null, access);
    }

    private static void requireWrite(RepositoryCaller caller, DocumentRecord current, Document candidate,
            SaveDocumentRequest request, boolean writesCore) {
        if (current == null && !caller.processAuthority()) throw notFound("Document is unavailable");
        if (current != null && (!canAccess(caller, current, ai.protomolt.proto.repo.v1.Access.ACCESS_WRITE)
                || (!caller.processAuthority() && !DocumentStatus.AVAILABLE.equals(current.status))))
            throw notFound("Document is unavailable");
        var ownership = candidate.getOwnership();
        var proposedPolicy = ownership.hasSecurity() ? ownership.getSecurity() : null;
        if (caller.processAuthority()) {
            DocumentAccessPolicy.allows(caller, ownership.getAccountId(), proposedPolicy,
                    List.of(), ai.protomolt.proto.repo.v1.Access.ACCESS_WRITE);
        } else {
            if (writesCore && !java.util.Objects.equals(storedSecurity(current), proposedPolicy))
                throw new ai.protomolt.proto.repo.spi.RepositoryException(
                        ai.protomolt.proto.repo.spi.RepositoryException.Code.PERMISSION_DENIED,
                        "Changing document access policy requires process authority");
            String reason = request.getSourceBlobDeleteReason().isBlank() ? null : request.getSourceBlobDeleteReason();
            if (!request.getDrive().equals(current.driveName)
                    || request.getDeleteSourceBlobsOnSettle() != current.deleteSourceBlobsOnSettle
                    || !java.util.Objects.equals(reason, current.sourceBlobDeleteReason)
                    || (writesCore && !java.util.Objects.equals(ownership.getDatasourceId(), current.datasourceId)))
                throw new ai.protomolt.proto.repo.spi.RepositoryException(
                        ai.protomolt.proto.repo.spi.RepositoryException.Code.PERMISSION_DENIED,
                        "Changing document storage, datasource or deletion policy requires process authority");
        }
    }

    @Override
    public DeleteDocumentResponse deleteDocument(RepositoryCaller caller, DeleteDocumentRequest request) {
        RepositoryErrors.requireProcessAuthority(caller);
        return RepositoryErrors.call(() -> {
            return delete(request);
        });
    }

    /**
     * Atomically admit the selected revisions into durable cleanup. Asynchronous
     * deletion returns after admission; synchronous deletion waits for its exact
     * records to finish, preserving manifest-only scope and DocumentDeleted events.
     * Storage failures propagate with the queue retained for recovery. New rows
     * matching a logical selector after sampling are not part of this command.
     */
    private DeleteDocumentResponse delete(DeleteDocumentRequest request) {
        List<DocumentRecord> targets;
        NodeAddress byRef = null;
        DeleteLogicalDocumentCommand logical = null;
        switch (request.getCommandCase()) {
            case BY_REFERENCE -> {
                byRef = DocumentRequests.validateAddress(request.getByReference().getAddress(),
                        "by_reference.address");
                targets = documents.findByReference(byRef)
                        .map(List::of)
                        .orElse(List.of());
            }
            case LOGICAL_DOCUMENT -> {
                logical = request.getLogicalDocument();
                if (logical.getDocId().isBlank() || logical.getAccountId().isBlank()
                        || logical.getDatasourceId().isBlank()) {
                    throw invalidArgument("logical_document requires doc_id, account_id and datasource_id");
                }
                targets = findLogicalRows(logical.getDocId(), logical.getAccountId(),
                        logical.getDatasourceId());
            }
            default -> throw invalidArgument(
                    "exactly one command (logical_document or by_reference) must be set");
        }

        if (targets.isEmpty()) {
            return DeleteDocumentResponse.newBuilder()
                    .setOutcome(DeleteDocumentOutcome.DELETE_DOCUMENT_OUTCOME_NOTHING_TO_REMOVE)
                    .setDocumentsRemoved(0)
                    .setMessage("no matching document rows")
                    .build();
        }

        List<StagedDelete> staged = stageDeletes(targets, request.getPurgeStorage());
        String detail = request.getPurgeStorage() ? "PURGED" : "TOMBSTONED";
        if (request.getPurgeStorage()) {
            // The fleet's Kafka queue owns consumer state on its own thread.
            // Request threads settle exact IDs through a separate JDBC handle.
            var purger = new ai.protomolt.proto.repo.container.lifecycle.S3Purger(tx, documents, drives,
                    new ai.protomolt.proto.repo.container.lifecycle.JdbcPurgeQueue(tx), events);
            for (StagedDelete item : staged) {
                String status = purger.purgeNow(blobStore, item.purge().purgeId);
                if (DocumentPurgeRecord.STATUS_VOID.equals(status))
                    throw RepositoryErrors.aborted("Document changed after deletion was admitted");
                if (!DocumentPurgeRecord.STATUS_PURGED.equals(status))
                    throw RepositoryErrors.unavailable("Document purge remains " + status + "; durable recovery is pending");
            }
        }
        List<DocumentRecord> removed = staged.stream().map(StagedDelete::row).toList();

        if (removed.isEmpty()) {
            return DeleteDocumentResponse.newBuilder()
                    .setOutcome(DeleteDocumentOutcome.DELETE_DOCUMENT_OUTCOME_NOTHING_TO_REMOVE)
                    .setDocumentsRemoved(0)
                    .setMessage("no matching document rows")
                    .build();
        }
        DeleteDocumentResponse.Builder response = DeleteDocumentResponse.newBuilder()
                .setOutcome(DeleteDocumentOutcome.DELETE_DOCUMENT_OUTCOME_REMOVED)
                .setDocumentsRemoved(removed.size())
                .setMessage(detail.equals("PURGED")
                        ? "rows removed and storage objects purged"
                        : "rows tombstoned to PENDING_PURGE");
        if (!request.getOmitRemovedNodes()) {
            for (DocumentRecord row : removed) {
                response.addRemovedNodes(RemovedDocumentNode.newBuilder()
                        .setNodeId(row.nodeId.toString())
                        .setDetail(detail));
            }
        }
        return response.build();
    }

    private record StagedDelete(DocumentRecord row, DocumentPurgeRecord purge) {}

    /** Admit exactly the sampled rows atomically; no object I/O occurs under these locks. */
    private List<StagedDelete> stageDeletes(List<DocumentRecord> targets, boolean synchronous) {
        Map<UUID, String> prefixes = new java.util.HashMap<>();
        for (DocumentRecord row : targets) {
            DriveRecord drive = drives.findByName(row.accountId, row.driveName)
                    .orElseThrow(() -> failedPrecondition("Document drive is unavailable for deletion"));
            prefixes.put(row.nodeId, drive.prefix);
        }
        return tx.inTransaction(em -> {
            List<DocumentRecord> locked = new ArrayList<>();
            for (DocumentRecord sampled : targets.stream().sorted(java.util.Comparator.comparing(row -> row.nodeId)).toList()) {
                DocumentRecord current = em.find(DocumentRecord.class, sampled.nodeId, LockModeType.PESSIMISTIC_WRITE);
                if (current == null || current.mutationRevision != sampled.mutationRevision)
                    throw RepositoryErrors.aborted("Document changed before deletion was admitted");
                // Both synchronous and queued legacy cleanup lack retained-version reclamation.
                // Reject the whole selection before admitting any tombstones or queue entries.
                if (!em.createNativeQuery("""
                        SELECT 1 FROM document_part_publications WHERE node_id=:node
                        UNION ALL
                        SELECT 1 FROM document_part_publication_history WHERE node_id=:node
                        """).setParameter("node", current.nodeId).setMaxResults(1).getResultList().isEmpty())
                    throw failedPrecondition("Managed document deletion requires retention-aware reclamation");
                if (current.pendingPurgeId == null && !DocumentStatus.AVAILABLE.equals(current.status)) {
                    long legacyPending = em.createQuery("SELECT count(p) FROM DocumentPurgeRecord p WHERE p.nodeId = :node"
                                    + " AND p.generationId IS NULL AND p.status = :pending", Long.class)
                            .setParameter("node", current.nodeId).setParameter("pending", DocumentPurgeRecord.STATUS_PENDING).getSingleResult();
                    if (legacyPending > 0)
                        throw failedPrecondition("Legacy purge admission must settle before another delete is admitted");
                }
                locked.add(current);
            }
            List<StagedDelete> staged = new ArrayList<>();
            Instant requestedAt = Instant.now();
            for (DocumentRecord current : locked) {
                DocumentPurgeRecord record = new DocumentPurgeRecord();
                record.purgeId = UUID.randomUUID();
                record.nodeId = current.nodeId;
                record.docId = current.docId;
                record.graphAddressId = current.graphAddressId;
                record.accountId = current.accountId;
                record.graphId = current.graphId;
                record.driveName = current.driveName;
                record.contentChecksum = current.checksum;
                record.completionMode = synchronous ? DocumentPurgeRecord.MODE_SYNCHRONOUS : DocumentPurgeRecord.MODE_ASYNC;
                // Repeated delete commands for the same tombstone keep its
                // generation, so an earlier admitted async raw cleanup is not
                // cancelled merely by a later synchronous part cleanup.
                record.generationId = current.pendingPurgeId != null
                        && (DocumentStatus.PENDING_PURGE.equals(current.status) || DocumentStatus.PURGE_FAILED.equals(current.status))
                        ? current.pendingPurgeId : record.purgeId;
                // Synchronous deletion has always covered manifest parts only.
                // Passing no prefix deliberately excludes the intake raw blob.
                record.writeObjectKeys(PurgeSnapshots.objectKeysOf(current, synchronous ? null : prefixes.get(current.nodeId)));
                record.requestedAt = requestedAt;
                current.status = DocumentStatus.PENDING_PURGE;
                current.pendingPurgeId = record.generationId;
                purgeQueue.enqueue(em, record);
                if (!synchronous && events != null)
                    events.enqueue(em, DocumentEventFactory.purgeRequested(record, current.checksum, requestedAt));
                staged.add(new StagedDelete(current, record));
            }
            return staged;
        });
    }

    /**
     * All rows of one logical document ({@code doc_id + account_id +
     * datasource_id}) across every storage address and graph. The ledger
     * deliberately exposes only the DELETE half of this shape (the purge path);
     * the tombstone path needs the rows first, so it reads them here through
     * the shared {@link Tx} — the sanctioned one-EntityManager-per-call path.
     */
    private List<DocumentRecord> findLogicalRows(String docId, String accountId, String datasourceId) {
        return tx.readOnly(em -> em.createQuery(
                        "SELECT d FROM DocumentRecord d WHERE d.docId = :docId"
                                + " AND d.accountId = :accountId AND d.datasourceId = :datasourceId",
                        DocumentRecord.class)
                .setParameter("docId", docId)
                .setParameter("accountId", accountId)
                .setParameter("datasourceId", datasourceId)
                .getResultList());
    }

    // ------------------------------------------------------------------ list

    @Override
    public ListDocumentsResponse listDocuments(RepositoryCaller caller, ListDocumentsRequest request) {
        requireReadBinding(caller);
        return RepositoryErrors.call(() -> {
            int limit = request.getLimit() <= 0 ? DEFAULT_LIST_LIMIT
                    : Math.min(request.getLimit(), MAX_LIST_LIMIT);
            long offset = DocumentRequests.parseContinuationToken(request.getContinuationToken());
            ListDocumentsFilter filter = new ListDocumentsFilter(
                    DocumentRequests.blankToNull(request.getDrive()),
                    DocumentRequests.blankToNull(request.getConnectorId()),
                    DocumentRequests.blankToNull(request.getCrawlId()),
                    DocumentRequests.blankToNull(request.getAccountId()),
                    limit, offset);
            ListDocumentsResult result = caller.processAuthority() ? documents.list(filter)
                    : documents.listVisible(filter, caller.accountIds(), row -> canRead(caller, row));

            if (result.totalCount() > Integer.MAX_VALUE)
                throw new ai.protomolt.proto.repo.spi.RepositoryException(
                        ai.protomolt.proto.repo.spi.RepositoryException.Code.RESOURCE_EXHAUSTED,
                        "Visible document count exceeds the response contract");

            ListDocumentsResponse.Builder response = ListDocumentsResponse.newBuilder()
                    .setTotalCount((int) result.totalCount());
            for (DocumentRecord row : result.rows()) {
                DocumentMetadata.Builder meta = DocumentMetadata.newBuilder()
                        .setNodeId(row.nodeId.toString())
                        .setDocId(row.docId)
                        .setDrive(row.driveName)
                        .setSizeBytes(row.sizeBytes)
                        .setCreatedAtEpochMs(row.createdAt.toEpochMilli())
                        .setAddress(SaveResolution.addressOf(row));
                if (row.connectorId != null) {
                    meta.setConnectorId(row.connectorId);
                }
                if (row.filename != null) {
                    meta.setTitle(row.filename);
                }
                if (row.crawlId != null) {
                    meta.setCrawlId(row.crawlId);
                }
                response.addDocuments(meta);
            }
            long nextOffset = offset + result.rows().size();
            if (nextOffset < result.totalCount()) {
                response.setNextContinuationToken(String.valueOf(nextOffset));
            }
            return response.build();
        });
    }


    // ------------------------------------------------------------------ blob











    // ------------------------------------------------------------------ plumbing

    private static String writeAttemptPrefix(String documentPrefix) {
        return documentPrefix + "/attempts/" + UUID.randomUUID();
    }

    /**
     * Insert-or-update the ledger row for a landed body. The body is already
     * in object storage by the time this runs, so the row goes straight to
     * AVAILABLE and {@code updated_at} moves — this IS the revive for a
     * re-saved tombstoned row. Bookkeeping columns (reprocess markers,
     * created_at) survive a rewrite from the existing row.
     */
    private DocumentRecord upsertRow(RepositoryCaller caller, SaveResolution.Resolved r, SaveDocumentRequest request, DriveRecord drive,
            UUID nodeId, String basePrefix, DocumentManifest manifest, String rootChecksum,
            long totalSize, String coreEtag, String coreVersionId, DocumentRecord existing,
            Map<UUID, Long> sourceRevisions, ManagedRawBindings.Plan bindings, RepositoryOperationControl control) {
        control.check();
        var row = DocumentSaveCandidate.build(caller, r, request, drive, nodeId, basePrefix, manifest, rootChecksum,
                totalSize, coreEtag, coreVersionId, existing);
        Instant now = row.updatedAt;
        try {
            return documents.saveIfRevision(row, existing == null ? null : existing.mutationRevision, sourceRevisions,
                    (em, committed) -> {
                        control.check();
                        bindings.publish(em, committed);
                        if (events != null) events.enqueue(em, DocumentEventFactory.saved(committed, now));
                        control.check();
                    });
        } catch (DocumentLedger.RevisionConflictException conflict) {
            throw new ai.protomolt.proto.repo.spi.RepositoryException(
                    ai.protomolt.proto.repo.spi.RepositoryException.Code.CONFLICT, conflict.getMessage(), conflict);
        }
    }

}
