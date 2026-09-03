package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.repo.v1.DeleteDocumentOutcome;
import ai.protomolt.proto.repo.v1.DeleteDocumentRequest;
import ai.protomolt.proto.repo.v1.DeleteDocumentResponse;
import ai.protomolt.proto.repo.v1.DeleteBlobRequest;
import ai.protomolt.proto.repo.v1.DeleteBlobResponse;
import ai.protomolt.proto.repo.v1.DeleteLogicalDocumentCommand;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.DocumentManifest;
import ai.protomolt.proto.repo.v1.DocumentMetadata;
import ai.protomolt.proto.repo.v1.DocumentPart;
import ai.protomolt.proto.repo.v1.DocumentServiceGrpc;
import ai.protomolt.proto.repo.v1.GetBlobRequest;
import ai.protomolt.proto.repo.v1.GetBlobResponse;
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
import ai.protomolt.proto.repo.container.blob.BlobStore;
import ai.protomolt.proto.repo.container.blob.DocumentIds;
import ai.protomolt.proto.repo.container.blob.PartStorage;
import ai.protomolt.proto.repo.container.codec.DocumentPartCodec;
import ai.protomolt.proto.repo.container.codec.PartLayout;
import ai.protomolt.proto.repo.container.codec.PartLayouts;
import ai.protomolt.proto.repo.container.codec.PartObject;
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
import io.grpc.stub.StreamObserver;
import jakarta.persistence.EntityManager;
import jakarta.persistence.LockModeType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.Set;
import java.util.UUID;

import static ai.protomolt.proto.repo.service.GrpcErrors.failedPrecondition;
import static ai.protomolt.proto.repo.service.GrpcErrors.invalidArgument;
import static ai.protomolt.proto.repo.service.GrpcErrors.notFound;
import static io.grpc.Status.UNAVAILABLE;

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
public final class DocumentGrpcService extends DocumentServiceGrpc.DocumentServiceImplBase {

    private static final Logger LOG = LoggerFactory.getLogger(DocumentGrpcService.class);

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
    private final BlobOperations blobs;

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
    public DocumentGrpcService(DocumentLedger documents, DriveLedger drives, Tx tx,
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
    public DocumentGrpcService(DocumentLedger documents, DriveLedger drives, Tx tx,
            BlobStore blobStore, PartStorage partStorage, PurgeQueue purgeQueue,
            JdbcEventOutbox events) {
        this.documents = documents;
        this.drives = drives;
        this.tx = tx;
        this.blobStore = blobStore;
        this.partStorage = partStorage;
        this.layout = PartLayouts.document();
        this.purgeQueue = purgeQueue;
        this.events = events;
        this.blobs = new BlobOperations(blobStore, tx);
    }

    // ------------------------------------------------------------------ save

    @Override
    public void saveDocument(SaveDocumentRequest request, StreamObserver<SaveDocumentResponse> observer) {
        GrpcErrors.run(observer, () -> saveBlocking(request));
    }

    /**
     * The blocking body of {@link #saveDocument}, package-private so the HTTP
     * upload route ({@code UploadHttpServer}) runs its assembled Document
     * through the SAME intake-save path as the gRPC SaveDocument — one dedupe,
     * one upsert, one revive semantic, no second implementation to drift.
     *
     * @param request the save request (validated exactly as on the wire)
     * @return the save response
     */
    SaveDocumentResponse saveBlocking(SaveDocumentRequest request) {
        SaveResolution.Resolved r = SaveResolution.resolve(request);
        DriveRecord drive = drives.findByName(r.address().getAccountId(), request.getDrive())
                .orElseThrow(() -> notFound("drive '" + request.getDrive() + "' not found for account '"
                        + r.address().getAccountId() + "'"));
        UUID nodeId = DocumentIds.nodeId(r.address());
        String basePrefix = SaveResolution.basePrefix(drive, r.address().getAccountId(), nodeId);

        if (request.getPartsWrittenList().isEmpty()) {
            return saveFull(r, request, drive, nodeId, basePrefix);
        }
        return savePartial(r, request, drive, nodeId, basePrefix);
    }

    /**
     * Full save: split → dedupe check under the row lock → (maybe) write all
     * parts → upsert the row. The split happens BEFORE the dedupe decision
     * because the Merkle root of the split is the dedupe key; identical
     * documents split into identical parts and therefore identical roots.
     */
    private SaveDocumentResponse saveFull(SaveResolution.Resolved r, SaveDocumentRequest request,
            DriveRecord drive, UUID nodeId, String basePrefix) {
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
        Decision decision = documents.withLockedReference(r.address(), existing -> {
            if (existing.isEmpty()) {
                return new Decision(false, null, 1L);
            }
            DocumentRecord row = existing.get();
            long nextVersion = SaveResolution.manifestVersion(row) + 1;
            if (intake && !request.getForceSave()
                    && DocumentStatus.AVAILABLE.equals(row.status)
                    && rootChecksum.equals(row.checksum)) {
                row.reprocessCount = row.reprocessCount + 1;
                row.lastReprocessedAt = Instant.now();
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

        // A re-saved PENDING_PURGE row needs no special case here: the upsert
        // below writes status AVAILABLE and bumps updated_at, which IS the
        // revive — the staleness guard then voids any purge queued against
        // the earlier body. force_save flips verifyChecksums on so the store
        // rejects a PUT whose landed bytes mismatch the part hash.
        PartStorage.WriteResult written = partStorage.writeParts(blobStore, drive.bucket, basePrefix,
                r.doc(), layout, r.address(),
                request.hasWrittenBy() ? request.getWrittenBy() : null,
                PART_CONTENT_TYPE, SaveResolution.s3Metadata(r), request.getForceSave(), decision.nextDocVersion());

        DocumentRecord row = upsertRow(r, request, drive, nodeId, basePrefix, written.manifest(),
                written.rootChecksum(), written.totalSizeBytes(), written.coreEtag(),
                written.coreVersionId(), decision.existing());
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
    private SaveDocumentResponse savePartial(SaveResolution.Resolved r, SaveDocumentRequest request,
            DriveRecord drive, UUID nodeId, String basePrefix) {
        Set<DocumentPart> partsWritten = DocumentRequests.partsOrThrow(request.getPartsWrittenList(), "parts_written");
        if (!request.hasCopyUnwrittenPartsFrom()) {
            throw invalidArgument("copy_unwritten_parts_from is required when parts_written is non-empty");
        }
        NodeAddress srcRef = DocumentRequests.validateAddress(request.getCopyUnwrittenPartsFrom(),
                "copy_unwritten_parts_from");

        // The copy source must be a live row with a manifest; a gone source is
        // FAILED_PRECONDITION (not NOT_FOUND) so the caller's
        // retry-as-full-save policy engages.
        DocumentRecord srcRow = documents.findByReference(srcRef)
                .orElseThrow(() -> failedPrecondition("partial-save copy source row not found: " + DocumentRequests.describe(srcRef)));
        if (!DocumentStatus.AVAILABLE.equals(srcRow.status)) {
            throw failedPrecondition("partial-save copy source row is " + srcRow.status
                    + " (need AVAILABLE): " + DocumentRequests.describe(srcRef));
        }
        DocumentManifest srcManifest = srcRow.readManifest();
        if (srcManifest == null) {
            throw failedPrecondition("partial-save copy source row has no manifest: " + DocumentRequests.describe(srcRef));
        }
        DriveRecord srcDrive = drives.findByName(srcRow.accountId, srcRow.driveName)
                .orElseThrow(() -> failedPrecondition("partial-save copy source drive '"
                        + srcRow.driveName + "' not found for account '" + srcRow.accountId + "'"));

        DocumentRecord destExisting = documents.findByNodeId(nodeId).orElse(null);
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

        PartStorage.WriteResult written = partStorage.writePartObjects(blobStore, drive.bucket, basePrefix,
                toWrite, r.address(),
                request.hasWrittenBy() ? request.getWrittenBy() : null,
                PART_CONTENT_TYPE, SaveResolution.s3Metadata(r), request.getForceSave(), docVersion);

        // Copy-forward: same BlobStore on both ends (one storage backend per
        // service), so this is always a server-side copy — the bytes never
        // transit this service. Carried entries keep their original
        // sha256/size/updated_at/written_by stamps; only the object key moves.
        // An in-place partial save (copy source == destination address, e.g.
        // the parsing coordinator re-staging PARSED+CORE onto the same row)
        // carries a part to the key it already lives at: nothing to copy, and
        // S3 rejects a metadata-unchanged self-copy outright.
        List<PartStorage.CopySpec> copies = new ArrayList<>(carried.size());
        List<PartManifestEntry> carriedAtDest = new ArrayList<>(carried.size());
        for (PartManifestEntry e : carried) {
            String destKey = DocumentPartCodec.objectKey(basePrefix, e.getPart(), e.getSubKey());
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
        String rootChecksum = DocumentPartCodec.rootChecksumFromManifest(combined);
        long totalSize = combined.getPartsList().stream()
                .filter(e -> e.getState() == PartState.PART_STATE_PRESENT)
                .mapToLong(PartManifestEntry::getSizeBytes).sum();

        // CORE is not always in a partial write; keep the row's representative
        // etag/version pointing at the live CORE object.
        String coreEtag = !written.coreEtag().isBlank() ? written.coreEtag()
                : (destExisting != null ? destExisting.etag : "");
        String coreVersionId = written.coreVersionId() != null ? written.coreVersionId()
                : (destExisting != null ? destExisting.versionId : null);

        DocumentRecord row = upsertRow(r, request, drive, nodeId, basePrefix, combined,
                rootChecksum, totalSize, coreEtag, coreVersionId, destExisting);
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
    private static DocumentManifest combineManifests(DocumentManifest written,
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

    // ------------------------------------------------------------------ reads

    @Override
    public void getDocument(GetDocumentRequest request, StreamObserver<GetDocumentResponse> observer) {
        GrpcErrors.run(observer, () -> {
            UUID nodeId = DocumentRequests.parseUuid(request.getNodeId(), "node_id");
            DocumentRecord row = documents.findByNodeId(nodeId)
                    .orElseThrow(() -> notFound("no document row for node_id " + nodeId));
            return assemble(row, DocumentRequests.partsOrThrow(request.getPartsList(), "parts"),
                    Set.copyOf(request.getChunkSetsList()));
        });
    }

    @Override
    public void getDocumentByReference(GetDocumentByReferenceRequest request,
            StreamObserver<GetDocumentResponse> observer) {
        GrpcErrors.run(observer, () -> {
            NodeAddress address = DocumentRequests.validateAddress(request.getAddress(), "address");
            DocumentRecord row = documents.findByReference(address)
                    .orElseThrow(() -> notFound("no document row for " + DocumentRequests.describe(address)));
            return assemble(row, DocumentRequests.partsOrThrow(request.getPartsList(), "parts"),
                    Set.copyOf(request.getChunkSetsList()));
        });
    }

    /**
     * Assembles the requested parts (empty mask = all) from the row's drive.
     * A manifest-PRESENT object gone from storage is FAILED_PRECONDITION
     * naming the missing parts — v1 deliberately does NOT silently reconcile
     * the manifest here; a {@code null} (transient) read is UNAVAILABLE so the
     * caller retries.
     */
    private GetDocumentResponse assemble(DocumentRecord row, Set<DocumentPart> parts, Set<String> chunkSets) {
        DocumentManifest manifest = row.readManifest();
        if (manifest == null) {
            throw failedPrecondition("document row " + row.nodeId + " carries no part manifest");
        }
        DriveRecord drive = drives.findByName(row.accountId, row.driveName)
                .orElseThrow(() -> notFound("drive '" + row.driveName + "' of document row "
                        + row.nodeId + " not found for account '" + row.accountId + "'"));
        Document assembled = partStorage.readParts(blobStore, drive.bucket, manifest, parts, chunkSets,
                Document.getDefaultInstance());
        if (assembled == null) {
            throw UNAVAILABLE.withDescription(
                    "transient part read failure for node_id " + row.nodeId + " — retry").asRuntimeException();
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
    public void getDocumentManifest(GetDocumentManifestRequest request,
            StreamObserver<GetDocumentManifestResponse> observer) {
        GrpcErrors.run(observer, () -> {
            DocumentRecord row = switch (request.getCoordinateCase()) {
                case NODE_ID -> {
                    UUID nodeId = DocumentRequests.parseUuid(request.getNodeId(), "node_id");
                    yield documents.findByNodeId(nodeId)
                            .orElseThrow(() -> notFound("no document row for node_id " + nodeId));
                }
                case ADDRESS -> {
                    NodeAddress address = DocumentRequests.validateAddress(request.getAddress(), "address");
                    yield documents.findByReference(address)
                            .orElseThrow(() -> notFound("no document row for " + DocumentRequests.describe(address)));
                }
                default -> throw invalidArgument(
                        "exactly one coordinate (node_id or address) must be set");
            };
            DocumentManifest manifest = row.readManifest();
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

    @Override
    public void deleteDocument(DeleteDocumentRequest request,
            StreamObserver<DeleteDocumentResponse> observer) {
        GrpcErrors.run(observer, () -> delete(request));
    }

    /**
     * Delete, idempotently. {@code purge_storage=false} tombstones each
     * matching row to PENDING_PURGE AND enqueues one purge record per row IN
     * THE SAME TRANSACTION (Phase A of the two-phase delete: metadata-only,
     * the snapshot of object keys captured at tombstone time, and the
     * tombstone deliberately does not bump {@code updated_at}) — the
     * background purger (Phase B) lands the actual object deletion.
     * {@code purge_storage=true} FIRST deletes each removed row's
     * manifest-PRESENT object keys from its drive's bucket (best-effort:
     * failures are logged, the row removal is still reported), then
     * hard-deletes the rows. Nothing matched → NOTHING_TO_REMOVE.
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

        List<DocumentRecord> removed;
        String detail;
        if (request.getPurgeStorage()) {
            for (DocumentRecord row : targets) {
                purgePartObjects(row);
            }
            // Re-delete through the ledger so the returned rows are the rows
            // actually removed (a concurrent delete settles to empty). When
            // eventing is on, the removal and the DocumentDeleted events
            // commit in ONE transaction instead (transactional outbox).
            if (events != null) {
                removed = hardDeleteAndEmit(byRef, logical);
            } else if (byRef != null) {
                removed = documents.deleteByReference(byRef)
                        .map(List::of)
                        .orElse(List.of());
            } else {
                removed = documents.deleteLogical(logical.getDocId(), logical.getAccountId(),
                        logical.getDatasourceId());
            }
            detail = "PURGED";
        } else {
            removed = new ArrayList<>(targets.size());
            for (DocumentRecord row : targets) {
                tombstoneAndEnqueue(row).ifPresent(removed::add);
            }
            detail = "TOMBSTONED";
        }

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

    /**
     * Phase A of the two-phase delete: tombstone the row to PENDING_PURGE and
     * enqueue its purge record IN ONE TRANSACTION, so the queue can never
     * drift from the tombstone (the sweeper covers only pre-lifecycle rows and
     * crashes outside this transaction). The purge record snapshots every
     * object key to delete (manifest PRESENT keys + the intake row's raw blob
     * key) so Phase B never recomputes. A re-delete of an already-tombstoned
     * row enqueues a fresh record — harmless: the drain is idempotent and
     * terminal queue transitions are conditional on PENDING.
     */
    private Optional<DocumentRecord> tombstoneAndEnqueue(DocumentRecord row) {
        // The raw-blob key derivation needs the drive prefix; unresolvable
        // drive → no raw key in the snapshot (the drain fails the record on
        // the missing drive anyway). Best-effort, outside the transaction.
        String drivePrefix = drives.findByName(row.accountId, row.driveName)
                .map(d -> d.prefix)
                .orElse(null);
        Instant requestedAt = Instant.now();
        // Cast disambiguates the Tx.inTransaction Function overload.
        return Optional.ofNullable(tx.inTransaction(
                (Function<EntityManager, DocumentRecord>) em -> {
                    DocumentRecord managed = em.find(DocumentRecord.class, row.nodeId,
                            LockModeType.PESSIMISTIC_WRITE);
                    if (managed == null) {
                        return null;
                    }
                    // Status-only transition: updated_at deliberately NOT
                    // bumped — the purger's staleness guard depends on it.
                    managed.status = DocumentStatus.PENDING_PURGE;
                    DocumentPurgeRecord record = new DocumentPurgeRecord();
                    record.purgeId = UUID.randomUUID();
                    record.nodeId = managed.nodeId;
                    record.docId = managed.docId;
                    record.graphAddressId = managed.graphAddressId;
                    record.accountId = managed.accountId;
                    record.graphId = managed.graphId;
                    record.driveName = managed.driveName;
                    record.writeObjectKeys(PurgeSnapshots.objectKeysOf(managed, drivePrefix));
                    record.requestedAt = requestedAt;
                    purgeQueue.enqueue(em, record);
                    if (events != null) {
                        // PurgeRequested commits with the tombstone and the
                        // purge record: one transaction, no drift.
                        events.enqueue(em, DocumentEventFactory.purgeRequested(record,
                                managed.checksum, requestedAt));
                    }
                    return managed;
                }));
    }

    /**
     * Hard-delete with eventing: re-find the target rows and remove them IN
     * ONE TRANSACTION with their DocumentDeleted events, so the event stream
     * cannot drift from the removal (transactional outbox). Mirrors the
     * ledger's deleteByReference/deleteLogical shapes - the events table is
     * the service's concern, not the ledger's.
     */
    private List<DocumentRecord> hardDeleteAndEmit(NodeAddress byRef,
            DeleteLogicalDocumentCommand logical) {
        Instant when = Instant.now();
        return tx.inTransaction(em -> {
            List<DocumentRecord> rows = new ArrayList<>(byRef != null
                    ? em.createQuery("SELECT d FROM DocumentRecord d WHERE d.docId = :docId"
                                    + " AND d.graphAddressId = :graphAddressId"
                                    + " AND d.accountId = :accountId AND d.graphId = :graphId",
                                    DocumentRecord.class)
                            .setParameter("docId", byRef.getDocId())
                            .setParameter("graphAddressId", byRef.getGraphAddressId())
                            .setParameter("accountId", byRef.getAccountId())
                            .setParameter("graphId", byRef.getGraphId())
                            .getResultList()
                    : em.createQuery("SELECT d FROM DocumentRecord d WHERE d.docId = :docId"
                                    + " AND d.accountId = :accountId"
                                    + " AND d.datasourceId = :datasourceId",
                                    DocumentRecord.class)
                            .setParameter("docId", logical.getDocId())
                            .setParameter("accountId", logical.getAccountId())
                            .setParameter("datasourceId", logical.getDatasourceId())
                            .getResultList());
            for (DocumentRecord managed : rows) {
                em.remove(managed);
                events.enqueue(em, DocumentEventFactory.deleted(managed, when));
            }
            return rows;
        });
    }

    /** Best-effort deletion of one row's manifest-PRESENT part objects. */
    private void purgePartObjects(DocumentRecord row) {
        DocumentManifest manifest = row.readManifest();
        if (manifest == null) {
            return;
        }
        List<String> keys = manifest.getPartsList().stream()
                .filter(e -> e.getState() == PartState.PART_STATE_PRESENT)
                .map(PartManifestEntry::getObjectKey)
                .filter(k -> k != null && !k.isBlank())
                .toList();
        if (keys.isEmpty()) {
            return;
        }
        try {
            Optional<DriveRecord> drive = drives.findByName(row.accountId, row.driveName);
            if (drive.isEmpty()) {
                LOG.warn("Purge of {} skipped: drive '{}' gone (account {})",
                        row.nodeId, row.driveName, row.accountId);
                return;
            }
            BlobStore.BatchDeleteResult result = blobStore.deleteAll(drive.get().bucket, keys);
            if (!result.allSucceeded()) {
                LOG.warn("Purge of {} left failed keys: {}", row.nodeId, result.failedKeys());
            }
        } catch (RuntimeException e) {
            // Best-effort: the row removal is still reported; a sweeper
            // reconciles orphaned objects later.
            LOG.warn("Purge of {} failed ({} objects): {}", row.nodeId, keys.size(), e.getMessage());
        }
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
    public void listDocuments(ListDocumentsRequest request,
            StreamObserver<ListDocumentsResponse> observer) {
        GrpcErrors.run(observer, () -> {
            int limit = request.getLimit() <= 0 ? DEFAULT_LIST_LIMIT
                    : Math.min(request.getLimit(), MAX_LIST_LIMIT);
            long offset = DocumentRequests.parseContinuationToken(request.getContinuationToken());
            ListDocumentsResult result = documents.list(new ListDocumentsFilter(
                    DocumentRequests.blankToNull(request.getDrive()),
                    DocumentRequests.blankToNull(request.getConnectorId()),
                    DocumentRequests.blankToNull(request.getCrawlId()),
                    DocumentRequests.blankToNull(request.getAccountId()),
                    limit, offset));

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

    @Override
    public void getBlob(GetBlobRequest request, StreamObserver<GetBlobResponse> observer) {
        GrpcErrors.run(observer, () -> blobs.get(request));
    }

    @Override
    public void putBlob(PutBlobRequest request, StreamObserver<PutBlobResponse> observer) {
        GrpcErrors.run(observer, () -> blobs.put(request));
    }

    @Override
    public void deleteBlob(DeleteBlobRequest request, StreamObserver<DeleteBlobResponse> observer) {
        GrpcErrors.run(observer, () -> blobs.delete(request));
    }

    // ------------------------------------------------------------------ plumbing

    /**
     * Insert-or-update the ledger row for a landed body. The body is already
     * in object storage by the time this runs, so the row goes straight to
     * AVAILABLE and {@code updated_at} moves — this IS the revive for a
     * re-saved tombstoned row. Bookkeeping columns (reprocess markers,
     * created_at) survive a rewrite from the existing row.
     */
    private DocumentRecord upsertRow(SaveResolution.Resolved r, SaveDocumentRequest request, DriveRecord drive,
            UUID nodeId, String basePrefix, DocumentManifest manifest, String rootChecksum,
            long totalSize, String coreEtag, String coreVersionId, DocumentRecord existing) {
        OwnershipContext ownership = r.doc().getOwnership();
        DocumentRecord row = new DocumentRecord();
        row.nodeId = nodeId;
        row.docId = r.address().getDocId();
        row.graphAddressId = r.address().getGraphAddressId();
        row.graphId = r.address().getGraphId();
        row.rowKind = r.rowKind();
        row.clusterId = r.clusterId();
        row.accountId = r.address().getAccountId();
        row.datasourceId = ownership.getDatasourceId();
        row.connectorId = !request.getConnectorId().isBlank() ? request.getConnectorId()
                : (ownership.hasConnectorId() ? ownership.getConnectorId() : null);
        row.checksum = rootChecksum;
        row.driveName = drive.name;
        row.objectKey = basePrefix;
        row.versionId = coreVersionId;
        row.etag = coreEtag != null ? coreEtag : "";
        row.sizeBytes = totalSize;
        row.contentType = PART_CONTENT_TYPE;
        row.filename = r.doc().hasSearchMetadata() && r.doc().getSearchMetadata().hasTitle()
                ? r.doc().getSearchMetadata().getTitle() : r.address().getDocId();
        row.writeManifest(manifest);
        row.writeSecurity(ownership.hasSecurity() ? ownership.getSecurity() : null);
        boolean intake = DocumentRowKind.INTAKE.equals(r.rowKind());
        row.deleteSourceBlobsOnSettle = intake && request.getDeleteSourceBlobsOnSettle();
        row.sourceBlobDeleteReason = intake && !request.getSourceBlobDeleteReason().isBlank()
                ? request.getSourceBlobDeleteReason() : null;
        row.status = DocumentStatus.AVAILABLE;
        row.crawlId = request.hasCrawlId() && !request.getCrawlId().isBlank()
                ? request.getCrawlId() : null;
        Instant now = Instant.now();
        if (existing != null) {
            row.createdAt = existing.createdAt;
            row.reprocessCount = existing.reprocessCount;
            row.lastReprocessedAt = existing.lastReprocessedAt;
        } else {
            row.createdAt = now;
        }
        // Body rewrite: the staleness guard moves. Deliberately explicit —
        // nothing else bumps updated_at (see DocumentRecord's class Javadoc).
        row.updatedAt = now;
        if (events == null) {
            return documents.save(row);
        }
        // The DocumentSaved event commits with the row upsert: the event
        // stream cannot drift from the ledger (transactional outbox).
        // Cast disambiguates the Tx.inTransaction Function overload.
        return tx.inTransaction((Function<EntityManager, DocumentRecord>) em -> {
                    DocumentRecord merged = em.merge(row);
                    events.enqueue(em, DocumentEventFactory.saved(merged, now));
                    return merged;
                });
    }

}
