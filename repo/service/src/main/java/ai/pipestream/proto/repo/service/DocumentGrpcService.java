package ai.pipestream.proto.repo.service;

import ai.pipestream.proto.repo.v1.DeleteDocumentByReferenceCommand;
import ai.pipestream.proto.repo.v1.DeleteDocumentOutcome;
import ai.pipestream.proto.repo.v1.DeleteDocumentRequest;
import ai.pipestream.proto.repo.v1.DeleteDocumentResponse;
import ai.pipestream.proto.repo.v1.DeleteBlobRequest;
import ai.pipestream.proto.repo.v1.DeleteBlobResponse;
import ai.pipestream.proto.repo.v1.DeleteLogicalDocumentCommand;
import ai.pipestream.proto.repo.v1.Document;
import ai.pipestream.proto.repo.v1.DocumentManifest;
import ai.pipestream.proto.repo.v1.DocumentMetadata;
import ai.pipestream.proto.repo.v1.DocumentPart;
import ai.pipestream.proto.repo.v1.DocumentServiceGrpc;
import ai.pipestream.proto.repo.v1.FileStorageReference;
import ai.pipestream.proto.repo.v1.GetBlobRequest;
import ai.pipestream.proto.repo.v1.GetBlobResponse;
import ai.pipestream.proto.repo.v1.GetDocumentByReferenceRequest;
import ai.pipestream.proto.repo.v1.GetDocumentManifestRequest;
import ai.pipestream.proto.repo.v1.GetDocumentManifestResponse;
import ai.pipestream.proto.repo.v1.GetDocumentRequest;
import ai.pipestream.proto.repo.v1.GetDocumentResponse;
import ai.pipestream.proto.repo.v1.ListDocumentsRequest;
import ai.pipestream.proto.repo.v1.ListDocumentsResponse;
import ai.pipestream.proto.repo.v1.NodeAddress;
import ai.pipestream.proto.repo.v1.OwnershipContext;
import ai.pipestream.proto.repo.v1.PartManifestEntry;
import ai.pipestream.proto.repo.v1.PartState;
import ai.pipestream.proto.repo.v1.PutBlobRequest;
import ai.pipestream.proto.repo.v1.PutBlobResponse;
import ai.pipestream.proto.repo.v1.RemovedDocumentNode;
import ai.pipestream.proto.repo.v1.SaveDocumentRequest;
import ai.pipestream.proto.repo.v1.SaveDocumentResponse;
import ai.pipestream.proto.repo.v1.WriteProvenance;
import ai.pipestream.proto.repo.container.blob.BlobStore;
import ai.pipestream.proto.repo.container.blob.DocumentIds;
import ai.pipestream.proto.repo.container.blob.PartStorage;
import ai.pipestream.proto.repo.container.codec.DocumentPartCodec;
import ai.pipestream.proto.repo.container.codec.PartLayout;
import ai.pipestream.proto.repo.container.codec.PartLayouts;
import ai.pipestream.proto.repo.container.codec.PartObject;
import ai.pipestream.proto.repo.container.ledger.DocumentLedger;
import ai.pipestream.proto.repo.container.ledger.DocumentRecord;
import ai.pipestream.proto.repo.container.ledger.DocumentRowKind;
import ai.pipestream.proto.repo.container.ledger.DocumentStatus;
import ai.pipestream.proto.repo.container.ledger.DriveLedger;
import ai.pipestream.proto.repo.container.ledger.DriveRecord;
import ai.pipestream.proto.repo.container.ledger.ListDocumentsFilter;
import ai.pipestream.proto.repo.container.ledger.ListDocumentsResult;
import ai.pipestream.proto.repo.container.ledger.Tx;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static ai.pipestream.proto.repo.service.GrpcErrors.failedPrecondition;
import static ai.pipestream.proto.repo.service.GrpcErrors.invalidArgument;
import static ai.pipestream.proto.repo.service.GrpcErrors.notFound;
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

    /**
     * @param documents the document-row ledger
     * @param drives the drive-row ledger (drive name → bucket/prefix)
     * @param tx the shared transaction wrapper, used directly for the two
     *        ad-hoc reads the ledgers deliberately do not expose (logical-row
     *        enumeration, account-less drive lookup)
     * @param blobStore the object-storage port every part IO goes through
     * @param partStorage the part fan-out IO layer
     */
    public DocumentGrpcService(DocumentLedger documents, DriveLedger drives, Tx tx,
            BlobStore blobStore, PartStorage partStorage) {
        this.documents = documents;
        this.drives = drives;
        this.tx = tx;
        this.blobStore = blobStore;
        this.partStorage = partStorage;
        this.layout = PartLayouts.document();
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
        ResolvedSave r = resolve(request);
        DriveRecord drive = drives.findByName(r.address().getAccountId(), request.getDrive())
                .orElseThrow(() -> notFound("drive '" + request.getDrive() + "' not found for account '"
                        + r.address().getAccountId() + "'"));
        UUID nodeId = DocumentIds.nodeId(r.address());
        String basePrefix = basePrefix(drive, r.address().getAccountId(), nodeId);

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
    private SaveDocumentResponse saveFull(ResolvedSave r, SaveDocumentRequest request,
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
            long nextVersion = manifestVersion(row) + 1;
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
                PART_CONTENT_TYPE, s3Metadata(r), request.getForceSave(), decision.nextDocVersion());

        DocumentRecord row = upsertRow(r, request, drive, nodeId, basePrefix, written.manifest(),
                written.rootChecksum(), written.totalSizeBytes(), written.coreEtag(),
                written.coreVersionId(), decision.existing());
        LOG.debug("Saved {} at {} (node_id={}, version={}, bytes={})",
                r.address().getDocId(), r.address().getGraphAddressId(), nodeId,
                decision.nextDocVersion(), written.totalSizeBytes());
        return saveResponse(row, written.rootChecksum());
    }

    /**
     * Partial save: write ONLY {@code parts_written} (and, within CHUNKS, only
     * {@code chunk_sets_written} when non-empty) from the supplied document;
     * carry every other PRESENT part forward from the copy source's manifest.
     * No dedupe on partial saves — they are pipeline restages, not re-crawls.
     */
    private SaveDocumentResponse savePartial(ResolvedSave r, SaveDocumentRequest request,
            DriveRecord drive, UUID nodeId, String basePrefix) {
        Set<DocumentPart> partsWritten = partsOrThrow(request.getPartsWrittenList(), "parts_written");
        if (!request.hasCopyUnwrittenPartsFrom()) {
            throw invalidArgument("copy_unwritten_parts_from is required when parts_written is non-empty");
        }
        NodeAddress srcRef = validateAddress(request.getCopyUnwrittenPartsFrom(),
                "copy_unwritten_parts_from");

        // The copy source must be a live row with a manifest; a gone source is
        // FAILED_PRECONDITION (not NOT_FOUND) so the caller's
        // retry-as-full-save policy engages.
        DocumentRecord srcRow = documents.findByReference(srcRef)
                .orElseThrow(() -> failedPrecondition("partial-save copy source row not found: " + describe(srcRef)));
        if (!DocumentStatus.AVAILABLE.equals(srcRow.status)) {
            throw failedPrecondition("partial-save copy source row is " + srcRow.status
                    + " (need AVAILABLE): " + describe(srcRef));
        }
        DocumentManifest srcManifest = srcRow.readManifest();
        if (srcManifest == null) {
            throw failedPrecondition("partial-save copy source row has no manifest: " + describe(srcRef));
        }
        DriveRecord srcDrive = drives.findByName(srcRow.accountId, srcRow.driveName)
                .orElseThrow(() -> failedPrecondition("partial-save copy source drive '"
                        + srcRow.driveName + "' not found for account '" + srcRow.accountId + "'"));

        DocumentRecord destExisting = documents.findByNodeId(nodeId).orElse(null);
        long docVersion = manifestVersion(destExisting) + 1;

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
                        + " is PRESENT but carries a blank object_key: " + describe(srcRef));
            }
        }

        PartStorage.WriteResult written = partStorage.writePartObjects(blobStore, drive.bucket, basePrefix,
                toWrite, r.address(),
                request.hasWrittenBy() ? request.getWrittenBy() : null,
                PART_CONTENT_TYPE, s3Metadata(r), request.getForceSave(), docVersion);

        // Copy-forward: same BlobStore on both ends (one storage backend per
        // service), so this is always a server-side copy — the bytes never
        // transit this service. Carried entries keep their original
        // sha256/size/updated_at/written_by stamps; only the object key moves.
        List<PartStorage.CopySpec> copies = new ArrayList<>(carried.size());
        List<PartManifestEntry> carriedAtDest = new ArrayList<>(carried.size());
        for (PartManifestEntry e : carried) {
            String destKey = DocumentPartCodec.objectKey(basePrefix, e.getPart(), e.getSubKey());
            copies.add(new PartStorage.CopySpec(e, destKey));
            carriedAtDest.add(e.toBuilder().setObjectKey(destKey).build());
        }
        try {
            partStorage.copyParts(blobStore, srcDrive.bucket, blobStore, drive.bucket, true, copies);
        } catch (RuntimeException e) {
            if (hasNotFoundCause(e)) {
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
        return saveResponse(row, rootChecksum);
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
        Timestamp now = timestampNow();
        for (DocumentPart part : CANONICAL_ORDER) {
            if (part == DocumentPart.DOCUMENT_PART_CHUNKS) {
                List<String> srcChunkOrder = source.getPartsList().stream()
                        .filter(e -> e.getPart() == part && e.getState() == PartState.PART_STATE_PRESENT)
                        .map(PartManifestEntry::getSubKey)
                        .toList();
                Set<String> placed = new HashSet<>();
                for (String subKey : srcChunkOrder) {
                    Optional<PartManifestEntry> replacement = findEntry(writtenPresent, part, subKey);
                    Optional<PartManifestEntry> kept = findEntry(carried, part, subKey);
                    if (replacement.isPresent()) {
                        ordered.add(replacement.get());
                        placed.add(subKey);
                    } else if (kept.isPresent()) {
                        ordered.add(kept.get());
                        placed.add(subKey);
                    }
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
                Optional<PartManifestEntry> w = findEntry(writtenPresent, part, null);
                if (w.isPresent()) {
                    ordered.add(w.get());
                } else {
                    ordered.add(findEntry(carried, part, null).orElse(emptyEntry(part, now)));
                }
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
            UUID nodeId = parseUuid(request.getNodeId(), "node_id");
            DocumentRecord row = documents.findByNodeId(nodeId)
                    .orElseThrow(() -> notFound("no document row for node_id " + nodeId));
            return assemble(row, partsOrThrow(request.getPartsList(), "parts"),
                    Set.copyOf(request.getChunkSetsList()));
        });
    }

    @Override
    public void getDocumentByReference(GetDocumentByReferenceRequest request,
            StreamObserver<GetDocumentResponse> observer) {
        GrpcErrors.run(observer, () -> {
            NodeAddress address = validateAddress(request.getAddress(), "address");
            DocumentRecord row = documents.findByReference(address)
                    .orElseThrow(() -> notFound("no document row for " + describe(address)));
            return assemble(row, partsOrThrow(request.getPartsList(), "parts"),
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
                    UUID nodeId = parseUuid(request.getNodeId(), "node_id");
                    yield documents.findByNodeId(nodeId)
                            .orElseThrow(() -> notFound("no document row for node_id " + nodeId));
                }
                case ADDRESS -> {
                    NodeAddress address = validateAddress(request.getAddress(), "address");
                    yield documents.findByReference(address)
                            .orElseThrow(() -> notFound("no document row for " + describe(address)));
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
     * matching row to PENDING_PURGE (metadata-only; the async purger lands in
     * a later phase — and the tombstone deliberately does not bump
     * {@code updated_at}). {@code purge_storage=true} FIRST deletes each
     * removed row's manifest-PRESENT object keys from its drive's bucket
     * (best-effort: failures are logged, the row removal is still reported),
     * then hard-deletes the rows. Nothing matched → NOTHING_TO_REMOVE.
     */
    private DeleteDocumentResponse delete(DeleteDocumentRequest request) {
        List<DocumentRecord> targets;
        NodeAddress byRef = null;
        DeleteLogicalDocumentCommand logical = null;
        switch (request.getCommandCase()) {
            case BY_REFERENCE -> {
                byRef = validateAddress(request.getByReference().getAddress(),
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
            // actually removed (a concurrent delete settles to empty).
            if (byRef != null) {
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
                documents.tombstone(row.nodeId).ifPresent(removed::add);
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
            long offset = parseContinuationToken(request.getContinuationToken());
            ListDocumentsResult result = documents.list(new ListDocumentsFilter(
                    blankToNull(request.getDrive()),
                    blankToNull(request.getConnectorId()),
                    blankToNull(request.getCrawlId()),
                    blankToNull(request.getAccountId()),
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
                        .setAddress(addressOf(row));
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
        GrpcErrors.run(observer, () -> {
            if (!request.hasStorageRef()) {
                throw invalidArgument("storage_ref is required");
            }
            FileStorageReference ref = request.getStorageRef();
            if (ref.getDriveName().isBlank()) {
                throw invalidArgument("storage_ref.drive_name is required");
            }
            if (ref.getObjectKey().isBlank()) {
                throw invalidArgument("storage_ref.object_key is required");
            }
            DriveRecord drive = findDriveByName(ref.getDriveName())
                    .orElseThrow(() -> notFound("drive '" + ref.getDriveName() + "' not found"));
            BlobStore.GetResult got = blobStore.get(drive.bucket, ref.getObjectKey(),
                    ref.hasVersionId() && !ref.getVersionId().isBlank() ? ref.getVersionId() : null);
            GetBlobResponse.Builder response = GetBlobResponse.newBuilder()
                    .setData(ByteString.copyFrom(got.data()))
                    .setSizeBytes(got.data().length)
                    .setRetrievedAtEpochMs(System.currentTimeMillis());
            if (got.contentType() != null) {
                response.setMimeType(got.contentType());
            }
            return response.build();
        });
    }

    @Override
    public void putBlob(PutBlobRequest request, StreamObserver<PutBlobResponse> observer) {
        GrpcErrors.run(observer, () -> {
            if (request.getDriveName().isBlank()) {
                throw invalidArgument("drive_name is required");
            }
            DriveRecord drive = findDriveByName(request.getDriveName())
                    .orElseThrow(() -> notFound("drive '" + request.getDriveName() + "' not found"));
            byte[] data = request.getData().toByteArray();
            String sha256 = DocumentPartCodec.sha256Hex(data);
            // Content-addressed default key: identical puts land on the same
            // object, so a retried upload is an idempotent overwrite, never a
            // second randomly-keyed copy.
            String objectKey = request.getObjectKey().isBlank()
                    ? generatedBlobKey(drive, sha256)
                    : request.getObjectKey();
            String contentType = request.getMimeType().isBlank()
                    ? "application/octet-stream" : request.getMimeType();
            // Verified write: the store's checksum trailer makes it reject
            // the PUT when the landed bytes mismatch the computed digest.
            blobStore.put(new BlobStore.PutSpec(drive.bucket, objectKey, contentType, null, sha256),
                    data);
            return PutBlobResponse.newBuilder()
                    .setStorageRef(FileStorageReference.newBuilder()
                            .setDriveName(request.getDriveName())
                            .setObjectKey(objectKey))
                    .setSizeBytes(data.length)
                    .setSha256(sha256)
                    .build();
        });
    }

    @Override
    public void deleteBlob(DeleteBlobRequest request, StreamObserver<DeleteBlobResponse> observer) {
        GrpcErrors.run(observer, () -> {
            if (!request.hasStorageRef()) {
                throw invalidArgument("storage_ref is required");
            }
            FileStorageReference ref = request.getStorageRef();
            if (ref.getDriveName().isBlank()) {
                throw invalidArgument("storage_ref.drive_name is required");
            }
            if (ref.getObjectKey().isBlank()) {
                throw invalidArgument("storage_ref.object_key is required");
            }
            DriveRecord drive = findDriveByName(ref.getDriveName())
                    .orElseThrow(() -> notFound("drive '" + ref.getDriveName() + "' not found"));
            // Idempotent: delete-of-absent reports deleted=false, not an error.
            boolean deleted = blobStore.delete(drive.bucket, ref.getObjectKey());
            return DeleteBlobResponse.newBuilder().setDeleted(deleted).build();
        });
    }

    /** Content-addressed default blob key: {@code <drive.prefix>/blobs/<name-uuid of the sha256>}. */
    private static String generatedBlobKey(DriveRecord drive, String sha256Hex) {
        String prefix = drive.prefix == null ? "" : drive.prefix;
        if (prefix.endsWith("/")) {
            prefix = prefix.substring(0, prefix.length() - 1);
        }
        String nameUuid = UUID.nameUUIDFromBytes(
                ("blob-content|" + sha256Hex).getBytes(java.nio.charset.StandardCharsets.UTF_8))
                .toString();
        return (prefix.isBlank() ? "" : prefix + "/") + "blobs/" + nameUuid;
    }

    /**
     * Drive lookup by bare name, across accounts. {@link FileStorageReference}
     * carries no account, and drive names are unique only per account — v1
     * trusts the caller's drive reference and takes the first match. Tighten
     * this if multi-account name reuse becomes real.
     */
    private Optional<DriveRecord> findDriveByName(String name) {
        return tx.readOnly(em -> em.createQuery(
                        "SELECT d FROM DriveRecord d WHERE d.name = :name", DriveRecord.class)
                .setParameter("name", name)
                .setMaxResults(1)
                .getResultStream()
                .findFirst());
    }

    // ------------------------------------------------------------------ plumbing

    /**
     * Everything validation and defaulting settled about a save: the document
     * (always carrying a doc id) plus the canonical {@link NodeAddress} of the
     * storage identity, the row kind and the cluster hint. The address's
     * {@code graph_id} is never blank on either kind — blank-graph rows are
     * unrepresentable.
     */
    private record ResolvedSave(Document doc, NodeAddress address, String rowKind, String clusterId) {
    }

    /**
     * Validation + defaulting half of a save: ownership/account required, a
     * blank doc id mints a fresh UUID (the caller is intake and will persist
     * the returned coordinates), and the {@code graph_address} oneof arm is
     * the EXPLICIT origin discriminator with {@code graph_id} required on both
     * arms — every rejection names the offending field.
     */
    private ResolvedSave resolve(SaveDocumentRequest request) {
        if (!request.hasDocument()) {
            throw invalidArgument("document is required");
        }
        Document doc = request.getDocument();
        if (doc.getDocId().isBlank()) {
            doc = doc.toBuilder().setDocId(UUID.randomUUID().toString()).build();
        }
        if (!doc.hasOwnership()) {
            throw invalidArgument("document.ownership is required");
        }
        OwnershipContext ownership = doc.getOwnership();
        String accountId = ownership.getAccountId();
        if (accountId.isBlank()) {
            throw invalidArgument("document.ownership.account_id is required");
        }
        if (request.getDrive().isBlank()) {
            throw invalidArgument("drive is required");
        }
        String requestGraphId = request.hasGraphId() ? request.getGraphId() : "";

        return switch (request.getGraphAddressCase()) {
            case USE_DATASOURCE_ID -> {
                if (request.hasClusterId() && !request.getClusterId().isBlank()) {
                    throw invalidArgument("cluster_id must be absent on intake saves"
                            + " (use_datasource_id); the intake layer is its own single-node graph");
                }
                String expected = "intake:" + accountId;
                if (requestGraphId.isBlank()) {
                    throw invalidArgument("graph_id is required on intake saves"
                            + " (use_datasource_id): expected \"" + expected + "\"");
                }
                if (!expected.equals(requestGraphId)) {
                    throw invalidArgument("graph_id must equal the account's intake graph \""
                            + expected + "\" on intake saves (got \"" + requestGraphId + "\")");
                }
                String datasourceId = ownership.getDatasourceId();
                if (datasourceId.isBlank()) {
                    throw invalidArgument(
                            "document.ownership.datasource_id is required on intake saves"
                                    + " (use_datasource_id) — it is the storage address");
                }
                yield new ResolvedSave(doc, NodeAddress.newBuilder()
                        .setDocId(doc.getDocId())
                        .setGraphAddressId(datasourceId)
                        .setAccountId(accountId)
                        .setGraphId(requestGraphId)
                        .build(), DocumentRowKind.INTAKE, null);
            }
            case GRAPH_LOCATION_ID -> {
                String graphAddressId = request.getGraphLocationId();
                if (graphAddressId.isBlank()) {
                    throw invalidArgument("graph_location_id must not be blank");
                }
                if (requestGraphId.isBlank()) {
                    throw invalidArgument("graph_id is required on pipeline saves"
                            + " (graph_location_id=\"" + graphAddressId + "\")");
                }
                if (requestGraphId.startsWith("intake:")) {
                    throw invalidArgument("graph_id must not be an intake graph id on pipeline saves"
                            + " (got \"" + requestGraphId + "\")");
                }
                String clusterId = request.hasClusterId() && !request.getClusterId().isBlank()
                        ? request.getClusterId() : null;
                yield new ResolvedSave(doc, NodeAddress.newBuilder()
                        .setDocId(doc.getDocId())
                        .setGraphAddressId(graphAddressId)
                        .setAccountId(accountId)
                        .setGraphId(requestGraphId)
                        .build(), DocumentRowKind.PIPELINE, clusterId);
            }
            default -> throw invalidArgument(
                    "exactly one graph address arm (use_datasource_id or graph_location_id) must be set");
        };
    }

    /**
     * Insert-or-update the ledger row for a landed body. The body is already
     * in object storage by the time this runs, so the row goes straight to
     * AVAILABLE and {@code updated_at} moves — this IS the revive for a
     * re-saved tombstoned row. Bookkeeping columns (reprocess markers,
     * created_at) survive a rewrite from the existing row.
     */
    private DocumentRecord upsertRow(ResolvedSave r, SaveDocumentRequest request, DriveRecord drive,
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
        return documents.save(row);
    }

    private static SaveDocumentResponse saveResponse(DocumentRecord row, String rootChecksum) {
        return SaveDocumentResponse.newBuilder()
                .setNodeId(row.nodeId.toString())
                .setDrive(row.driveName)
                .setStoragePrefix(row.objectKey)
                .setSizeBytes(row.sizeBytes)
                .setChecksum(rootChecksum)
                .setCreatedAtEpochMs(row.createdAt.toEpochMilli())
                .setDeduplicated(false)
                .setAddress(addressOf(row))
                .build();
    }

    /** The row's canonical storage address, rebuilt from its identity columns. */
    private static NodeAddress addressOf(DocumentRecord row) {
        return NodeAddress.newBuilder()
                .setDocId(row.docId)
                .setGraphAddressId(row.graphAddressId)
                .setAccountId(row.accountId)
                .setGraphId(row.graphId)
                .build();
    }

    /** Part-object key root: {@code <drive.prefix>/documents/<accountId>/<nodeId>}. */
    private static String basePrefix(DriveRecord drive, String accountId, UUID nodeId) {
        String prefix = drive.prefix == null ? "" : drive.prefix;
        if (prefix.endsWith("/")) {
            prefix = prefix.substring(0, prefix.length() - 1);
        }
        return (prefix.isBlank() ? "" : prefix + "/") + "documents/" + accountId + "/" + nodeId;
    }

    /** Provider metadata stamped on every part object for observability. */
    private static Map<String, String> s3Metadata(ResolvedSave r) {
        Map<String, String> metadata = new HashMap<>();
        metadata.put("doc-id", r.address().getDocId());
        metadata.put("account-id", r.address().getAccountId());
        metadata.put("graph-id", r.address().getGraphId());
        metadata.put("row-kind", r.rowKind());
        metadata.put("graph-address-id", r.address().getGraphAddressId());
        return metadata;
    }

    /** The row's current manifest doc_version, or 0 when absent/unparseable. */
    private static long manifestVersion(DocumentRecord row) {
        if (row == null) {
            return 0;
        }
        try {
            DocumentManifest manifest = row.readManifest();
            return manifest == null ? 0 : manifest.getDocVersion();
        } catch (RuntimeException e) {
            return 0;
        }
    }

    private static Set<DocumentPart> partsOrThrow(List<DocumentPart> parts, String field) {
        Set<DocumentPart> out = EnumSet.noneOf(DocumentPart.class);
        for (DocumentPart part : parts) {
            if (part == DocumentPart.DOCUMENT_PART_UNSPECIFIED || part == DocumentPart.UNRECOGNIZED) {
                throw invalidArgument(field + " must not contain DOCUMENT_PART_UNSPECIFIED");
            }
            out.add(part);
        }
        return out;
    }

    private static NodeAddress validateAddress(NodeAddress address, String field) {
        if (address.getDocId().isBlank() || address.getGraphAddressId().isBlank()
                || address.getAccountId().isBlank() || address.getGraphId().isBlank()) {
            throw invalidArgument(field + " requires doc_id, graph_address_id, account_id and graph_id");
        }
        return address;
    }

    private static String describe(NodeAddress address) {
        return "doc_id=" + address.getDocId() + ", graph_address_id=" + address.getGraphAddressId()
                + ", account_id=" + address.getAccountId() + ", graph_id=" + address.getGraphId();
    }

    private static UUID parseUuid(String raw, String field) {
        if (raw == null || raw.isBlank()) {
            throw invalidArgument(field + " is required");
        }
        try {
            return UUID.fromString(raw.trim());
        } catch (IllegalArgumentException e) {
            throw invalidArgument(field + " must be a UUID (got \"" + raw + "\")");
        }
    }

    private static long parseContinuationToken(String token) {
        if (token == null || token.isBlank()) {
            return 0;
        }
        try {
            long offset = Long.parseLong(token.trim());
            if (offset < 0) {
                throw invalidArgument("continuation_token must be a non-negative row offset");
            }
            return offset;
        } catch (NumberFormatException e) {
            throw invalidArgument("continuation_token must be a row offset (got \"" + token + "\")");
        }
    }

    private static String blankToNull(String value) {
        return value == null || value.isBlank() ? null : value;
    }

    private static boolean hasNotFoundCause(Throwable t) {
        for (Throwable cur = t; cur != null; cur = cur.getCause() == cur ? null : cur.getCause()) {
            if (cur instanceof BlobStore.BlobNotFoundException) {
                return true;
            }
        }
        return false;
    }

    private static Timestamp timestampNow() {
        Instant now = Instant.now();
        return Timestamp.newBuilder().setSeconds(now.getEpochSecond()).setNanos(now.getNano()).build();
    }
}
