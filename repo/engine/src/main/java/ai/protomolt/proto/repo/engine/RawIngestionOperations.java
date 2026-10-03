package ai.protomolt.proto.repo.engine;

import ai.protomolt.proto.repo.blob.spi.BlobCapability;
import ai.protomolt.proto.repo.blob.spi.BlobStore;
import ai.protomolt.proto.repo.container.blob.DocumentIds;
import ai.protomolt.proto.repo.container.ledger.*;
import ai.protomolt.proto.repo.spi.RawIngestionRepository;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.v1.*;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.HexFormat;
import java.util.Set;
import java.util.UUID;

/** Immutable streamed intake with durable candidates and transactional document admission. */
public final class RawIngestionOperations implements RawIngestionRepository {
    private static final Duration LEASE = Duration.ofMinutes(5);
    private static final long RENEW_NANOS = Duration.ofMinutes(1).toNanos();
    private static final int PUBLICATION_ATTEMPTS = 3;
    private final DocumentOperations documents;
    private final DocumentLedger ledger;
    private final DriveLedger drives;
    private final RawObjectLedger raw;
    private final BlobStore store;
    private final String backendIdentity;

    /**
     * The host must qualify backendIdentity against deployment retention policy,
     * not just these adapter capabilities. Expiring/remote adapters fail closed.
     * This component borrows its collaborators; their lifetimes belong to the host.
     */
    public RawIngestionOperations(DocumentOperations documents, DocumentLedger ledger, DriveLedger drives,
            BlobStore store, String backendIdentity, Set<BlobCapability> capabilities) {
        this.documents = java.util.Objects.requireNonNull(documents, "documents");
        this.ledger = java.util.Objects.requireNonNull(ledger, "ledger");
        this.drives = java.util.Objects.requireNonNull(drives, "drives");
        this.store = java.util.Objects.requireNonNull(store, "store");
        requireText(backendIdentity, "backendIdentity");
        if (!Set.copyOf(capabilities).containsAll(Set.of(BlobCapability.STREAMING_WRITE, BlobCapability.NON_EXPIRING_WRITES)))
            throw new RepositoryException(RepositoryException.Code.UNSUPPORTED,
                    "Managed ingestion requires non-expiring streaming writes");
        this.backendIdentity = backendIdentity;
        this.raw = ledger.rawObjects();
    }

    @Override public Result upload(RepositoryCaller caller, Request request, InputStream body, long knownLength) {
        RepositoryErrors.requireProcessAuthority(caller);
        java.util.Objects.requireNonNull(request, "request");
        java.util.Objects.requireNonNull(body, "body");
        requireText(request.accountId(), "accountId");
        requireText(request.datasourceId(), "datasourceId");
        requireText(request.driveName(), "driveName");
        requireText(request.filename(), "filename");
        requireText(request.mimeType(), "mimeType");
        if (knownLength < 0) throw RepositoryErrors.invalidArgument("knownLength must be nonnegative");
        String declared = blank(request.declaredSha256()) ? null : request.declaredSha256().trim().toLowerCase(java.util.Locale.ROOT);
        if (declared != null && !declared.matches("[0-9a-f]{64}"))
            throw RepositoryErrors.invalidArgument("declaredSha256 must be 64 hexadecimal characters");
        try { return RepositoryErrors.call(() -> ingest(caller, request, body, knownLength, declared)); }
        catch (RepositoryException domain) { throw domain; }
        catch (RuntimeException failure) {
            // Providers and persistence adapters may throw their own exceptions.
            // Preserve the cause without leaking an SDK type as the API contract;
            // never assume a failed acknowledgement means no commit occurred.
            throw new RepositoryException(RepositoryException.Code.UNAVAILABLE,
                    "Raw ingestion did not complete; publication outcome is not confirmed", failure);
        }
    }

    private Result ingest(RepositoryCaller caller, Request request, InputStream body, long length, String declared) {
        checkCancellation();
        DriveRecord drive = drives.findByName(request.accountId(), request.driveName())
                .orElseThrow(() -> RepositoryErrors.notFound("Upload drive is unavailable"));
        if (!"ACTIVE".equals(drive.status)) throw RepositoryErrors.failedPrecondition("Upload drive is not active");
        var admittedDrive = ManagedRawBindings.DriveState.of(drive);
        var attempt = raw.begin(new RawObjectLedger.Location(request.accountId(), backendIdentity, drive.driveId,
                drive.name, drive.bucket, DriveKeys.managedRaw(drive.prefix, UUID.randomUUID())),
                length, request.mimeType(), LEASE);
        UploadInput input = new UploadInput(body, length, attempt);
        BlobStore.PutResult put;
        try {
            put = store.put(new BlobStore.PutSpec(drive.bucket, attempt.objectKey, request.mimeType(), null, null), input, length);
            input.finish();
        } catch (IOException failure) {
            throw new RepositoryException(RepositoryException.Code.UNAVAILABLE, "Upload body read failed", failure);
        } catch (RuntimeException failure) {
            // An SDK may wrap a failure raised by the supplied stream. Preserve
            // a length violation we actually observed, not an inferred SDK error.
            if (input.lengthFailure != null && failure != input.lengthFailure)
                throw new RepositoryException(RepositoryException.Code.INVALID_ARGUMENT,
                        input.lengthFailure.getMessage(), failure);
            throw failure;
        }
        if (put == null) throw new RepositoryException(RepositoryException.Code.INTERNAL, "Upload returned no storage receipt");
        String sha = HexFormat.of().formatHex(input.digest.digest());
        if (declared != null && !declared.equals(sha))
            throw RepositoryErrors.invalidArgument("X-Content-Sha256 mismatch: received body has a different checksum");
        checkCancellation();
        RawObjectRecord verified;
        try { verified = raw.verify(attempt.rawId, attempt.leaseToken, input.count, sha, put.versionId(), put.eTag()); }
        catch (RawObjectLedger.FenceException expired) {
            throw new RepositoryException(RepositoryException.Code.CONFLICT, "Upload lease expired before verification", expired);
        }
        String docId = blank(request.docId()) ? UUID.nameUUIDFromBytes(
                ("doc-content|" + sha).getBytes(StandardCharsets.UTF_8)).toString() : request.docId();
        var ref = FileStorageReference.newBuilder().setDriveName(drive.name).setObjectKey(attempt.objectKey);
        if (put.versionId() != null) ref.setVersionId(put.versionId());
        Blob blob = Blob.newBuilder().setBlobId(DocumentIds.blobId(docId, request.datasourceId(), request.accountId()).toString())
                .setDriveId(drive.name).setStorageRef(ref).setMimeType(request.mimeType()).setFilename(request.filename())
                .setSizeBytes(length).setChecksum(sha).setChecksumType(ChecksumType.CHECKSUM_TYPE_SHA256).build();
        var ownership = OwnershipContext.newBuilder().setAccountId(request.accountId()).setDatasourceId(request.datasourceId());
        if (!blank(request.connectorId())) ownership.setConnectorId(request.connectorId());
        Document candidate = Document.newBuilder().setDocId(docId).setOwnership(ownership)
                .setBlobBag(BlobBag.newBuilder().setBlob(blob)).build();
        NodeAddress address = NodeAddress.newBuilder().setDocId(docId).setAccountId(request.accountId())
                .setGraphId("intake:" + request.accountId()).setGraphAddressId(request.datasourceId()).build();
        UUID node = DocumentIds.nodeId(address);
        for (int retry = 0; retry < PUBLICATION_ATTEMPTS; retry++) {
            checkCancellation();
            DocumentRecord current = ledger.findByNodeId(node).orElse(null);
            // Upload metadata does not express policy edits. Preserve the current
            // ledger ACL and deletion controls when replacing an existing body.
            var currentCandidate = candidate.toBuilder();
            if (current != null && current.readSecurity() != null)
                currentCandidate.getOwnershipBuilder().setSecurity(current.readSecurity());
            Document normalized = normalize(caller, currentCandidate.build(), current);
            var save = SaveDocumentRequest.newBuilder().setDocument(normalized).setDrive(drive.name)
                    .setUseDatasourceId(true).setGraphId(address.getGraphId());
            if (!blank(request.connectorId())) save.setConnectorId(request.connectorId());
            if (!blank(request.crawlId())) save.setCrawlId(request.crawlId());
            if (current != null) {
                save.setDeleteSourceBlobsOnSettle(current.deleteSourceBlobsOnSettle);
                if (current.sourceBlobDeleteReason != null) save.setSourceBlobDeleteReason(current.sourceBlobDeleteReason);
            }
            try {
                var committed = documents.saveIngested(caller, save.build(), verified.rawId, verified.leaseToken,
                        current == null ? null : current.mutationRevision, drive.driveId, admittedDrive);
                return new Result(attempt.rawId, committed, normalized.getBlobBag().getBlob().getStorageRef(), length, sha);
            } catch (RepositoryException conflict) {
                if (conflict.code() != RepositoryException.Code.CONFLICT
                        || !(conflict.getCause() instanceof DocumentLedger.RevisionConflictException)
                        || retry + 1 == PUBLICATION_ATTEMPTS) throw conflict;
                // Only a known precommit conflict is retried. Every pass samples
                // the current document again; storage/SQL ambiguity propagates.
            }
        }
        throw new AssertionError("Publication retry loop exhausted without a result");
    }

    private Document normalize(RepositoryCaller caller, Document candidate, DocumentRecord current) {
        if (current == null || !DocumentStatus.AVAILABLE.equals(current.status) || raw.references(current.nodeId).isEmpty())
            return candidate;
        var stored = documents.getDocument(caller, GetDocumentRequest.newBuilder().setNodeId(current.nodeId.toString())
                .addParts(DocumentPart.DOCUMENT_PART_BLOBS).build()).getDocument().getBlobBag();
        if (!stored.hasBlob()) return candidate;
        Blob previous = stored.getBlob();
        Blob incoming = candidate.getBlobBag().getBlob();
        if (!previous.hasStorageRef() || !DriveKeys.isManaged(previous.getStorageRef().getObjectKey())
                || !previous.toBuilder().clearStorageRef().build().equals(incoming.toBuilder().clearStorageRef().build()))
            return candidate;
        // The candidate digest/length came from consumed bytes. The document
        // engine validates this retained reference against the sampled binding
        // and revision before either dedupe or rewrite can commit.
        return candidate.toBuilder().setBlobBag(BlobBag.newBuilder()
                .setBlob(incoming.toBuilder().setStorageRef(previous.getStorageRef()))).build();
    }

    private final class UploadInput extends InputStream {
        private final InputStream body;
        private final long length;
        private final RawObjectRecord attempt;
        private final MessageDigest digest = sha256();
        private long nextRenewal = System.nanoTime() + RENEW_NANOS;
        private long count;
        private RepositoryException lengthFailure;
        UploadInput(InputStream body, long length, RawObjectRecord attempt) {
            this.body = body; this.length = length; this.attempt = attempt;
        }
        @Override public int read() throws IOException {
            byte[] single = new byte[1];
            int got = read(single, 0, 1);
            return got < 0 ? -1 : single[0] & 0xff;
        }
        @Override public int read(byte[] bytes, int offset, int size) throws IOException {
            if (lengthFailure != null) throw lengthFailure;
            checkCancellation();
            if (System.nanoTime() - nextRenewal >= 0) {
                try { raw.renew(attempt.rawId, attempt.leaseToken, LEASE); }
                catch (RawObjectLedger.FenceException expired) {
                    throw new RepositoryException(RepositoryException.Code.CONFLICT, "Upload lease expired while streaming", expired);
                }
                nextRenewal = System.nanoTime() + RENEW_NANOS;
            }
            int got = body.read(bytes, offset, size);
            if (got < 0 && count < length) throw invalidLength("Upload body is shorter than knownLength");
            if (got > 0) {
                count = Math.addExact(count, got);
                if (count > length) throw invalidLength("Upload body exceeds knownLength");
                digest.update(bytes, offset, got);
            }
            return got;
        }
        private RepositoryException invalidLength(String message) {
            lengthFailure = RepositoryErrors.invalidArgument(message);
            return lengthFailure;
        }
        void finish() throws IOException {
            checkCancellation();
            if (count != length) throw RepositoryErrors.invalidArgument("Upload body length differs from knownLength");
            if (read() != -1) throw RepositoryErrors.invalidArgument("Upload body exceeds knownLength");
        }
        // Deliberately inherit InputStream.close(): this wrapper borrows the body.
    }

    private static boolean blank(String value) { return value == null || value.isBlank(); }
    private static void requireText(String value, String field) {
        if (blank(value)) throw RepositoryErrors.invalidArgument(field + " is required");
    }
    private static void checkCancellation() {
        if (Thread.currentThread().isInterrupted())
            throw new RepositoryException(RepositoryException.Code.CANCELLED, "Upload was interrupted");
    }
    private static MessageDigest sha256() {
        try { return MessageDigest.getInstance("SHA-256"); }
        catch (NoSuchAlgorithmException impossible) { throw new IllegalStateException("SHA-256 is unavailable", impossible); }
    }
}
