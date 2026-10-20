package ai.protomolt.proto.intake.service;

import ai.protomolt.proto.intake.service.identity.IntakeScope;
import ai.protomolt.proto.repo.v1.Blob;
import ai.protomolt.proto.repo.v1.BlobBag;
import ai.protomolt.proto.repo.v1.ChecksumType;
import ai.protomolt.proto.repo.v1.DocIdDerivation;
import ai.protomolt.proto.repo.v1.DocIdDerivationMethod;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v2.DocumentServiceGrpc;
import ai.protomolt.proto.repo.v1.OwnershipContext;
import ai.protomolt.proto.repo.v2.SaveDocumentRequest;
import ai.protomolt.proto.repo.v2.SaveDocumentResponse;
import ai.protomolt.proto.repo.v1.SearchMetadata;
import ai.protomolt.proto.repo.v2.*;
import ai.protomolt.proto.descriptors.MessageFingerprints;
import com.google.protobuf.ByteString;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Map;
import java.util.UUID;

/**
 * The wrap-and-save core every intake lane shares: scope narrowing, payload
 * caps, the raw-payload Document shape, and the intake arm of repo's save
 * contract ({@code use_datasource_id=true},
 * {@code graph_id = "intake:" + account_id}). The gRPC lanes
 * ({@link IntakeGrpcService}) and the HTTP lane ({@link IntakeHttpServer})
 * both delegate here, so a rule changed once changes everywhere.
 *
 * <p>Failures are raised as gRPC status errors at the decision point —
 * INVALID_ARGUMENT for missing targeting, PERMISSION_DENIED for scope
 * violations, RESOURCE_EXHAUSTED for cap breaches — which the gRPC lanes
 * forward as-is and the HTTP lane flattens onto HTTP status codes.
 */
final class RawIngests {

    private final DocumentServiceGrpc.DocumentServiceBlockingStub documents;
    private final long maxPayloadBytes;
    private final java.util.Set<String> stepWriters;

    /** Constructs an intake saver with the default pipeline step writers. */
    RawIngests(DocumentServiceGrpc.DocumentServiceBlockingStub documents, long maxPayloadBytes) {
        this(documents, maxPayloadBytes, IntakeServiceConfig.DEFAULT_STEP_WRITERS);
    }

    /**
     * @param documents the repo-service document stub every save goes through
     * @param maxPayloadBytes the service-wide payload cap for the in-memory
     *        lanes; must be positive
     * @param stepWriters pipeline step module IDs that gain read+write ACL rules
     *        on created documents; empty keeps documents process-authority-only
     */
    RawIngests(DocumentServiceGrpc.DocumentServiceBlockingStub documents, long maxPayloadBytes,
            java.util.Set<String> stepWriters) {
        if (documents == null) {
            throw new IllegalArgumentException("documents stub must not be null");
        }
        if (maxPayloadBytes <= 0) {
            throw new IllegalArgumentException("maxPayloadBytes must be positive");
        }
        this.documents = documents;
        this.maxPayloadBytes = maxPayloadBytes;
        this.stepWriters = java.util.Set.copyOf(stepWriters);
    }

    /**
     * Wraps raw bytes into the content-hash-identified intake Document and
     * saves it: caps and content-type restriction enforced, doc id derived
     * from the payload SHA-256, inline blob with the checksum stamped,
     * ownership from the scope, connector identity {@code "intake"}.
     */
    Saved saveRawPayload(
            IntakeScope scope,
            Targeting targeting,
            ByteString data,
            String filename,
            String mimeType,
            Map<String, String> metadata, ExpectedRevision expected) {
        requirePayloadWithinCaps(scope, data.size());
        requireDeclaredMimeTypeWithinScope(scope, mimeType);
        byte[] digest = sha256(data.toByteArray());
        String sha256 = HexFormat.of().formatHex(digest);
        String docId = UUID.nameUUIDFromBytes(digest).toString();

        Blob.Builder blob =
                Blob.newBuilder()
                        .setBlobId(docId)
                        .setDriveId(targeting.drive())
                        .setData(data)
                        .setSizeBytes(data.size())
                        .setChecksum(sha256)
                        .setChecksumType(ChecksumType.CHECKSUM_TYPE_SHA256);
        if (!filename.isBlank()) {
            blob.setFilename(filename);
        }
        if (!mimeType.isBlank()) {
            blob.setMimeType(mimeType);
        }
        SearchMetadata.Builder searchMetadata = SearchMetadata.newBuilder();
        if (!mimeType.isBlank()) {
            searchMetadata.setSourceMimeType(mimeType);
        }
        Document document =
                Document.newBuilder()
                        .setDocId(docId)
                        .setSearchMetadata(searchMetadata)
                        .setBlobBag(BlobBag.newBuilder().setBlob(blob))
                        .setOwnership(
                                OwnershipContext.newBuilder()
                                        .setAccountId(scope.accountId())
                                        .setDatasourceId(targeting.datasourceId())
                                        .setConnectorId(IntakeGrpcService.CONNECTOR_ID))
                        .setDocIdDerivation(
                                DocIdDerivation.newBuilder()
                                        .setMethod(DocIdDerivationMethod.DOC_ID_DERIVATION_METHOD_CONTENT_HASH)
                                        .setSourceValue(sha256))
                        .build();
        return save(scope, targeting, document, metadata, sha256, data.size(), expected);
    }

    /**
     * Issues the intake save for an already-assembled Document:
     * {@code use_datasource_id=true} at the account's intake graph, connector
     * and provenance identity {@code "intake"}.
     */
    Saved save(
            IntakeScope scope,
            Targeting targeting,
            Document document,
            Map<String, String> metadata,
            String sha256,
            long payloadSize, ExpectedRevision expected) {
        if (expected != null) requireExpected(expected);
        var content = document.toBuilder();
        if (document.hasBlobBag()) {
            var bag = document.getBlobBag().toBuilder();
            switch (bag.getBlobDataCase()) {
                case BLOB -> bag.setBlob(inlineBlob(bag.getBlob(), targeting.drive()));
                case BLOBS -> {
                    var blobs = bag.getBlobs().toBuilder().clearBlob();
                    for (var blob : bag.getBlobs().getBlobList()) blobs.addBlob(inlineBlob(blob, targeting.drive()));
                    bag.setBlobs(blobs);
                }
                case BLOBDATA_NOT_SET -> { }
            }
            content.setBlobBag(bag);
        }
        document = withStepWriters(content.build());
        var origin = IntakeOrigin.newBuilder().setDatasourceId(targeting.datasourceId());
        if (document.getOwnership().hasConnectorId()) origin.setConnectorId(document.getOwnership().getConnectorId());
        SaveDocumentRequest.Builder request =
                SaveDocumentRequest.newBuilder()
                        .setDocument(document)
                        .setDrive(targeting.drive())
                        .setTarget(DocumentTarget.newBuilder().setUseDatasourceId(targeting.datasourceId()))
                        .setOwner(SaveOwner.newBuilder().setIntake(origin))
                        .addWrites(put(DocumentPart.DOCUMENT_PART_CORE))
                        .putAllMetadata(metadata);
        if (document.hasBlobBag()) request.addWrites(put(DocumentPart.DOCUMENT_PART_BLOBS));
        if (document.getParserResultsCount() > 0) request.addWrites(put(DocumentPart.DOCUMENT_PART_PARSED));
        for (var segmentation : document.getSearchMetadata().getSegmentationsList())
            request.addWrites(PartWrite.newBuilder().setSelector(PartSelector.newBuilder()
                    .setSegmentationId(segmentation.getSegmentationId())).setAction(WriteAction.WRITE_ACTION_PUT));
        for (var embedding : document.getSearchMetadata().getEmbeddingSetsList())
            request.addWrites(PartWrite.newBuilder().setSelector(PartSelector.newBuilder().setEmbeddingSet(
                    ai.protomolt.proto.repo.semantic.v1.EmbeddingSetKey.newBuilder()
                            .setSegmentationId(embedding.getSegmentationId()).setEmbeddingSetId(embedding.getEmbeddingSetId())))
                    .setAction(WriteAction.WRITE_ACTION_PUT));
        if (expected == null) {
            var address = ai.protomolt.proto.repo.v1.NodeAddress.newBuilder().setAccountId(scope.accountId())
                    .setGraphId(IntakeGrpcService.INTAKE_GRAPH_PREFIX + scope.accountId())
                    .setGraphAddressId(targeting.datasourceId()).setDocId(document.getDocId()).build();
            var selection = PartSelection.newBuilder();
            request.getWritesList().forEach(write -> selection.addSelectors(write.getSelector()));
            GetDocumentResponse existing = null;
            try {
                existing = documents.getDocument(GetDocumentRequest.newBuilder()
                        .setAddress(address).setSelection(selection).build());
            } catch (io.grpc.StatusRuntimeException failure) {
                if (failure.getStatus().getCode() != io.grpc.Status.Code.NOT_FOUND) throw failure;
            }
            if (existing == null) expected = ExpectedRevision.newBuilder().setNone(true).build();
            else {
                var owner = existing.getDocument().getOwnership();
                if (!owner.getAccountId().equals(scope.accountId())
                        || !owner.getDatasourceId().equals(targeting.datasourceId())
                        || (document.getOwnership().hasConnectorId()
                            && !owner.getConnectorId().equals(document.getOwnership().getConnectorId())))
                    throw GrpcErrors.permissionDenied("Existing document ownership differs from intake source");
                long revision = existing.getManifest().getRevision();
                if (!existing.getManifest().getAddress().equals(address) || revision <= 0)
                    throw io.grpc.Status.DATA_LOSS.withDescription("Repository returned an invalid document identity").asRuntimeException();
                if (existing.getMetadataMap().equals(metadata)
                        && IngestComparisons.same(documents, address, revision, document, existing.getDocument()))
                    return new Saved(address, revision, true, targeting.drive(), sha256, payloadSize);
                expected = ExpectedRevision.newBuilder().setRevision(revision).build();
            }
        }
        request.setExpected(expected);
        // Include the full typed intent, including metadata and target, in retry identity.
        // The shared fingerprint helper uses deterministic protobuf encoding, never JSON.
        request.setOperationId("intake-v2:" + MessageFingerprints.fingerprint(request.build()));
        SaveDocumentResponse response = documents.saveDocument(request.build());
        return new Saved(response.getAccession().getAddress(), response.getAccession().getRevision(),
                response.getReplayed(), targeting.drive(), sha256, payloadSize);
    }

    /** Null denotes UPSERT; every explicit expectation remains a repository CAS. */
    static ExpectedRevision resolveExpected(ai.protomolt.proto.intake.v1.IngestMode mode, ExpectedRevision expected) {
        if (expected != null) requireExpected(expected);
        return switch (mode) {
            case INGEST_MODE_UNSPECIFIED -> expected == null
                    ? ExpectedRevision.newBuilder().setNone(true).build() : expected;
            case INGEST_MODE_CREATE_ONLY -> {
                if (expected != null && !expected.getNone())
                    throw GrpcErrors.invalidArgument("CREATE_ONLY requires none=true when expected is supplied");
                yield ExpectedRevision.newBuilder().setNone(true).build();
            }
            case INGEST_MODE_EXPECTED_REVISION -> {
                if (expected == null || !expected.hasRevision())
                    throw GrpcErrors.invalidArgument("EXPECTED_REVISION requires a positive observed revision");
                yield expected;
            }
            case INGEST_MODE_UPSERT -> {
                if (expected != null) throw GrpcErrors.invalidArgument("UPSERT mode resolves the current revision; do not also set expected");
                yield null;
            }
            default -> throw GrpcErrors.invalidArgument("Unknown intake mode");
        };
    }

    static void requireExpected(ExpectedRevision expected) {
        if ((!expected.hasNone() || !expected.getNone())
                && (!expected.hasRevision() || expected.getRevision() <= 0))
            throw GrpcErrors.invalidArgument("Expected revision must be none=true or a positive revision");
        if (!expected.getUnknownFields().asMap().isEmpty())
            throw GrpcErrors.invalidArgument("Unknown expected-revision fields");
    }

    DeleteDocumentResponse delete(IntakeScope scope, DeleteDocumentRequest request) {
        var address = request.getAddress();
        if (!address.getAccountId().equals(scope.accountId())
                || !address.getGraphId().equals(IntakeGrpcService.INTAKE_GRAPH_PREFIX + scope.accountId())
                || !scope.allowsDatasource(address.getGraphAddressId()))
            throw GrpcErrors.permissionDenied("Delete address is outside the intake key's scope");
        if (address.getDocId().isBlank() || address.getGraphAddressId().isBlank()
                || !request.getExpected().hasRevision())
            throw GrpcErrors.invalidArgument("Delete requires a document, datasource and last observed revision");
        requireExpected(request.getExpected());
        // No conflict retry: a delayed delete must never target a newer revision.
        return documents.deleteDocument(request);
    }

    /** RESOURCE_EXHAUSTED when {@code sizeBytes} breaches the service or per-key cap. */
    void requirePayloadWithinCaps(IntakeScope scope, long sizeBytes) {
        if (sizeBytes > maxPayloadBytes) {
            throw GrpcErrors.resourceExhausted(
                    "payload of " + sizeBytes + " bytes exceeds the service cap of "
                            + maxPayloadBytes + " bytes; use the HTTP upload lane for bulk payloads");
        }
        if (!scope.allowsPayloadSize(sizeBytes)) {
            throw GrpcErrors.resourceExhausted(
                    "payload of " + sizeBytes + " bytes exceeds the API key's cap of "
                            + scope.maxPayloadBytes() + " bytes");
        }
    }

    /** PERMISSION_DENIED when the declared type falls outside a content-type-restricted key. */
    static void requireDeclaredMimeTypeWithinScope(IntakeScope scope, String mimeType) {
        if (!scope.allowsMimeType(blankToNull(mimeType))) {
            throw GrpcErrors.permissionDenied(
                    "mime_type '" + mimeType + "' is outside the API key's content-type restrictions");
        }
    }

    static String blankToNull(String value) {
        return value == null || value.isBlank() ? null : value;
    }

    /**
     * Adds read+write ACL rules for the configured pipeline step writers to a
     * created document, so a credential-bound step save (the parse coordinator)
     * passes the mandatory document write check under its writer grant
     * (requirements §3, W1). A rule the caller already named is left alone.
     */
    private Document withStepWriters(Document document) {
        if (stepWriters.isEmpty()) {
            return document;
        }
        var ownership = document.getOwnership();
        var security = ownership.getSecurity();
        var updated = security.toBuilder();
        boolean changed = false;
        for (String writer : stepWriters) {
            boolean named = security.getPermissionsList().stream().anyMatch(rule ->
                    rule.getIdentityType().equalsIgnoreCase("service") && rule.getIdentity().equalsIgnoreCase(writer));
            if (named) {
                continue;
            }
            updated.addPermissions(ai.protomolt.proto.repo.v1.AccessRule.newBuilder()
                    .setIdentityType("service").setIdentity(writer)
                    .setAccess(ai.protomolt.proto.repo.v1.Access.ACCESS_READ));
            updated.addPermissions(ai.protomolt.proto.repo.v1.AccessRule.newBuilder()
                    .setIdentityType("service").setIdentity(writer)
                    .setAccess(ai.protomolt.proto.repo.v1.Access.ACCESS_WRITE));
            changed = true;
        }
        if (!changed) {
            return document;
        }
        return document.toBuilder()
                .setOwnership(ownership.toBuilder().setSecurity(updated.build()))
                .build();
    }

    static byte[] sha256(byte[] data) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(data);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }

    static String sha256Hex(byte[] data) {
        return HexFormat.of().formatHex(sha256(data));
    }

    /** A completed repo save plus the receipt's integrity fields. */
    record Saved(ai.protomolt.proto.repo.v1.NodeAddress address, long revision, boolean deduplicated,
                 String drive, String sha256, long payloadSize) {}

    private static Blob inlineBlob(Blob blob, String drive) {
        if (blob.hasStorageRef()) throw GrpcErrors.invalidArgument("Intake blobs must carry bytes, not storage references");
        if (blob.getContentCase() != Blob.ContentCase.DATA)
            throw GrpcErrors.invalidArgument("Intake blobs must carry inline data");
        return blob.toBuilder().setDriveId(drive).build();
    }

    private static PartWrite put(DocumentPart part) {
        return PartWrite.newBuilder().setSelector(PartSelector.newBuilder().setPart(part))
                .setAction(WriteAction.WRITE_ACTION_PUT).build();
    }

    /**
     * Scope-checked request targeting. Construction is the single place the
     * PERMISSION_DENIED narrowing rules run, shared by every lane.
     */
    record Targeting(String datasourceId, String drive) {

        static Targeting of(IntakeScope scope, String datasourceId, String driveOrNull) {
            if (datasourceId == null || datasourceId.isBlank()) {
                throw GrpcErrors.invalidArgument("datasource_id is required");
            }
            if (!scope.allowsDatasource(datasourceId)) {
                throw GrpcErrors.permissionDenied(
                        "datasource_id '" + datasourceId + "' is outside the API key's scope");
            }
            String drive =
                    driveOrNull == null || driveOrNull.isBlank()
                            ? IntakeGrpcService.DEFAULT_DRIVE
                            : driveOrNull;
            if (!scope.allowsDrive(drive)) {
                throw GrpcErrors.permissionDenied(
                        "drive '" + drive + "' is outside the API key's scope");
            }
            return new Targeting(datasourceId, drive);
        }
    }
}
