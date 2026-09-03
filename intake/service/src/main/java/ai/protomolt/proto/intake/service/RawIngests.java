package ai.protomolt.proto.intake.service;

import ai.protomolt.proto.intake.service.identity.IntakeScope;
import ai.protomolt.proto.repo.v1.Blob;
import ai.protomolt.proto.repo.v1.BlobBag;
import ai.protomolt.proto.repo.v1.ChecksumType;
import ai.protomolt.proto.repo.v1.DocIdDerivation;
import ai.protomolt.proto.repo.v1.DocIdDerivationMethod;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.DocumentServiceGrpc;
import ai.protomolt.proto.repo.v1.OwnershipContext;
import ai.protomolt.proto.repo.v1.SaveDocumentRequest;
import ai.protomolt.proto.repo.v1.SaveDocumentResponse;
import ai.protomolt.proto.repo.v1.SearchMetadata;
import ai.protomolt.proto.repo.v1.WriteProvenance;
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

    /**
     * @param documents the repo-service document stub every save goes through
     * @param maxPayloadBytes the service-wide payload cap for the in-memory
     *        lanes; must be positive
     */
    RawIngests(DocumentServiceGrpc.DocumentServiceBlockingStub documents, long maxPayloadBytes) {
        if (documents == null) {
            throw new IllegalArgumentException("documents stub must not be null");
        }
        if (maxPayloadBytes <= 0) {
            throw new IllegalArgumentException("maxPayloadBytes must be positive");
        }
        this.documents = documents;
        this.maxPayloadBytes = maxPayloadBytes;
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
            Map<String, String> metadata) {
        requirePayloadWithinCaps(scope, data.size());
        requireDeclaredMimeTypeWithinScope(scope, mimeType);
        byte[] digest = sha256(data.toByteArray());
        String sha256 = HexFormat.of().formatHex(digest);
        String docId = UUID.nameUUIDFromBytes(digest).toString();

        Blob.Builder blob =
                Blob.newBuilder()
                        .setBlobId(docId)
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
        return save(scope, targeting, document, metadata, sha256, data.size());
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
            long payloadSize) {
        SaveDocumentRequest request =
                SaveDocumentRequest.newBuilder()
                        .setDocument(document)
                        .setDrive(targeting.drive())
                        .setConnectorId(IntakeGrpcService.CONNECTOR_ID)
                        .setUseDatasourceId(true)
                        .setGraphId(IntakeGrpcService.INTAKE_GRAPH_PREFIX + scope.accountId())
                        .setWrittenBy(
                                WriteProvenance.newBuilder().setModuleId(IntakeGrpcService.CONNECTOR_ID))
                        .putAllMetadata(metadata)
                        .build();
        SaveDocumentResponse response = documents.saveDocument(request);
        return new Saved(response, sha256, payloadSize);
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
    record Saved(SaveDocumentResponse response, String sha256, long payloadSize) {}

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
