package ai.pipestream.proto.acquire.pull;

import ai.pipestream.proto.repo.v1.Blob;
import ai.pipestream.proto.repo.v1.BlobBag;
import ai.pipestream.proto.repo.v1.ChecksumType;
import ai.pipestream.proto.repo.v1.DocIdDerivation;
import ai.pipestream.proto.repo.v1.DocIdDerivationMethod;
import ai.pipestream.proto.repo.v1.Document;
import ai.pipestream.proto.repo.v1.OwnershipContext;
import ai.pipestream.proto.repo.v1.SearchMetadata;
import com.google.protobuf.ByteString;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.UUID;

/**
 * The stable-identity document wrap shared by every pull connector. The doc id is a
 * deterministic name-based UUID over {@code connector NUL datasource NUL sourceKey}, so the
 * same source item always lands on the same document: an updated object or row re-saves its
 * own doc id instead of accumulating a duplicate, and downstream replace-by-identity (repo
 * save, search-door indexing) keeps exactly one live document per source item.
 */
public final class PullDocuments {

    private PullDocuments() {
    }

    /**
     * Wraps one source item into the intake-ready document: stable doc id, one inline blob with
     * its SHA-256 stamped, source mime type on the search metadata, the connector identity on
     * the ownership context, and a caller-provided doc-id derivation naming the source key.
     * Account and datasource ownership are deliberately left blank — the intake door stamps
     * them from the API key's scope, and whatever a caller supplies loses anyway.
     *
     * @param connectorId the pulling connector's identity, e.g. {@code s3-pull}
     * @param datasourceId the datasource the item belongs to
     * @param sourceKey the item's identity at the source, e.g. {@code s3://bucket/key}
     * @param data the payload bytes
     * @param filename the source filename, or blank
     * @param mimeType the source content type, or blank
     */
    public static Document document(String connectorId, String datasourceId, String sourceKey,
                                    ByteString data, String filename, String mimeType) {
        requireNonBlank(connectorId, "connectorId");
        requireNonBlank(datasourceId, "datasourceId");
        requireNonBlank(sourceKey, "sourceKey");
        if (data == null) {
            throw new IllegalArgumentException("data must not be null");
        }

        String docId = docId(connectorId, datasourceId, sourceKey);
        String sha256 = sha256Hex(data.toByteArray());
        Blob.Builder blob = Blob.newBuilder()
                .setBlobId(docId)
                .setData(data)
                .setSizeBytes(data.size())
                .setChecksum(sha256)
                .setChecksumType(ChecksumType.CHECKSUM_TYPE_SHA256);
        if (filename != null && !filename.isBlank()) {
            blob.setFilename(filename);
        }
        if (mimeType != null && !mimeType.isBlank()) {
            blob.setMimeType(mimeType);
        }
        SearchMetadata.Builder searchMetadata = SearchMetadata.newBuilder();
        if (mimeType != null && !mimeType.isBlank()) {
            searchMetadata.setSourceMimeType(mimeType);
        }
        return Document.newBuilder()
                .setDocId(docId)
                .setSearchMetadata(searchMetadata)
                .setBlobBag(BlobBag.newBuilder().setBlob(blob))
                .setOwnership(OwnershipContext.newBuilder().setConnectorId(connectorId))
                .setDocIdDerivation(DocIdDerivation.newBuilder()
                        .setMethod(DocIdDerivationMethod.DOC_ID_DERIVATION_METHOD_CALLER_PROVIDED)
                        .setSourceValue(sourceKey))
                .build();
    }

    /** The stable doc id for a source item; NUL separators keep field boundaries collision-free. */
    public static String docId(String connectorId, String datasourceId, String sourceKey) {
        String identity = connectorId + '\0' + datasourceId + '\0' + sourceKey;
        return UUID.nameUUIDFromBytes(identity.getBytes(StandardCharsets.UTF_8)).toString();
    }

    private static void requireNonBlank(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
    }

    private static String sha256Hex(byte[] data) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(data));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }
}
