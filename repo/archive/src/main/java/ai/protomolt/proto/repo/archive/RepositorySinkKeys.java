package ai.protomolt.proto.repo.archive;

import ai.protomolt.proto.descriptors.MessageFingerprints;
import ai.protomolt.proto.repo.sink.v1.PublishRepositorySinkRequest;
import ai.protomolt.proto.repo.sink.v1.RepositoryRevisionSource;
import ai.protomolt.proto.repo.sink.v1.SinkDestination;
import ai.protomolt.proto.repo.sink.v1.SinkSource;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.WireFormat;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.UUID;

/**
 * Deterministic intent hashing and UUIDv5 operation identifier derivation for
 * {@code RepositorySinkService} publications.
 */
final class RepositorySinkKeys {
    /** Fixed UUIDv5 namespace for repository sink operation identifiers. */
    static final UUID NAMESPACE = UUID.fromString("4b7f82a1-91d4-4c6e-a3b5-8e1f0d54c818");

    private static final String PUBLISH_OPERATION_KIND = "repository-sink.publish";
    private static final String DOWNSTREAM_UPLOAD_KIND = "repository-sink.upload";
    private static final int METADATA_FIELD = SinkDestination.METADATA_FIELD_NUMBER;

    private RepositorySinkKeys() {}

    /**
     * Resolves and validates the owning account of {@code source}, refusing any mismatch with
     * {@code destination}.
     */
    static String resolveAndValidateAccount(SinkSource source, SinkDestination destination) {
        String sourceAccount = sourceAccountId(source);
        String destAccount = destination.getAddress().getAccountId();
        if (!sourceAccount.equals(destAccount)) {
            throw ArchiveErrors.invalid(
                    "source, destination.address.account_id",
                    "source account '"
                            + sourceAccount
                            + "' must match destination account '"
                            + destAccount
                            + "'");
        }
        return destAccount;
    }

    static String sourceAccountId(SinkSource source) {
        return switch (source.getKindCase()) {
            case PROCESSING_STATE -> source.getProcessingState().getAccountId();
            case REPOSITORY_REVISION -> repositorySourceAccountId(source.getRepositoryRevision());
            case KIND_NOT_SET -> throw ArchiveErrors.invalid("source", "must select one source arm");
        };
    }

    private static String repositorySourceAccountId(RepositoryRevisionSource repoSource) {
        return switch (repoSource.getTargetCase()) {
            case ARCHIVE_RENDITION -> repoSource.getArchiveRendition().getAddress().getAccountId();
            case DOCUMENT_PART -> repoSource.getDocumentPart().getAddress().getAccountId();
            case TARGET_NOT_SET -> throw ArchiveErrors.invalid(
                    "source.repository_revision", "must select archive_rendition or document_part");
        };
    }

    /**
     * Computes the canonical lowercase hex SHA-256 over {@code (source, conversion, destination)},
     * independent of {@code operation_id} and {@code async}.
     */
    static String intentSha256(PublishRepositorySinkRequest request) {
        SinkDestination clearedDest = request.getDestination().toBuilder().clearMetadata().build();
        PublishRepositorySinkRequest cleared = request.toBuilder()
                .clearOperationId()
                .clearAsync()
                .setDestination(clearedDest)
                .build();
        MessageDigest sha256 = digest("SHA-256");
        sha256.update(MessageFingerprints.deterministicBytes(cleared));
        List<String> keys = new ArrayList<>(request.getDestination().getMetadataMap().keySet());
        keys.sort(ArchiveOperationKeys::compareUtf8);
        for (String key : keys) {
            sha256.update(
                    metadataEntry(key, request.getDestination().getMetadataOrThrow(key)));
        }
        return HexFormat.of().formatHex(sha256.digest());
    }

    /**
     * Resolves the caller-supplied {@code operation_id}, or derives a deterministic UUIDv5 from
     * {@code (accountId, intentSha256)} when omitted.
     */
    static String resolveOperationId(
            PublishRepositorySinkRequest request, String accountId, String intentSha256) {
        if (!request.getOperationId().isEmpty()) {
            return request.getOperationId().trim();
        }
        return deriveUuidV5(PUBLISH_OPERATION_KIND, accountId, intentSha256).toString();
    }

    /**
     * Derives the deterministic downstream v2 {@code UploadDocument} / {@code SaveDocument}
     * operation UUID bound to {@code (accountId, sinkOperationId)} for lost-reply reconciliation.
     */
    static String downstreamUploadOperationId(String accountId, String sinkOperationId) {
        return deriveUuidV5(DOWNSTREAM_UPLOAD_KIND, accountId, sinkOperationId).toString();
    }

    static UUID deriveUuidV5(String kind, String accountId, String discriminator) {
        MessageDigest sha1 = digest("SHA-1");
        sha1.update(
                ByteBuffer.allocate(16)
                        .putLong(NAMESPACE.getMostSignificantBits())
                        .putLong(NAMESPACE.getLeastSignificantBits())
                        .array());
        updateLengthPrefixed(sha1, kind);
        updateLengthPrefixed(sha1, accountId);
        updateLengthPrefixed(sha1, discriminator);
        byte[] hash = sha1.digest();
        hash[6] = (byte) ((hash[6] & 0x0f) | 0x50);
        hash[8] = (byte) ((hash[8] & 0x3f) | 0x80);
        ByteBuffer bytes = ByteBuffer.wrap(hash, 0, 16);
        return new UUID(bytes.getLong(), bytes.getLong());
    }

    private static void updateLengthPrefixed(MessageDigest digest, String value) {
        byte[] utf8 = value.getBytes(StandardCharsets.UTF_8);
        digest.update(ByteBuffer.allocate(4).putInt(utf8.length).array());
        digest.update(utf8);
    }

    private static byte[] metadataEntry(String key, String value) {
        try {
            ByteArrayOutputStream entry = new ByteArrayOutputStream();
            CodedOutputStream entryOut = CodedOutputStream.newInstance(entry);
            entryOut.writeString(1, key);
            entryOut.writeString(2, value);
            entryOut.flush();
            ByteArrayOutputStream field = new ByteArrayOutputStream();
            CodedOutputStream out = CodedOutputStream.newInstance(field);
            out.writeTag(METADATA_FIELD, WireFormat.WIRETYPE_LENGTH_DELIMITED);
            out.writeByteArrayNoTag(entry.toByteArray());
            out.flush();
            return field.toByteArray();
        } catch (IOException impossible) {
            throw new IllegalStateException("In-memory metadata encoding failed", impossible);
        }
    }

    private static MessageDigest digest(String algorithm) {
        try {
            return MessageDigest.getInstance(algorithm);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(algorithm + " unavailable", impossible);
        }
    }
}
