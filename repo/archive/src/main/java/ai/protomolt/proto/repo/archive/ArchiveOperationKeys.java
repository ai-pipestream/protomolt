package ai.protomolt.proto.repo.archive;

import ai.protomolt.proto.asset.v1.Classification;
import ai.protomolt.proto.descriptors.MessageFingerprints;
import ai.protomolt.proto.repo.archive.v1.ArchiveOperationKey;
import ai.protomolt.proto.repo.archive.v1.BridgeEntryRequest;
import ai.protomolt.proto.repo.archive.v1.ClassifyEntryRequest;
import ai.protomolt.proto.repo.archive.v1.EntryAddress;
import ai.protomolt.proto.repo.archive.v1.PutEntryRequest;
import ai.protomolt.proto.repo.archive.v1.RenditionContent;
import ai.protomolt.proto.repo.archive.v1.UploadRenditionHeader;
import ai.protomolt.proto.repo.v1.Blob;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.WireFormat;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import java.util.UUID;

/**
 * Deterministic operation IDs for archive writes (docs/design/archive.md,
 * section 5). A write with a blank {@code operation_id} gets
 * {@code UUIDv5(NAMESPACE, encode(ArchiveOperationKey))} per RFC 9562: SHA-1 over
 * the namespace's 16 bytes followed by the key's binary encoding. The key holds
 * the caller's requested {@code expected_version}, never a revision read later,
 * so a retry of the same request derives the same ID.
 */
public final class ArchiveOperationKeys {

    /** The fixed UUIDv5 namespace of archive operation IDs, never configuration. */
    public static final UUID NAMESPACE = UUID.fromString("6e947da6-c6b8-49a7-9c65-fdf26dbd03e2");

    /** The {@code operation_kind} of a {@code PutEntry}. */
    public static final String PUT_ENTRY = "archive.put-entry";

    /** The {@code operation_kind} of an {@code UploadRendition}. */
    public static final String UPLOAD_RENDITION = "archive.upload-rendition";

    /** The {@code operation_kind} of a {@code ClassifyEntry}. */
    public static final String CLASSIFY_ENTRY = "archive.classify-entry";

    /** The {@code operation_kind} of a {@code BridgeEntry}. */
    public static final String BRIDGE_ENTRY = "archive.bridge-entry";

    private static final int METADATA_FIELD = PutEntryRequest.METADATA_FIELD_NUMBER;

    private ArchiveOperationKeys() {
    }

    /** The UUIDv5 of {@code key} under {@link #NAMESPACE}. */
    public static UUID derive(ArchiveOperationKey key) {
        MessageDigest sha1 = digest("SHA-1");
        sha1.update(ByteBuffer.allocate(16)
                .putLong(NAMESPACE.getMostSignificantBits())
                .putLong(NAMESPACE.getLeastSignificantBits())
                .array());
        sha1.update(MessageFingerprints.deterministicBytes(key));
        byte[] hash = sha1.digest();
        hash[6] = (byte) ((hash[6] & 0x0f) | 0x50);
        hash[8] = (byte) ((hash[8] & 0x3f) | 0x80);
        ByteBuffer bytes = ByteBuffer.wrap(hash, 0, 16);
        return new UUID(bytes.getLong(), bytes.getLong());
    }

    /** The key of a {@code PutEntry} with the given content digest. */
    public static ArchiveOperationKey putEntryKey(PutEntryRequest request, String contentSha256) {
        return key(PUT_ENTRY, request.getAddress(), request.getExpectedVersion(), contentSha256);
    }

    /** The key of an {@code UploadRendition}: the header's requested {@code expected_version}, 0 for last write wins. */
    public static ArchiveOperationKey uploadKey(UploadRenditionHeader header, String contentSha256) {
        return key(UPLOAD_RENDITION, header.getAddress(), header.getExpectedVersion(), contentSha256);
    }

    /** The key of a {@code ClassifyEntry} against {@code expectedVersion}. */
    public static ArchiveOperationKey classifyEntryKey(
            ClassifyEntryRequest request, long expectedVersion, String contentSha256) {
        return key(CLASSIFY_ENTRY, request.getAddress(), expectedVersion, contentSha256);
    }

    /** The key of a {@code BridgeEntry} against {@code expectedVersion}. */
    public static ArchiveOperationKey bridgeEntryKey(
            BridgeEntryRequest request, long expectedVersion, String contentSha256) {
        return key(BRIDGE_ENTRY, request.getAddress(), expectedVersion, contentSha256);
    }

    private static ArchiveOperationKey key(
            String kind, EntryAddress address, long expectedVersion, String contentSha256) {
        return ArchiveOperationKey.newBuilder()
                .setOperationKind(kind)
                .setAddress(address)
                .setExpectedVersion(expectedVersion)
                .setContentSha256(contentSha256)
                .build();
    }

    /**
     * The content digest of a {@code PutEntry}: SHA-256 of the request's
     * deterministic encoding with {@code operation_id} and
     * {@code expected_version} cleared and renditions sorted by
     * {@code (name, sub_key)} in code point order, followed by each metadata
     * entry as a field-99 map entry (key and value both written) in unsigned
     * UTF-8 key order. Map order in the protobuf runtime is not that order, so
     * the entries are encoded here rather than left to the serializer.
     */
    public static String putEntryContentSha256(PutEntryRequest request) {
        List<RenditionContent> renditions = new ArrayList<>(request.getRenditionsList());
        renditions.sort(Comparator
                .comparing((RenditionContent r) -> r.getRendition().getName(), ArchiveOperationKeys::compareUtf8)
                .thenComparing(r -> r.getRendition().getSubKey(), ArchiveOperationKeys::compareUtf8));
        PutEntryRequest cleared = request.toBuilder()
                .clearOperationId()
                .clearExpectedVersion()
                .clearRenditions()
                .addAllRenditions(renditions)
                .clearMetadata()
                .build();
        MessageDigest sha256 = digest("SHA-256");
        sha256.update(MessageFingerprints.deterministicBytes(cleared));
        List<String> keys = new ArrayList<>(request.getMetadataMap().keySet());
        keys.sort(ArchiveOperationKeys::compareUtf8);
        for (String key : keys) {
            sha256.update(metadataEntry(key, request.getMetadataOrThrow(key)));
        }
        return HexFormat.of().formatHex(sha256.digest());
    }

    /**
     * The content digest of an {@code UploadRendition}: SHA-256 of the header's
     * deterministic encoding with {@code operation_id} and {@code expected_version}
     * cleared, as for a {@code PutEntry}. The header keeps {@code expected_sha256},
     * because the ID must exist before any byte arrives.
     */
    public static String uploadContentSha256(UploadRenditionHeader header) {
        return HexFormat.of().formatHex(digest("SHA-256").digest(MessageFingerprints.deterministicBytes(
                header.toBuilder().clearOperationId().clearExpectedVersion().build())));
    }

    /** The operation ID of a {@code PutEntry}: the supplied one, or the derived one. */
    public static String operationId(PutEntryRequest request, String contentSha256) {
        if (!request.getOperationId().isEmpty()) {
            return request.getOperationId();
        }
        return derive(putEntryKey(request, contentSha256)).toString();
    }

    /**
     * The operation ID of an {@code UploadRendition}: the supplied one, or the
     * derived one when the header declares {@code expected_sha256}.
     *
     * @throws io.grpc.StatusRuntimeException {@code INVALID_ARGUMENT} naming both
     *         fields when both are blank: the content is unknown when the ID is
     *         needed, and a random ID is never the fallback
     */
    public static String operationId(UploadRenditionHeader header, String contentSha256) {
        if (!header.getOperationId().isEmpty()) {
            return header.getOperationId();
        }
        if (header.getExpectedSha256().isEmpty()) {
            throw ArchiveErrors.invalid("operation_id, expected_sha256",
                    "an upload with no operation_id must declare expected_sha256 so its ID can be derived");
        }
        return derive(uploadKey(header, contentSha256)).toString();
    }

    /**
     * The content digest of a {@code ClassifyEntry}: SHA-256 of the deterministic
     * encoding of the request and the resolved {@link Classification} (with
     * {@code classified_at} cleared).
     */
    public static String classifyEntryContentSha256(
            ClassifyEntryRequest request, Classification classification) {
        MessageDigest sha256 = digest("SHA-256");
        sha256.update(MessageFingerprints.deterministicBytes(request));
        sha256.update(MessageFingerprints.deterministicBytes(
                classification.toBuilder().clearClassifiedAt().build()));
        return HexFormat.of().formatHex(sha256.digest());
    }

    /** The deterministic operation ID of a {@code ClassifyEntry} save. */
    public static String classifyOperationId(
            ClassifyEntryRequest request, long expectedVersion, String contentSha256) {
        return derive(classifyEntryKey(request, expectedVersion, contentSha256)).toString();
    }

    /**
     * The content digest of a {@code BridgeEntry}: SHA-256 of the deterministic
     * encoding of the request and each derived {@link Blob}.
     */
    public static String bridgeEntryContentSha256(
            BridgeEntryRequest request, List<Blob> derivedBlobs) {
        MessageDigest sha256 = digest("SHA-256");
        sha256.update(MessageFingerprints.deterministicBytes(request));
        for (Blob blob : derivedBlobs) {
            sha256.update(MessageFingerprints.deterministicBytes(blob));
        }
        return HexFormat.of().formatHex(sha256.digest());
    }

    /** The deterministic operation ID of a {@code BridgeEntry} save. */
    public static String bridgeOperationId(
            BridgeEntryRequest request, long expectedVersion, String contentSha256) {
        return derive(bridgeEntryKey(request, expectedVersion, contentSha256)).toString();
    }

    private static byte[] metadataEntry(String key, String value) {
        try {
            var entry = new ByteArrayOutputStream();
            CodedOutputStream entryOut = CodedOutputStream.newInstance(entry);
            entryOut.writeString(1, key);
            entryOut.writeString(2, value);
            entryOut.flush();
            var field = new ByteArrayOutputStream();
            CodedOutputStream out = CodedOutputStream.newInstance(field);
            out.writeTag(METADATA_FIELD, WireFormat.WIRETYPE_LENGTH_DELIMITED);
            out.writeByteArrayNoTag(entry.toByteArray());
            out.flush();
            return field.toByteArray();
        } catch (IOException impossible) {
            throw new IllegalStateException("in-memory encoding failed", impossible);
        }
    }

    /** Unsigned UTF-8 byte order, which equals code point order. */
    static int compareUtf8(String left, String right) {
        int i = 0;
        int j = 0;
        while (i < left.length() && j < right.length()) {
            int x = left.codePointAt(i);
            int y = right.codePointAt(j);
            if (x != y) {
                return Integer.compare(x, y);
            }
            i += Character.charCount(x);
            j += Character.charCount(y);
        }
        return Integer.compare(left.length() - i, right.length() - j);
    }

    private static MessageDigest digest(String algorithm) {
        try {
            return MessageDigest.getInstance(algorithm);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(algorithm + " unavailable", impossible);
        }
    }
}
