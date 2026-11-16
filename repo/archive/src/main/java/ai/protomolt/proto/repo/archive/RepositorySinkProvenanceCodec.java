package ai.protomolt.proto.repo.archive;

import ai.protomolt.proto.descriptors.MessageFingerprints;
import ai.protomolt.proto.repo.sink.v1.RenditionProvenance;
import ai.protomolt.proto.repo.v1.Blob;
import ai.protomolt.proto.repo.v1.SearchMetadata;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Struct;
import com.google.protobuf.Timestamp;
import com.google.protobuf.Value;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Deterministic binary encoding of {@link RenditionProvenance} and retained protobuf descriptor
 * closures on archive {@link Blob} metadata and {@link SearchMetadata} entries.
 */
final class RepositorySinkProvenanceCodec {
    /** {@code Blob.metadata} struct field holding Base64-encoded binary {@link RenditionProvenance}. */
    static final String BLOB_PROVENANCE_FIELD = "archive.rendition_provenance";

    /** {@code Blob.metadata} struct field holding Base64-gzipped closed {@code FileDescriptorSet} bytes. */
    static final String BLOB_RETAINED_DESCRIPTORS_FIELD = "archive.retained_descriptors";

    /** {@code Blob.metadata} struct field holding the retained protobuf message full name. */
    static final String BLOB_RETAINED_MESSAGE_TYPE_FIELD = "archive.retained_message_type";

    /** {@code SearchMetadata.metadata} key holding Base64 binary {@link RenditionProvenance} for {@code parsed}. */
    static final String PARSED_PROVENANCE_METADATA_KEY = "archive.rendition_provenance.parsed";

    /** {@code SearchMetadata.metadata} key holding Base64-gzipped descriptors for {@code parsed}. */
    static final String PARSED_RETAINED_DESCRIPTORS_METADATA_KEY =
            "archive.retained_descriptors.parsed";

    /** {@code SearchMetadata.metadata} key holding the retained message full name for {@code parsed}. */
    static final String PARSED_RETAINED_MESSAGE_TYPE_METADATA_KEY =
            "archive.retained_message_type.parsed";

    static final String PROVENANCE_METADATA_PREFIX = "archive.rendition_provenance.";
    static final String RETAINED_DESCRIPTORS_METADATA_PREFIX = "archive.retained_descriptors.";
    static final String RETAINED_MESSAGE_TYPE_METADATA_PREFIX = "archive.retained_message_type.";

    private static final int GZIP_BUFFER_BYTES = 8192;

    private RepositorySinkProvenanceCodec() {}

    static String encodeProvenance(RenditionProvenance provenance) {
        return Base64.getEncoder()
                .encodeToString(MessageFingerprints.deterministicBytes(provenance));
    }

    static RenditionProvenance decodeProvenance(String encoded) {
        byte[] raw;
        try {
            raw = Base64.getDecoder().decode(encoded);
        } catch (IllegalArgumentException e) {
            throw ArchiveErrors.dataLoss("Stored rendition provenance is not valid Base64", e);
        }
        try {
            return RenditionProvenance.parseFrom(raw);
        } catch (InvalidProtocolBufferException e) {
            throw ArchiveErrors.dataLoss("Stored rendition provenance does not decode", e);
        }
    }

    static String encodeDescriptors(ByteString descriptorSetBytes) {
        ByteArrayOutputStream compressed = new ByteArrayOutputStream();
        try (GZIPOutputStream gzip = new GZIPOutputStream(compressed)) {
            descriptorSetBytes.writeTo(gzip);
        } catch (IOException impossible) {
            throw new IllegalStateException("In-memory GZIP compression failed", impossible);
        }
        return Base64.getEncoder().encodeToString(compressed.toByteArray());
    }

    static ByteString decodeDescriptors(String encoded, int maxBytes) {
        byte[] compressed;
        try {
            compressed = Base64.getDecoder().decode(encoded);
        } catch (IllegalArgumentException e) {
            throw ArchiveErrors.dataLoss("Stored retained descriptors are not valid Base64", e);
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] buffer = new byte[GZIP_BUFFER_BYTES];
        try (GZIPInputStream gzip = new GZIPInputStream(new ByteArrayInputStream(compressed))) {
            int read;
            int total = 0;
            while ((read = gzip.read(buffer)) >= 0) {
                if (read > maxBytes - total) {
                    throw ArchiveErrors.dataLoss(
                            "Stored retained descriptors exceed maximum size of "
                                    + maxBytes
                                    + " bytes",
                            null);
                }
                out.write(buffer, 0, read);
                total += read;
            }
        } catch (IOException e) {
            throw ArchiveErrors.dataLoss("Stored retained descriptors failed GZIP decoding", e);
        }
        return ByteString.copyFrom(out.toByteArray());
    }

    static Map<String, Value> blobMetadataFields(
            RenditionProvenance provenance, ByteString retainedDescriptors) {
        Map<String, Value> fields = new LinkedHashMap<>();
        fields.put(
                BLOB_PROVENANCE_FIELD,
                Value.newBuilder().setStringValue(encodeProvenance(provenance)).build());
        if (retainedDescriptors != null && !retainedDescriptors.isEmpty()) {
            fields.put(
                    BLOB_RETAINED_DESCRIPTORS_FIELD,
                    Value.newBuilder()
                            .setStringValue(encodeDescriptors(retainedDescriptors))
                            .build());
            fields.put(
                    BLOB_RETAINED_MESSAGE_TYPE_FIELD,
                    Value.newBuilder().setStringValue(provenance.getSchemaType()).build());
        }
        return Map.copyOf(fields);
    }

    static Map<String, String> parsedSearchMetadata(
            RenditionProvenance provenance, ByteString retainedDescriptors) {
        Map<String, String> entries = new LinkedHashMap<>();
        entries.put(PARSED_PROVENANCE_METADATA_KEY, encodeProvenance(provenance));
        if (retainedDescriptors != null && !retainedDescriptors.isEmpty()) {
            entries.put(
                    PARSED_RETAINED_DESCRIPTORS_METADATA_KEY,
                    encodeDescriptors(retainedDescriptors));
            entries.put(PARSED_RETAINED_MESSAGE_TYPE_METADATA_KEY, provenance.getSchemaType());
        }
        return Map.copyOf(entries);
    }

    static Optional<RenditionProvenance> readBlobProvenance(Blob blob) {
        if (!blob.hasMetadata()) {
            return Optional.empty();
        }
        Struct meta = blob.getMetadata();
        Value encoded = meta.getFieldsMap().get(BLOB_PROVENANCE_FIELD);
        if (encoded == null
                || encoded.getKindCase() != Value.KindCase.STRING_VALUE
                || encoded.getStringValue().isEmpty()) {
            return Optional.empty();
        }
        RenditionProvenance decoded = decodeProvenance(encoded.getStringValue());
        RenditionProvenance.Builder updated = decoded.toBuilder();
        if (blob.hasWritten() && blob.getWritten().getRevision() > 0) {
            updated.setDestinationVersion(blob.getWritten().getRevision());
            if (blob.getWritten().hasCommittedAt()) {
                updated.setCommittedAt(blob.getWritten().getCommittedAt());
            }
        }
        if (blob.getSizeBytes() >= 0) {
            updated.setBytesCommitted(blob.getSizeBytes());
        }
        return Optional.of(updated.build());
    }

    static Optional<RenditionProvenance> readParsedProvenance(
            SearchMetadata search, long writtenRevision, Timestamp writtenAt, long sizeBytes) {
        String encoded = search.getMetadataOrDefault(PARSED_PROVENANCE_METADATA_KEY, "");
        if (encoded.isEmpty()) {
            return Optional.empty();
        }
        RenditionProvenance.Builder updated = decodeProvenance(encoded).toBuilder();
        if (writtenRevision > 0) {
            updated.setDestinationVersion(writtenRevision);
        }
        if (writtenAt != null && (writtenAt.getSeconds() != 0 || writtenAt.getNanos() != 0)) {
            updated.setCommittedAt(writtenAt);
        }
        if (sizeBytes >= 0) {
            updated.setBytesCommitted(sizeBytes);
        }
        return Optional.of(updated.build());
    }

    static Optional<RetainedBlobSchema> readBlobRetainedSchema(Blob blob, int maxBytes) {
        if (!blob.hasMetadata()) {
            return Optional.empty();
        }
        Map<String, Value> fields = blob.getMetadata().getFieldsMap();
        Value descVal = fields.get(BLOB_RETAINED_DESCRIPTORS_FIELD);
        Value typeVal = fields.get(BLOB_RETAINED_MESSAGE_TYPE_FIELD);
        if (descVal == null
                || typeVal == null
                || descVal.getKindCase() != Value.KindCase.STRING_VALUE
                || typeVal.getKindCase() != Value.KindCase.STRING_VALUE
                || descVal.getStringValue().isEmpty()
                || typeVal.getStringValue().isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(
                new RetainedBlobSchema(
                        typeVal.getStringValue(),
                        decodeDescriptors(descVal.getStringValue(), maxBytes)));
    }

    static void preserveSystemSearchMetadata(
            Map<String, String> previousMetadata,
            SearchMetadata.Builder nextSearch,
            boolean clearParsedProvenance) {
        for (Map.Entry<String, String> entry : previousMetadata.entrySet()) {
            String key = entry.getKey();
            if (isSinkSearchMetadataKey(key)) {
                if (clearParsedProvenance && isParsedMetadataKey(key)) {
                    nextSearch.removeMetadata(key);
                } else {
                    nextSearch.putMetadata(key, entry.getValue());
                }
            }
        }
    }

    private static boolean isSinkSearchMetadataKey(String key) {
        return key.startsWith(PROVENANCE_METADATA_PREFIX)
                || key.startsWith(RETAINED_DESCRIPTORS_METADATA_PREFIX)
                || key.startsWith(RETAINED_MESSAGE_TYPE_METADATA_PREFIX);
    }

    private static boolean isParsedMetadataKey(String key) {
        return key.equals(PARSED_PROVENANCE_METADATA_KEY)
                || key.equals(PARSED_RETAINED_DESCRIPTORS_METADATA_KEY)
                || key.equals(PARSED_RETAINED_MESSAGE_TYPE_METADATA_KEY);
    }

    record RetainedBlobSchema(String messageType, ByteString descriptorSetBytes) {}
}
