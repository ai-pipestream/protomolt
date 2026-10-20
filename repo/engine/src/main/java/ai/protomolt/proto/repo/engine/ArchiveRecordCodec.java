package ai.protomolt.proto.repo.engine;

import ai.protomolt.proto.asset.v1.Classification;
import ai.protomolt.proto.repo.archive.v1.Archive;
import ai.protomolt.proto.repo.archive.v1.EntryAddress;
import ai.protomolt.proto.repo.archive.v1.EntryInfo;
import ai.protomolt.proto.repo.archive.v1.VersioningPolicy;
import ai.protomolt.proto.repo.container.archive.ArchiveEntryRecord;
import ai.protomolt.proto.repo.container.archive.ArchiveRecord;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.util.Timestamps;

import java.util.Map;

/**
 * Codecs between the archive's ledger rows and its protobuf messages: the
 * stored metadata-map JSON column, and the {@link Archive} and
 * {@link EntryInfo} views of archive and entry rows. The stored forms are
 * read and written exactly as the ledger holds them.
 */
final class ArchiveRecordCodec {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final TypeReference<Map<String, String>> STRING_MAP = new TypeReference<>() {
    };

    private ArchiveRecordCodec() {
    }

    /** The archive message of a stored archive row. */
    static Archive toProto(ArchiveRecord record) {
        Archive.Builder archive = Archive.newBuilder()
                .setName(record.name)
                .setAccountId(record.accountId)
                .setDriveName(record.driveName)
                .setVersioning(record.retainsVersions()
                        ? VersioningPolicy.VERSIONING_POLICY_RETAINED
                        : VersioningPolicy.VERSIONING_POLICY_NONE)
                .setCreatedAt(Timestamps.fromMillis(record.createdAt.toEpochMilli()));
        if (record.description != null) {
            archive.setDescription(record.description);
        }
        archive.putAllMetadata(jsonToMap(record.metadata));
        return archive.build();
    }

    /** The entry message of a stored entry row; absent columns stay unset. */
    static EntryInfo toProto(ArchiveEntryRecord record) {
        EntryInfo.Builder info = EntryInfo.newBuilder()
                .setAddress(EntryAddress.newBuilder()
                        .setAccountId(record.accountId)
                        .setArchive(record.archive)
                        .setEntryId(record.entryId))
                .setEntryUuid(record.entryUuid.toString())
                .setCurrentVersion(record.currentVersion)
                .setCreatedAt(Timestamps.fromMillis(record.createdAt.toEpochMilli()))
                .setUpdatedAt(Timestamps.fromMillis(record.updatedAt.toEpochMilli()));
        if (record.title != null) {
            info.setTitle(record.title);
        }
        if (record.filename != null) {
            info.setFilename(record.filename);
        }
        if (record.contentType != null) {
            info.setContentType(record.contentType);
        }
        if (record.sourceUri != null) {
            info.setSourceUri(record.sourceUri);
        }
        if (record.sourceModifiedAt != null) {
            info.setSourceModifiedAt(Timestamps.fromMillis(record.sourceModifiedAt.toEpochMilli()));
        }
        Classification classification = ArchiveClassifications.fromJson(record.classification);
        if (classification != null) {
            info.setClassification(classification);
        }
        info.putAllMetadata(jsonToMap(record.metadata));
        return info.build();
    }

    /**
     * The stored metadata column of a map; null for an empty map, which the
     * column stores as absent.
     */
    static String mapToJson(Map<String, String> map) {
        if (map.isEmpty()) {
            return null;
        }
        try {
            return MAPPER.writeValueAsString(map);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("metadata map does not print as JSON", e);
        }
    }

    /**
     * The map a stored metadata column holds; empty for an absent or blank column.
     *
     * @throws IllegalStateException when the stored column does not parse as a
     *         string-to-string JSON object
     */
    static Map<String, String> jsonToMap(String json) {
        if (json == null || json.isBlank()) {
            return Map.of();
        }
        try {
            return MAPPER.readValue(json, STRING_MAP);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("stored metadata does not parse", e);
        }
    }
}
