package ai.protomolt.proto.repo.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.protomolt.proto.repo.archive.v1.Archive;
import ai.protomolt.proto.repo.archive.v1.EntryInfo;
import ai.protomolt.proto.repo.archive.v1.VersioningPolicy;
import ai.protomolt.proto.repo.container.archive.ArchiveEntryRecord;
import ai.protomolt.proto.repo.container.archive.ArchiveRecord;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.protobuf.util.Timestamps;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * The metadata column round-trips exactly, absent columns stay absent in the messages, and a
 * stored column that does not parse fails with its cause rather than reading as empty.
 */
class ArchiveRecordCodecTest {

    private static final Instant CREATED = Instant.parse("2026-01-01T00:00:00Z");
    private static final Instant UPDATED = Instant.parse("2026-01-02T00:00:00Z");

    @Test
    void metadataColumnStoresAnEmptyMapAsAbsentAndRoundTripsTheRest() {
        assertThat(ArchiveRecordCodec.mapToJson(Map.of())).isNull();
        assertThat(ArchiveRecordCodec.jsonToMap(null)).isEmpty();
        assertThat(ArchiveRecordCodec.jsonToMap(" ")).isEmpty();

        Map<String, String> metadata = new LinkedHashMap<>();
        metadata.put("team", "search");
        metadata.put("quote", "a \"b\" é");
        String stored = ArchiveRecordCodec.mapToJson(metadata);
        assertThat(stored).isEqualTo("{\"team\":\"search\",\"quote\":\"a \\\"b\\\" é\"}");
        assertThat(ArchiveRecordCodec.jsonToMap(stored)).isEqualTo(metadata);
    }

    @ParameterizedTest
    @ValueSource(strings = {"not json", "[\"a\"]", "{\"a\":{\"b\":\"c\"}}", "{\"a\":\"b\""})
    void malformedStoredMetadataFailsWithItsCause(String stored) {
        assertThatThrownBy(() -> ArchiveRecordCodec.jsonToMap(stored))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("stored metadata does not parse")
                .hasCauseInstanceOf(JsonProcessingException.class);
    }

    @Test
    void archiveRowBecomesItsMessageWithAbsentColumnsUnset() {
        ArchiveRecord record = new ArchiveRecord();
        record.accountId = "acct";
        record.name = "docs";
        record.driveName = "main";
        record.versioning = ArchiveRecord.VERSIONING_NONE;
        record.createdAt = CREATED;

        Archive bare = ArchiveRecordCodec.toProto(record);
        assertThat(bare).isEqualTo(Archive.newBuilder()
                .setAccountId("acct").setName("docs").setDriveName("main")
                .setVersioning(VersioningPolicy.VERSIONING_POLICY_NONE)
                .setCreatedAt(Timestamps.fromMillis(CREATED.toEpochMilli()))
                .build());

        record.versioning = ArchiveRecord.VERSIONING_RETAINED;
        record.description = "contracts";
        record.metadata = "{\"k\":\"v\"}";
        Archive full = ArchiveRecordCodec.toProto(record);
        assertThat(full.getVersioning()).isEqualTo(VersioningPolicy.VERSIONING_POLICY_RETAINED);
        assertThat(full.getDescription()).isEqualTo("contracts");
        assertThat(full.getMetadataMap()).isEqualTo(Map.of("k", "v"));
    }

    @Test
    void entryRowBecomesItsMessageWithAbsentColumnsUnset() {
        ArchiveEntryRecord record = new ArchiveEntryRecord();
        record.entryUuid = UUID.fromString("00000000-0000-0000-0000-00000000000e");
        record.accountId = "acct";
        record.archive = "docs";
        record.entryId = "e1";
        record.currentVersion = 3;
        record.createdAt = CREATED;
        record.updatedAt = UPDATED;

        EntryInfo bare = ArchiveRecordCodec.toProto(record);
        assertThat(bare.getAddress().getEntryId()).isEqualTo("e1");
        assertThat(bare.getEntryUuid()).isEqualTo(record.entryUuid.toString());
        assertThat(bare.getCurrentVersion()).isEqualTo(3);
        assertThat(bare.getUpdatedAt()).isEqualTo(Timestamps.fromMillis(UPDATED.toEpochMilli()));
        assertThat(bare.getTitle()).isEmpty();
        assertThat(bare.hasSourceModifiedAt()).isFalse();
        assertThat(bare.hasClassification()).isFalse();
        assertThat(bare.getMetadataMap()).isEmpty();

        record.title = "Title";
        record.filename = "a.pdf";
        record.contentType = "application/pdf";
        record.sourceUri = "s3://bucket/a.pdf";
        record.sourceModifiedAt = CREATED;
        record.metadata = "{\"k\":\"v\"}";
        EntryInfo full = ArchiveRecordCodec.toProto(record);
        assertThat(full.getTitle()).isEqualTo("Title");
        assertThat(full.getFilename()).isEqualTo("a.pdf");
        assertThat(full.getContentType()).isEqualTo("application/pdf");
        assertThat(full.getSourceUri()).isEqualTo("s3://bucket/a.pdf");
        assertThat(full.getSourceModifiedAt()).isEqualTo(Timestamps.fromMillis(CREATED.toEpochMilli()));
        assertThat(full.getMetadataMap()).isEqualTo(Map.of("k", "v"));
    }

    @Test
    void entryRowWithUnparseableMetadataIsRefusedNotEmptied() {
        ArchiveEntryRecord record = new ArchiveEntryRecord();
        record.entryUuid = UUID.fromString("00000000-0000-0000-0000-00000000000e");
        record.accountId = "acct";
        record.archive = "docs";
        record.entryId = "e1";
        record.createdAt = CREATED;
        record.updatedAt = UPDATED;
        record.metadata = "{";

        assertThatThrownBy(() -> ArchiveRecordCodec.toProto(record))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("stored metadata does not parse");
    }
}
