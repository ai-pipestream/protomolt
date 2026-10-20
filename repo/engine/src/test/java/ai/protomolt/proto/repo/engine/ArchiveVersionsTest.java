package ai.protomolt.proto.repo.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.protomolt.proto.asset.bridge.Bridges;
import ai.protomolt.proto.asset.v1.BridgeKind;
import ai.protomolt.proto.repo.archive.v1.EntryAddress;
import ai.protomolt.proto.repo.archive.v1.RenditionDescriptor;
import ai.protomolt.proto.repo.archive.v1.RenditionManifestEntry;
import ai.protomolt.proto.repo.archive.v1.RenditionState;
import ai.protomolt.proto.repo.archive.v1.VersionManifest;
import ai.protomolt.proto.repo.archive.v1.WriteAttribution;
import ai.protomolt.proto.repo.container.archive.ArchiveEntryRecord;
import ai.protomolt.proto.repo.container.archive.ArchiveIds;
import ai.protomolt.proto.repo.container.archive.ArchiveLedger.StatsDelta;
import ai.protomolt.proto.repo.container.archive.ArchiveManifests;
import ai.protomolt.proto.repo.container.archive.ArchiveVersionRecord;
import ai.protomolt.proto.repo.container.ledger.DriveRecord;
import ai.protomolt.proto.repo.spi.RepositoryException;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.Timestamps;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * Version calculations decode retained manifests exactly as stored, compute ownership and
 * statistics deltas from object keys, and build the entry a write produces without storage.
 */
class ArchiveVersionsTest {

    private static final EntryAddress ADDRESS = EntryAddress.newBuilder()
            .setAccountId("acct").setArchive("docs").setEntryId("e1").build();
    private static final UUID ENTRY = ArchiveIds.entryUuid(ADDRESS);
    private static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");

    /** A manifest as rows written before storage bindings and metadata snapshots existed hold it. */
    private static final String LEGACY_MANIFEST = "{\"address\":{\"accountId\":\"acct\",\"archive\":\"docs\","
            + "\"entryId\":\"e1\"},\"version\":\"2\",\"rootChecksum\":\"" + "cd".repeat(32) + "\","
            + "\"totalBytes\":\"3\",\"createdAt\":\"2026-01-01T00:00:00Z\",\"renditions\":[{\"rendition\":"
            + "{\"name\":\"original\"},\"state\":\"RENDITION_STATE_PRESENT\",\"sizeBytes\":\"3\",\"sha256\":\""
            + "ab".repeat(32) + "\",\"objectKey\":\"k/original\",\"writtenAt\":\"2026-01-01T00:00:00Z\"}]}";

    private static RenditionManifestEntry present(String name, String key, long size) {
        return RenditionManifestEntry.newBuilder()
                .setRendition(RenditionDescriptor.newBuilder().setName(name))
                .setState(RenditionState.RENDITION_STATE_PRESENT)
                .setSizeBytes(size)
                .setSha256(key)
                .setObjectKey(key)
                .build();
    }

    private static ArchiveVersionRecord row(long version, String manifestJson) {
        ArchiveVersionRecord row = new ArchiveVersionRecord();
        row.entryUuid = ENTRY;
        row.version = version;
        row.manifest = manifestJson;
        row.rootChecksum = "root-" + version;
        return row;
    }

    private static ArchiveVersionRecord row(long version, RenditionManifestEntry... renditions) {
        return row(version, ArchiveManifests.toJson(VersionManifest.newBuilder()
                .setAddress(ADDRESS).setVersion(version).addAllRenditions(List.of(renditions)).build()));
    }

    @Test
    void legacyManifestDecodesToTheSameMessageAndReEncodesByteForByte() {
        VersionManifest expected = VersionManifest.newBuilder()
                .setAddress(ADDRESS)
                .setVersion(2)
                .setRootChecksum("cd".repeat(32))
                .setTotalBytes(3)
                .setCreatedAt(Timestamps.fromMillis(NOW.toEpochMilli()))
                .addRenditions(RenditionManifestEntry.newBuilder()
                        .setRendition(RenditionDescriptor.newBuilder().setName("original"))
                        .setState(RenditionState.RENDITION_STATE_PRESENT)
                        .setSizeBytes(3)
                        .setSha256("ab".repeat(32))
                        .setObjectKey("k/original")
                        .setWrittenAt(Timestamps.fromMillis(NOW.toEpochMilli())))
                .build();
        List<ArchiveVersionRecord> rows = List.of(row(1, present("original", "k/old", 1)),
                row(2, LEGACY_MANIFEST));

        assertThat(ArchiveVersions.manifestOf(rows, 2)).isEqualTo(expected);
        assertThat(ArchiveVersions.manifests(rows).get(1)).isEqualTo(expected);
        assertThat(ArchiveVersions.rootChecksumOf(rows, 2)).isEqualTo("root-2");
        assertThat(ArchiveVersions.versionRow(ENTRY, 2, expected, "r", 3, NOW).manifest)
                .isEqualTo(LEGACY_MANIFEST);
    }

    @Test
    void aVersionWithNoRetainedRowIsRefusedNamingTheVersion() {
        List<ArchiveVersionRecord> rows = List.of(row(1, present("original", "k", 1)));

        assertThatThrownBy(() -> ArchiveVersions.manifestOf(rows, 2))
                .isInstanceOf(RepositoryException.class)
                .hasMessage("the entry's current version 2 has no retained manifest");
    }

    @Test
    void malformedStoredManifestFailsLoudlyWithItsCause() {
        List<ArchiveVersionRecord> rows = List.of(row(1, "{\"renditions\":\"not a list\"}"));

        assertThatThrownBy(() -> ArchiveVersions.manifestOf(rows, 1))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("stored manifest does not parse")
                .hasCauseInstanceOf(InvalidProtocolBufferException.class);
    }

    @Test
    void ownershipDropsOnlyTheDroppedVersionAndAddsTheNewManifest() {
        List<ArchiveVersionRecord> retained = List.of(
                row(1, present("original", "k1", 10)),
                row(2, present("original", "k2", 20)));
        VersionManifest added = VersionManifest.newBuilder().setAddress(ADDRESS).setVersion(3)
                .addRenditions(present("original", "k3", 30)).build();

        assertThat(ArchiveVersions.afterOwnership(retained, 1, added)).containsOnlyKeys("k2", "k3");
        assertThat(ArchiveVersions.afterOwnership(retained, 0, added)).containsOnlyKeys("k1", "k2", "k3");
    }

    @Test
    void deltaCountsOnlyObjectsThatEnterOrLeaveOwnership() {
        Map<String, RenditionManifestEntry> before = Map.of(
                "k1", present("original", "k1", 10),
                "k2", present("text", "k2", 5));
        Map<String, RenditionManifestEntry> after = Map.of(
                "k2", present("text", "k2", 5),
                "k3", present("original", "k3", 7),
                "k4", present("page", "k4", 4));

        StatsDelta delta = ArchiveVersions.delta(1, 0, before, after, -6);

        assertThat(delta).isEqualTo(new StatsDelta(1, 0, 1, -6,
                Map.of("original", 0L, "page", 1L), Map.of("original", -3L, "page", 4L)));
        assertThat(ArchiveVersions.delta(0, 1, before, before, 0))
                .isEqualTo(new StatsDelta(0, 1, 0, 0, Map.of(), Map.of()));
    }

    @Test
    void writtenEntryIsEmptyForNoBytesAndPresentUnderAFreshKeyOtherwise() {
        DriveRecord drive = new DriveRecord();
        drive.prefix = "tenant/";
        RenditionDescriptor descriptor = RenditionDescriptor.newBuilder().setName("original").build();
        WriteAttribution blank = WriteAttribution.getDefaultInstance();
        WriteAttribution module = WriteAttribution.newBuilder().setModule("parser").build();

        RenditionManifestEntry empty = ArchiveVersions.writtenEntry(descriptor, new byte[0], drive,
                ADDRESS, ENTRY, blank, NOW);
        assertThat(empty.getState()).isEqualTo(RenditionState.RENDITION_STATE_EMPTY);
        assertThat(empty.getObjectKey()).isEmpty();
        assertThat(empty.hasWrittenBy()).isFalse();
        assertThat(empty.getWrittenAt()).isEqualTo(Timestamps.fromMillis(NOW.toEpochMilli()));

        byte[] data = "abc".getBytes(StandardCharsets.UTF_8);
        RenditionManifestEntry first = ArchiveVersions.writtenEntry(descriptor, data, drive,
                ADDRESS, ENTRY, module, NOW);
        RenditionManifestEntry second = ArchiveVersions.writtenEntry(descriptor, data, drive,
                ADDRESS, ENTRY, null, NOW);
        assertThat(first.getState()).isEqualTo(RenditionState.RENDITION_STATE_PRESENT);
        assertThat(first.getSizeBytes()).isEqualTo(3);
        assertThat(first.getSha256()).isEqualTo(ArchiveManifests.sha256Hex(data));
        assertThat(first.getObjectKey())
                .startsWith("tenant/archive/acct/docs/" + ENTRY + "/original/writes/")
                .endsWith("/" + first.getSha256());
        assertThat(first.getWrittenBy()).isEqualTo(module);
        assertThat(second.hasWrittenBy()).isFalse();
        assertThat(second.getObjectKey()).isNotEqualTo(first.getObjectKey());
    }

    @Test
    void derivedDescriptorsCarryTheBridgeShapeAndContentTypeFallsBackToOctetStream() {
        RenditionDescriptor text = ArchiveVersions.derivedDescriptor(BridgeKind.BRIDGE_KIND_DOCUMENT_TEXT);
        assertThat(text.getName()).isEqualTo(Bridges.renditionName(BridgeKind.BRIDGE_KIND_DOCUMENT_TEXT));
        assertThat(text.getMediaType()).isEqualTo(Bridges.mediaType(BridgeKind.BRIDGE_KIND_DOCUMENT_TEXT));
        assertThat(text.getSchemaSubject())
                .isEqualTo(Bridges.schemaSubject(BridgeKind.BRIDGE_KIND_DOCUMENT_TEXT));

        assertThat(ArchiveVersions.contentTypeOf(RenditionDescriptor.newBuilder().setName("a").build()))
                .isEqualTo("application/octet-stream");
        assertThat(ArchiveVersions.contentTypeOf(RenditionDescriptor.newBuilder().setName("a")
                .setMediaType("text/plain").build())).isEqualTo("text/plain");
    }

    @Test
    void manifestRefusesAnEntryRowThatIsNotTheVersionBeingPublished() {
        ArchiveEntryRecord entry = new ArchiveEntryRecord();
        entry.entryUuid = ENTRY;
        entry.accountId = "acct";
        entry.archive = "docs";
        entry.entryId = "e1";
        entry.currentVersion = 4;
        entry.createdAt = NOW;
        entry.updatedAt = NOW;
        List<RenditionManifestEntry> ordered = List.of(present("original", "k", 1));

        VersionManifest manifest = ArchiveVersions.manifestProto(ADDRESS, 4, "r", 1, ordered, NOW, entry);
        assertThat(manifest.getMetadataSnapshot().getAddress()).isEqualTo(ADDRESS);
        assertThat(manifest.getMetadataSnapshot().getCurrentVersion()).isEqualTo(4);
        assertThat(manifest.getRenditionsList()).isEqualTo(ordered);

        assertThatThrownBy(() -> ArchiveVersions.manifestProto(ADDRESS, 5, "r", 1, ordered, NOW, entry))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Archive version metadata does not match its entry identity");
        EntryAddress other = ADDRESS.toBuilder().setEntryId("e2").build();
        assertThatThrownBy(() -> ArchiveVersions.manifestProto(other, 4, "r", 1, ordered, NOW, entry))
                .isInstanceOf(IllegalStateException.class);
    }
}
