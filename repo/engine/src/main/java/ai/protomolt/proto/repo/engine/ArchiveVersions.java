package ai.protomolt.proto.repo.engine;

import ai.protomolt.proto.asset.bridge.Bridges;
import ai.protomolt.proto.asset.v1.BridgeKind;
import ai.protomolt.proto.repo.archive.v1.EntryAddress;
import ai.protomolt.proto.repo.archive.v1.EntryInfo;
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
import com.google.protobuf.util.Timestamps;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static ai.protomolt.proto.repo.engine.RepositoryErrors.failedPrecondition;

/**
 * Rendition and version calculations: the manifest entry a write produces,
 * the retained manifests a version list decodes to, the object ownership
 * before and after a mutation, and the exact statistics delta between them.
 * Nothing here reads or writes storage; allocation, commit and retries stay
 * with {@link ArchiveOperations}.
 */
final class ArchiveVersions {

    private ArchiveVersions() {
    }

    /** Every retained row's manifest, in row order. */
    static List<VersionManifest> manifests(List<ArchiveVersionRecord> rows) {
        return rows.stream().map(row -> ArchiveManifests.fromJson(row.manifest)).toList();
    }

    /** The retained manifest of {@code version}, refused when no row holds it. */
    static VersionManifest manifestOf(List<ArchiveVersionRecord> rows, long version) {
        return rows.stream().filter(row -> row.version == version).findFirst()
                .map(row -> ArchiveManifests.fromJson(row.manifest))
                .orElseThrow(() -> failedPrecondition(
                        "the entry's current version " + version + " has no retained manifest"));
    }

    /** The stored root checksum of {@code version}; the row must be present. */
    static String rootChecksumOf(List<ArchiveVersionRecord> rows, long version) {
        return rows.stream().filter(row -> row.version == version).findFirst()
                .orElseThrow().rootChecksum;
    }

    /** The ownership set after a mutation: retained rows minus the dropped one, plus the new manifest. */
    static Map<String, RenditionManifestEntry> afterOwnership(
            List<ArchiveVersionRecord> retained, long dropVersion, VersionManifest added) {
        List<VersionManifest> after = new ArrayList<>();
        for (ArchiveVersionRecord row : retained) {
            if (row.version != dropVersion) {
                after.add(ArchiveManifests.fromJson(row.manifest));
            }
        }
        after.add(added);
        return ArchiveManifests.referencedObjects(after);
    }

    /** Exact counter deltas from the before/after ownership sets. */
    static StatsDelta delta(long entries, long versions,
                            Map<String, RenditionManifestEntry> before,
                            Map<String, RenditionManifestEntry> after,
                            long currentBytesDelta) {
        long retainedDelta = 0;
        Map<String, Long> renditionObjects = new HashMap<>();
        Map<String, Long> renditionBytes = new HashMap<>();
        for (Map.Entry<String, RenditionManifestEntry> object : after.entrySet()) {
            if (!before.containsKey(object.getKey())) {
                RenditionManifestEntry item = object.getValue();
                retainedDelta += item.getSizeBytes();
                String name = item.getRendition().getName();
                renditionObjects.merge(name, 1L, Long::sum);
                renditionBytes.merge(name, item.getSizeBytes(), Long::sum);
            }
        }
        for (Map.Entry<String, RenditionManifestEntry> object : before.entrySet()) {
            if (!after.containsKey(object.getKey())) {
                RenditionManifestEntry item = object.getValue();
                retainedDelta -= item.getSizeBytes();
                String name = item.getRendition().getName();
                renditionObjects.merge(name, -1L, Long::sum);
                renditionBytes.merge(name, -item.getSizeBytes(), Long::sum);
            }
        }
        return new StatsDelta(entries, versions, retainedDelta, currentBytesDelta,
                renditionObjects, renditionBytes);
    }

    /**
     * The manifest entry one write produces: EMPTY for no bytes, otherwise
     * PRESENT under a fresh physical key carrying the content's SHA-256.
     */
    static RenditionManifestEntry writtenEntry(RenditionDescriptor descriptor, byte[] data,
                                               DriveRecord drive, EntryAddress address,
                                               UUID entryUuid, WriteAttribution writtenBy,
                                               Instant now) {
        RenditionManifestEntry.Builder entry = RenditionManifestEntry.newBuilder()
                .setRendition(descriptor)
                .setWrittenAt(Timestamps.fromMillis(now.toEpochMilli()));
        if (writtenBy != null
                && (!writtenBy.getModule().isBlank() || !writtenBy.getActor().isBlank())) {
            entry.setWrittenBy(writtenBy);
        }
        if (data.length == 0) {
            return entry.setState(RenditionState.RENDITION_STATE_EMPTY).build();
        }
        String sha256 = ArchiveManifests.sha256Hex(data);
        return entry.setState(RenditionState.RENDITION_STATE_PRESENT)
                .setSizeBytes(data.length)
                .setSha256(sha256)
                .setObjectKey(ArchiveKeys.rendition(drive, address.getAccountId(),
                        address.getArchive(), entryUuid, descriptor.getName(),
                        descriptor.getSubKey(), sha256))
                .build();
    }

    /** The descriptor a bridge's output lands under: name, media type, shape pin. */
    static RenditionDescriptor derivedDescriptor(BridgeKind kind) {
        RenditionDescriptor.Builder descriptor = RenditionDescriptor.newBuilder()
                .setName(Bridges.renditionName(kind))
                .setMediaType(Bridges.mediaType(kind));
        String subject = Bridges.schemaSubject(kind);
        if (!subject.isBlank()) {
            descriptor.setSchemaSubject(subject);
        }
        return descriptor.build();
    }

    /** The object content type of a rendition: its media type, or octet-stream when it names none. */
    static String contentTypeOf(RenditionDescriptor descriptor) {
        return descriptor.getMediaType().isBlank()
                ? "application/octet-stream" : descriptor.getMediaType();
    }

    /** The version row that stores {@code manifest}. */
    static ArchiveVersionRecord versionRow(UUID entryUuid, long version,
                                           VersionManifest manifest, String root,
                                           long totalBytes, Instant now) {
        ArchiveVersionRecord row = new ArchiveVersionRecord();
        row.entryUuid = entryUuid;
        row.version = version;
        row.manifest = ArchiveManifests.toJson(manifest);
        row.rootChecksum = root;
        row.totalBytes = totalBytes;
        row.createdAt = now;
        return row;
    }

    /**
     * The manifest of a new version, carrying the entry's metadata snapshot.
     *
     * @throws IllegalStateException when the entry row does not match the
     *         address, version or entry UUID being published
     */
    static VersionManifest manifestProto(EntryAddress address, long version,
                                         String root, long totalBytes,
                                         List<RenditionManifestEntry> ordered,
                                         Instant createdAt, ArchiveEntryRecord entry) {
        EntryInfo snapshot = ArchiveRecordCodec.toProto(entry);
        if (!snapshot.getAddress().equals(address) || entry.currentVersion != version
                || !ArchiveIds.entryUuid(address).equals(entry.entryUuid)) {
            throw new IllegalStateException("Archive version metadata does not match its entry identity");
        }
        return VersionManifest.newBuilder()
                .setAddress(address)
                .setVersion(version)
                .setRootChecksum(root)
                .setTotalBytes(totalBytes)
                .setCreatedAt(Timestamps.fromMillis(createdAt.toEpochMilli()))
                .addAllRenditions(ordered)
                .setMetadataSnapshot(snapshot)
                .build();
    }
}
