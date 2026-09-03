package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.asset.characterize.Characterizer;
import ai.protomolt.proto.asset.v1.Classification;
import ai.protomolt.proto.asset.v1.ClassificationState;
import ai.protomolt.proto.asset.v1.FormatFact;
import ai.protomolt.proto.asset.v1.ObjectStoreOrigin;
import ai.protomolt.proto.repo.archive.v1.Archive;
import ai.protomolt.proto.repo.archive.v1.ArchiveStats;
import ai.protomolt.proto.repo.archive.v1.ClassificationStateCount;
import ai.protomolt.proto.repo.archive.v1.ClassifyEntryRequest;
import ai.protomolt.proto.repo.archive.v1.ClassifyEntryResponse;
import ai.protomolt.proto.repo.archive.v1.CreateArchiveRequest;
import ai.protomolt.proto.repo.archive.v1.CreateArchiveResponse;
import ai.protomolt.proto.repo.archive.v1.DeleteEntryRequest;
import ai.protomolt.proto.repo.archive.v1.DeleteEntryResponse;
import ai.protomolt.proto.repo.archive.v1.DeleteRenditionRequest;
import ai.protomolt.proto.repo.archive.v1.DeleteRenditionResponse;
import ai.protomolt.proto.repo.archive.v1.EntryAddress;
import ai.protomolt.proto.repo.archive.v1.EntryInfo;
import ai.protomolt.proto.repo.archive.v1.GetArchiveRequest;
import ai.protomolt.proto.repo.archive.v1.GetArchiveResponse;
import ai.protomolt.proto.repo.archive.v1.GetArchiveStatsRequest;
import ai.protomolt.proto.repo.archive.v1.GetArchiveStatsResponse;
import ai.protomolt.proto.repo.archive.v1.GetEntryManifestRequest;
import ai.protomolt.proto.repo.archive.v1.GetEntryManifestResponse;
import ai.protomolt.proto.repo.archive.v1.GetEntryRequest;
import ai.protomolt.proto.repo.archive.v1.GetEntryResponse;
import ai.protomolt.proto.repo.archive.v1.ListArchivesRequest;
import ai.protomolt.proto.repo.archive.v1.ListArchivesResponse;
import ai.protomolt.proto.repo.archive.v1.ListEntriesRequest;
import ai.protomolt.proto.repo.archive.v1.ListEntriesResponse;
import ai.protomolt.proto.repo.archive.v1.ListVersionsRequest;
import ai.protomolt.proto.repo.archive.v1.ListVersionsResponse;
import ai.protomolt.proto.repo.archive.v1.PruneVersionsRequest;
import ai.protomolt.proto.repo.archive.v1.PruneVersionsResponse;
import ai.protomolt.proto.repo.archive.v1.PutEntryRequest;
import ai.protomolt.proto.repo.archive.v1.PutEntryResponse;
import ai.protomolt.proto.repo.archive.v1.RenditionContent;
import ai.protomolt.proto.repo.archive.v1.RenditionDescriptor;
import ai.protomolt.proto.repo.archive.v1.RenditionManifestEntry;
import ai.protomolt.proto.repo.archive.v1.RenditionState;
import ai.protomolt.proto.repo.archive.v1.RenditionStats;
import ai.protomolt.proto.repo.archive.v1.VersionManifest;
import ai.protomolt.proto.repo.archive.v1.VersioningPolicy;
import ai.protomolt.proto.repo.archive.v1.WriteAttribution;
import ai.protomolt.proto.repo.container.archive.ArchiveEntryRecord;
import ai.protomolt.proto.repo.container.archive.ArchiveIds;
import ai.protomolt.proto.repo.container.archive.ArchiveLedger;
import ai.protomolt.proto.repo.container.archive.ArchiveLedger.StatsDelta;
import ai.protomolt.proto.repo.container.archive.ArchiveManifests;
import ai.protomolt.proto.repo.container.archive.ArchiveRecord;
import ai.protomolt.proto.repo.container.archive.ArchiveRenditionStatsRecord;
import ai.protomolt.proto.repo.container.archive.ArchiveStatsRecord;
import ai.protomolt.proto.repo.container.archive.ArchiveVersionRecord;
import ai.protomolt.proto.repo.container.blob.BlobStore;
import ai.protomolt.proto.repo.container.ledger.DriveLedger;
import ai.protomolt.proto.repo.container.ledger.DriveRecord;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import com.google.protobuf.util.Timestamps;
import jakarta.persistence.PersistenceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

import static ai.protomolt.proto.repo.service.GrpcErrors.aborted;
import static ai.protomolt.proto.repo.service.GrpcErrors.alreadyExists;
import static ai.protomolt.proto.repo.service.GrpcErrors.failedPrecondition;
import static ai.protomolt.proto.repo.service.GrpcErrors.invalidArgument;
import static ai.protomolt.proto.repo.service.GrpcErrors.notFound;

/**
 * The archive's flows: every mutation follows the same discipline — object
 * IO first (verified, content-addressed, outside any transaction), then one
 * atomic ledger commit carrying the rows and the exact counter deltas, then
 * best-effort deletion of no-longer-referenced objects. A failure between
 * phases leaves orphans, never lies: an object with no owning manifest is
 * reclaimable by the reconciler's standing rule.
 */
final class ArchiveOperations {

    /** Sorted map key for one rendition instance inside a manifest. */
    private record Slot(String name, String subKey) implements Comparable<Slot> {
        @Override
        public int compareTo(Slot other) {
            int byName = name.compareTo(other.name);
            return byName != 0 ? byName : subKey.compareTo(other.subKey);
        }
    }

    /** Receipt of a completed streamed upload. */
    record UploadResult(String entryUuid, long version, String sha256, long sizeBytes,
                        String objectKey, String rootChecksum, boolean deduplicated) {
    }

    private static final Logger LOG = LoggerFactory.getLogger(ArchiveOperations.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final TypeReference<Map<String, String>> STRING_MAP = new TypeReference<>() {
    };
    private static final int CONFLICT_RETRIES = 3;

    private final ArchiveLedger ledger;
    private final DriveLedger drives;
    private final BlobStore blobStore;

    ArchiveOperations(ArchiveLedger ledger, DriveLedger drives, BlobStore blobStore) {
        this.ledger = ledger;
        this.drives = drives;
        this.blobStore = blobStore;
    }

    // ------------------------------------------------------------------
    // Archives
    // ------------------------------------------------------------------

    CreateArchiveResponse createArchive(CreateArchiveRequest request) {
        if (!request.hasArchive()) {
            throw invalidArgument("archive is required");
        }
        Archive archive = request.getArchive();
        ArchiveRequests.accountId(archive.getAccountId());
        ArchiveRequests.archiveName(archive.getName(), "archive.name");
        if (archive.getDriveName().isBlank()) {
            throw invalidArgument("archive.drive_name is required");
        }
        ArchiveRequests.bounded(archive.getDriveName(), 200, "archive.drive_name");
        ArchiveRequests.bounded(archive.getDescription(), 2000, "archive.description");
        String versioning = switch (archive.getVersioning()) {
            case VERSIONING_POLICY_NONE -> ArchiveRecord.VERSIONING_NONE;
            case VERSIONING_POLICY_RETAINED -> ArchiveRecord.VERSIONING_RETAINED;
            default -> throw invalidArgument(
                    "archive.versioning must be NONE or RETAINED; nothing is assumed");
        };
        // The drive must exist before the archive claims it: a save for an
        // unprovisioned namespace hard-fails rather than inventing a bucket.
        drives.findByName(archive.getAccountId(), archive.getDriveName())
                .orElseThrow(() -> failedPrecondition("drive '" + archive.getDriveName()
                        + "' not found for account '" + archive.getAccountId() + "'"));

        ArchiveRecord record = new ArchiveRecord();
        record.archiveId = ArchiveIds.archiveId(archive.getAccountId(), archive.getName());
        record.accountId = archive.getAccountId();
        record.name = archive.getName();
        record.driveName = archive.getDriveName();
        record.versioning = versioning;
        record.description = archive.getDescription().isBlank()
                ? null : archive.getDescription();
        record.metadata = mapToJson(archive.getMetadataMap());
        record.createdAt = Instant.now();
        try {
            ledger.createArchive(record);
        } catch (PersistenceException e) {
            throw alreadyExists("archive '" + archive.getName()
                    + "' already exists for account '" + archive.getAccountId() + "'");
        }
        return CreateArchiveResponse.newBuilder().setArchive(toProto(record)).build();
    }

    GetArchiveResponse getArchive(GetArchiveRequest request) {
        ArchiveRequests.accountId(request.getAccountId());
        ArchiveRequests.archiveName(request.getArchive(), "archive");
        return GetArchiveResponse.newBuilder()
                .setArchive(toProto(archiveOrThrow(request.getAccountId(), request.getArchive())))
                .build();
    }

    ListArchivesResponse listArchives(ListArchivesRequest request) {
        ArchiveRequests.accountId(request.getAccountId());
        int limit = ArchiveRequests.page(request.getLimit());
        long offset = ArchiveRequests.offset(request.getContinuationToken());
        List<ArchiveRecord> page = ledger.listArchives(request.getAccountId(), limit, offset);
        ListArchivesResponse.Builder response = ListArchivesResponse.newBuilder();
        page.forEach(record -> response.addArchives(toProto(record)));
        if (page.size() == limit) {
            response.setNextContinuationToken(Long.toString(offset + limit));
        }
        return response.build();
    }

    GetArchiveStatsResponse stats(GetArchiveStatsRequest request) {
        ArchiveRequests.accountId(request.getAccountId());
        ArchiveRequests.archiveName(request.getArchive(), "archive");
        archiveOrThrow(request.getAccountId(), request.getArchive());
        ArchiveStats.Builder stats = ArchiveStats.newBuilder()
                .setArchive(request.getArchive())
                .setAccountId(request.getAccountId());
        Optional<ArchiveStatsRecord> row =
                ledger.findStats(request.getAccountId(), request.getArchive());
        row.ifPresent(r -> stats.setEntries(r.entries)
                .setVersions(r.versions)
                .setRetainedBytes(r.retainedBytes)
                .setCurrentBytes(r.currentBytes));
        for (ArchiveRenditionStatsRecord rendition
                : ledger.findRenditionStats(request.getAccountId(), request.getArchive())) {
            if (rendition.objectCount == 0 && rendition.totalBytes == 0) {
                continue;
            }
            stats.addRenditions(RenditionStats.newBuilder()
                    .setRenditionName(rendition.renditionName)
                    .setObjectCount(rendition.objectCount)
                    .setTotalBytes(rendition.totalBytes));
        }
        ledger.countByClassificationState(request.getAccountId(), request.getArchive())
                .forEach((state, count) -> stats.addClassificationStates(
                        ClassificationStateCount.newBuilder()
                                .setState(ArchiveClassifications.stateOf(state))
                                .setCount(count)));
        return GetArchiveStatsResponse.newBuilder().setStats(stats).build();
    }

    // ------------------------------------------------------------------
    // Saves
    // ------------------------------------------------------------------

    PutEntryResponse putEntry(PutEntryRequest request) {
        EntryAddress address = ArchiveRequests.address(request.hasAddress(), request.getAddress());
        ArchiveRequests.bounded(request.getTitle(), 500, "title");
        ArchiveRequests.bounded(request.getFilename(), 500, "filename");
        ArchiveRequests.bounded(request.getContentType(), 200, "content_type");
        ArchiveRequests.bounded(request.getSourceUri(), 2000, "source_uri");
        if (request.getRenditionsCount() > 256) {
            throw invalidArgument("renditions exceeds 256 items");
        }
        Set<Slot> seen = new HashSet<>();
        for (RenditionContent content : request.getRenditionsList()) {
            RenditionDescriptor descriptor =
                    ArchiveRequests.rendition(content.hasRendition(), content.getRendition());
            if (!seen.add(new Slot(descriptor.getName(), descriptor.getSubKey()))) {
                throw invalidArgument("renditions repeats '" + descriptor.getName()
                        + (descriptor.getSubKey().isBlank() ? "" : "/" + descriptor.getSubKey())
                        + "'");
            }
        }

        FormatFact declared = ArchiveClassifications.declared(
                request.hasDeclared(), request.getDeclared());
        ObjectStoreOrigin origin = ArchiveClassifications.origin(
                request.hasOrigin(), request.getOrigin());
        ArchiveRecord archive = archiveOrThrow(address.getAccountId(), address.getArchive());
        DriveRecord drive = driveOrThrow(archive);
        UUID entryUuid = ArchiveIds.entryUuid(address);

        for (int attempt = 1; ; attempt++) {
            Optional<ArchiveEntryRecord> existing = ledger.findEntry(entryUuid);
            long base = existing.map(e -> e.currentVersion).orElse(0L);
            if (request.getExpectedVersion() != 0 && request.getExpectedVersion() != base) {
                throw aborted("entry '" + address.getEntryId() + "' is at version " + base
                        + ", not the expected " + request.getExpectedVersion());
            }
            List<ArchiveVersionRecord> retained = existing.isEmpty()
                    ? List.of() : ledger.allVersions(entryUuid);
            VersionManifest current = base == 0 ? null : manifestOf(retained, base);

            // The new manifest: the current renditions carried by reference,
            // overlaid with what this save writes.
            TreeMap<Slot, RenditionManifestEntry> slots = slotsOf(current);
            Instant now = Instant.now();
            for (RenditionContent content : request.getRenditionsList()) {
                RenditionDescriptor descriptor = content.getRendition();
                byte[] data = content.getData().toByteArray();
                slots.put(new Slot(descriptor.getName(), descriptor.getSubKey()),
                        writtenEntry(descriptor, data, drive, address, entryUuid,
                                request.getWrittenBy(), now));
            }
            List<RenditionManifestEntry> ordered = new ArrayList<>(slots.values());
            String root = ArchiveManifests.rootChecksum(ordered);
            long totalBytes = ArchiveManifests.totalBytes(ordered);

            ArchiveEntryRecord entry = entryRow(existing.orElse(null), address, entryUuid,
                    request, now);
            applyClassification(entry, declared, origin,
                    primaryPrefix(slots, request), entry.filename, request.getWrittenBy());
            if (current != null && root.equals(retained.stream()
                    .filter(v -> v.version == base).findFirst().orElseThrow().rootChecksum)) {
                // Identical content: no bytes move, no version lands; the
                // entry's metadata still takes the request's values.
                entry.currentVersion = base;
                ledger.mergeEntry(entry);
                return putResponse(entryUuid, base, root, totalBytes, true,
                        manifestProto(address, base, root, totalBytes, ordered,
                                versionCreatedAt(retained, base)));
            }

            // Object IO first, outside any transaction: only keys no retained
            // manifest references yet are physically written.
            Map<String, RenditionManifestEntry> before =
                    ArchiveManifests.referencedObjects(manifests(retained));
            for (RenditionContent content : request.getRenditionsList()) {
                RenditionDescriptor descriptor = content.getRendition();
                RenditionManifestEntry written =
                        slots.get(new Slot(descriptor.getName(), descriptor.getSubKey()));
                if (written.getState() == RenditionState.RENDITION_STATE_PRESENT
                        && !before.containsKey(written.getObjectKey())) {
                    byte[] data = content.getData().toByteArray();
                    blobStore.put(new BlobStore.PutSpec(drive.bucket, written.getObjectKey(),
                                    contentTypeOf(descriptor), null, written.getSha256()),
                            data);
                }
            }

            long newVersion = base + 1;
            long dropVersion = !archive.retainsVersions() && base != 0 ? base : 0;
            VersionManifest manifest = manifestProto(address, newVersion, root, totalBytes,
                    ordered, now);
            ArchiveVersionRecord versionRow = versionRow(entryUuid, newVersion, manifest,
                    root, totalBytes, now);
            entry.currentVersion = newVersion;

            Map<String, RenditionManifestEntry> after = afterOwnership(retained, dropVersion,
                    manifest);
            StatsDelta delta = delta(existing.isEmpty() ? 1 : 0,
                    1 - (dropVersion != 0 ? 1 : 0),
                    before, after,
                    totalBytes - (current == null ? 0 : ArchiveManifests.totalBytes(
                            current.getRenditionsList())));
            try {
                ledger.commitSave(entry, base, versionRow, dropVersion, delta);
            } catch (ArchiveLedger.VersionConflictException e) {
                if (attempt >= CONFLICT_RETRIES) {
                    throw aborted(e.getMessage());
                }
                continue;
            } catch (PersistenceException e) {
                if (attempt >= CONFLICT_RETRIES) {
                    throw aborted("entry '" + address.getEntryId()
                            + "' is being written concurrently");
                }
                continue;
            }
            deleteQuietly(drive, ArchiveManifests.unreferencedKeys(before, after));
            return putResponse(entryUuid, newVersion, root, totalBytes, false, manifest);
        }
    }

    UploadResult uploadStream(EntryAddress rawAddress, RenditionDescriptor rawDescriptor,
                              long declaredSize, String declaredSha,
                              WriteAttribution writtenBy, InputStream body)
            throws IOException {
        return uploadStream(rawAddress, rawDescriptor, declaredSize, declaredSha, writtenBy,
                null, null, null, body);
    }

    UploadResult uploadStream(EntryAddress rawAddress, RenditionDescriptor rawDescriptor,
                              long declaredSize, String declaredSha,
                              WriteAttribution writtenBy, String filename,
                              FormatFact rawDeclaredFormat, ObjectStoreOrigin rawOrigin,
                              InputStream body)
            throws IOException {
        EntryAddress address = ArchiveRequests.address(true, rawAddress);
        RenditionDescriptor descriptor = ArchiveRequests.rendition(true, rawDescriptor);
        if (declaredSize <= 0) {
            throw invalidArgument("size_bytes must be positive");
        }
        String expectedSha = declaredSha == null ? "" : declaredSha.trim().toLowerCase();
        ArchiveRequests.sha256(expectedSha, "expected_sha256");
        FormatFact declaredFormat = ArchiveClassifications.declared(
                rawDeclaredFormat != null, rawDeclaredFormat);
        ObjectStoreOrigin origin = ArchiveClassifications.origin(
                rawOrigin != null, rawOrigin);

        ArchiveRecord archive = archiveOrThrow(address.getAccountId(), address.getArchive());
        DriveRecord drive = driveOrThrow(archive);
        UUID entryUuid = ArchiveIds.entryUuid(address);

        // Phase 1 — land the bytes, digest computed while streaming. With a
        // declared hash the final content-addressed key is already known and
        // the store's checksum trailer enforces it; without one the bytes
        // stage under the entry and settle onto the final key by server-side
        // copy once the digest completes.
        MessageDigest digest = ArchiveManifests.sha256();
        PrefixCapture capture = new PrefixCapture(body, Characterizer.PREFIX_BYTES);
        body = capture;
        String sha256;
        String objectKey;
        if (!expectedSha.isEmpty()) {
            objectKey = ArchiveKeys.rendition(drive, address.getAccountId(),
                    address.getArchive(), entryUuid, descriptor.getName(),
                    descriptor.getSubKey(), expectedSha);
            try (InputStream in = new DigestInputStream(body, digest)) {
                blobStore.put(new BlobStore.PutSpec(drive.bucket, objectKey,
                        contentTypeOf(descriptor), null, expectedSha), in, declaredSize);
            }
            sha256 = HexFormat.of().formatHex(digest.digest());
            if (!sha256.equals(expectedSha)) {
                // A store without server-side verification can land the
                // mismatch; it must not pose as the declared content.
                deleteQuietly(drive, List.of(objectKey));
                throw invalidArgument("expected_sha256 mismatch: declared " + expectedSha
                        + " but the received bytes hash to " + sha256);
            }
        } else {
            String stagingKey = ArchiveKeys.staging(drive, address.getAccountId(),
                    address.getArchive(), entryUuid, UUID.randomUUID());
            try (InputStream in = new DigestInputStream(body, digest)) {
                blobStore.put(new BlobStore.PutSpec(drive.bucket, stagingKey,
                        contentTypeOf(descriptor), null, null), in, declaredSize);
            }
            sha256 = HexFormat.of().formatHex(digest.digest());
            objectKey = ArchiveKeys.rendition(drive, address.getAccountId(),
                    address.getArchive(), entryUuid, descriptor.getName(),
                    descriptor.getSubKey(), sha256);
            try {
                blobStore.copy(drive.bucket, stagingKey, drive.bucket, objectKey);
            } finally {
                deleteQuietly(drive, List.of(stagingKey));
            }
        }

        // Phase 2 — one new version whose manifest re-references every other
        // current rendition. The bytes are already content-addressed, so a
        // ledger conflict retries against fresh state without re-uploading.
        for (int attempt = 1; ; attempt++) {
            Optional<ArchiveEntryRecord> existing = ledger.findEntry(entryUuid);
            long base = existing.map(e -> e.currentVersion).orElse(0L);
            List<ArchiveVersionRecord> retained = existing.isEmpty()
                    ? List.of() : ledger.allVersions(entryUuid);
            VersionManifest current = base == 0 ? null : manifestOf(retained, base);

            TreeMap<Slot, RenditionManifestEntry> slots = slotsOf(current);
            Instant now = Instant.now();
            RenditionManifestEntry.Builder written = RenditionManifestEntry.newBuilder()
                    .setRendition(descriptor)
                    .setState(RenditionState.RENDITION_STATE_PRESENT)
                    .setSizeBytes(declaredSize)
                    .setSha256(sha256)
                    .setObjectKey(objectKey)
                    .setWrittenAt(Timestamps.fromMillis(now.toEpochMilli()));
            if (writtenBy != null && (!writtenBy.getModule().isBlank()
                    || !writtenBy.getActor().isBlank())) {
                written.setWrittenBy(writtenBy);
            }
            slots.put(new Slot(descriptor.getName(), descriptor.getSubKey()), written.build());
            List<RenditionManifestEntry> ordered = new ArrayList<>(slots.values());
            String root = ArchiveManifests.rootChecksum(ordered);
            long totalBytes = ArchiveManifests.totalBytes(ordered);

            if (current != null && root.equals(retained.stream()
                    .filter(v -> v.version == base).findFirst().orElseThrow().rootChecksum)) {
                // The upload carried what the entry already holds: same hash,
                // same key, already owned — nothing to land or clean.
                return new UploadResult(entryUuid.toString(), base, sha256, declaredSize,
                        objectKey, root, true);
            }

            Map<String, RenditionManifestEntry> before =
                    ArchiveManifests.referencedObjects(manifests(retained));
            long newVersion = base + 1;
            long dropVersion = !archive.retainsVersions() && base != 0 ? base : 0;
            VersionManifest manifest = manifestProto(address, newVersion, root, totalBytes,
                    ordered, now);
            ArchiveEntryRecord entry = existing.orElseGet(() -> {
                ArchiveEntryRecord created = new ArchiveEntryRecord();
                created.entryUuid = entryUuid;
                created.accountId = address.getAccountId();
                created.archive = address.getArchive();
                created.entryId = address.getEntryId();
                created.contentType = descriptor.getMediaType().isBlank()
                        ? null : descriptor.getMediaType();
                created.createdAt = now;
                return created;
            });
            if (filename != null && !filename.isBlank()) {
                entry.filename = filename;
            }
            // Classification recomputes when this upload carries the entry's
            // primary rendition (the manifest's "original", or its first
            // rendition when no "original" exists).
            String primary = slots.containsKey(new Slot("original", ""))
                    ? "original" : slots.firstKey().name();
            if (descriptor.getName().equals(primary)) {
                applyClassification(entry, declaredFormat, origin, capture.prefix(),
                        filename != null && !filename.isBlank() ? filename : entry.filename,
                        writtenBy);
            }
            entry.currentVersion = newVersion;
            entry.updatedAt = now;
            Map<String, RenditionManifestEntry> after = afterOwnership(retained, dropVersion,
                    manifest);
            StatsDelta delta = delta(existing.isEmpty() ? 1 : 0,
                    1 - (dropVersion != 0 ? 1 : 0),
                    before, after,
                    totalBytes - (current == null ? 0 : ArchiveManifests.totalBytes(
                            current.getRenditionsList())));
            try {
                ledger.commitSave(entry,
                        base,
                        versionRow(entryUuid, newVersion, manifest, root, totalBytes, now),
                        dropVersion, delta);
            } catch (ArchiveLedger.VersionConflictException | PersistenceException e) {
                if (attempt >= CONFLICT_RETRIES) {
                    throw aborted("entry '" + address.getEntryId()
                            + "' is being written concurrently");
                }
                continue;
            }
            deleteQuietly(drive, ArchiveManifests.unreferencedKeys(before, after));
            return new UploadResult(entryUuid.toString(), newVersion, sha256, declaredSize,
                    objectKey, root, false);
        }
    }

    // ------------------------------------------------------------------
    // Reads
    // ------------------------------------------------------------------

    GetEntryResponse getEntry(GetEntryRequest request) {
        EntryAddress address = ArchiveRequests.address(request.hasAddress(), request.getAddress());
        ArchiveRecord archive = archiveOrThrow(address.getAccountId(), address.getArchive());
        DriveRecord drive = driveOrThrow(archive);
        ArchiveEntryRecord entry = entryOrThrow(address);
        ArchiveVersionRecord version = versionOrThrow(entry, request.getVersion());
        VersionManifest manifest = ArchiveManifests.fromJson(version.manifest);

        Set<String> wanted = new HashSet<>(request.getRenditionsList());
        GetEntryResponse.Builder response = GetEntryResponse.newBuilder()
                .setInfo(toProto(entry))
                .setManifest(manifest);
        for (RenditionManifestEntry item : manifest.getRenditionsList()) {
            if (item.getState() != RenditionState.RENDITION_STATE_PRESENT) {
                continue;
            }
            if (!wanted.isEmpty() && !wanted.contains(item.getRendition().getName())) {
                continue;
            }
            BlobStore.GetResult got;
            try {
                got = blobStore.get(drive.bucket, item.getObjectKey());
            } catch (BlobStore.BlobNotFoundException e) {
                // The manifest says PRESENT and the store disagrees: fail
                // honestly with the account of what is missing, never an
                // opaque not-found.
                throw failedPrecondition("rendition '" + item.getRendition().getName()
                        + "' of entry '" + address.getEntryId()
                        + "' is unavailable: object " + item.getObjectKey() + " is missing");
            }
            String sha256 = ArchiveManifests.sha256Hex(got.data());
            if (!sha256.equals(item.getSha256())) {
                throw failedPrecondition("rendition '" + item.getRendition().getName()
                        + "' of entry '" + address.getEntryId()
                        + "' is corrupt: stored bytes hash to " + sha256
                        + " but the manifest attests " + item.getSha256());
            }
            response.addRenditions(RenditionContent.newBuilder()
                    .setRendition(item.getRendition())
                    .setData(ByteString.copyFrom(got.data())));
        }
        return response.build();
    }

    GetEntryManifestResponse getManifest(GetEntryManifestRequest request) {
        EntryAddress address = ArchiveRequests.address(request.hasAddress(), request.getAddress());
        archiveOrThrow(address.getAccountId(), address.getArchive());
        ArchiveEntryRecord entry = entryOrThrow(address);
        ArchiveVersionRecord version = versionOrThrow(entry, request.getVersion());
        return GetEntryManifestResponse.newBuilder()
                .setInfo(toProto(entry))
                .setManifest(ArchiveManifests.fromJson(version.manifest))
                .build();
    }

    ListEntriesResponse listEntries(ListEntriesRequest request) {
        ArchiveRequests.accountId(request.getAccountId());
        ArchiveRequests.archiveName(request.getArchive(), "archive");
        archiveOrThrow(request.getAccountId(), request.getArchive());
        int limit = ArchiveRequests.page(request.getLimit());
        long offset = ArchiveRequests.offset(request.getContinuationToken());
        String stateFilter = request.getClassificationState()
                == ClassificationState.CLASSIFICATION_STATE_UNSPECIFIED
                ? null
                : ArchiveClassifications.stateName(Classification.newBuilder()
                        .setState(request.getClassificationState()).build());
        List<ArchiveEntryRecord> page = ledger.listEntries(request.getAccountId(),
                request.getArchive(), stateFilter, limit, offset);
        ListEntriesResponse.Builder response = ListEntriesResponse.newBuilder()
                .setTotalCount(ledger.countEntries(request.getAccountId(), request.getArchive()));
        page.forEach(entry -> response.addEntries(toProto(entry)));
        if (page.size() == limit) {
            response.setNextContinuationToken(Long.toString(offset + limit));
        }
        return response.build();
    }

    ListVersionsResponse listVersions(ListVersionsRequest request) {
        EntryAddress address = ArchiveRequests.address(request.hasAddress(), request.getAddress());
        archiveOrThrow(address.getAccountId(), address.getArchive());
        ArchiveEntryRecord entry = entryOrThrow(address);
        int limit = ArchiveRequests.page(request.getLimit());
        long offset = ArchiveRequests.offset(request.getContinuationToken());
        List<ArchiveVersionRecord> page = ledger.listVersions(entry.entryUuid, limit, offset);
        ListVersionsResponse.Builder response = ListVersionsResponse.newBuilder();
        page.forEach(row -> response.addVersions(ArchiveManifests.fromJson(row.manifest)));
        if (page.size() == limit) {
            response.setNextContinuationToken(Long.toString(offset + limit));
        }
        return response.build();
    }

    // ------------------------------------------------------------------
    // Deletion
    // ------------------------------------------------------------------

    DeleteEntryResponse deleteEntry(DeleteEntryRequest request) {
        EntryAddress address = ArchiveRequests.address(request.hasAddress(), request.getAddress());
        ArchiveRecord archive = archiveOrThrow(address.getAccountId(), address.getArchive());
        DriveRecord drive = driveOrThrow(archive);
        UUID entryUuid = ArchiveIds.entryUuid(address);
        Optional<ArchiveEntryRecord> entry = ledger.findEntry(entryUuid);
        if (entry.isEmpty()) {
            return DeleteEntryResponse.newBuilder().setDeleted(false).build();
        }
        List<ArchiveVersionRecord> retained = ledger.allVersions(entryUuid);
        Map<String, RenditionManifestEntry> owned =
                ArchiveManifests.referencedObjects(manifests(retained));
        VersionManifest current = manifestOf(retained, entry.get().currentVersion);

        // Objects first (the manifests' exact key list, never a prefix
        // sweep), then the rows. A failed object delete leaves an orphan for
        // the reconciler, never a row pointing at nothing.
        deleteQuietly(drive, List.copyOf(owned.keySet()));
        StatsDelta delta = delta(-1, -retained.size(), owned, Map.of(),
                current == null ? 0
                        : -ArchiveManifests.totalBytes(current.getRenditionsList()));
        boolean deleted = ledger.commitDeleteEntry(entryUuid, delta);
        return DeleteEntryResponse.newBuilder()
                .setDeleted(deleted)
                .setVersionsRemoved(deleted ? retained.size() : 0)
                .setObjectsDeleted(deleted ? owned.size() : 0)
                .build();
    }

    DeleteRenditionResponse deleteRendition(DeleteRenditionRequest request) {
        EntryAddress address = ArchiveRequests.address(request.hasAddress(), request.getAddress());
        String rendition = ArchiveRequests.renditionName(request.getRendition(), "rendition");
        if (request.getReason().isBlank()) {
            throw invalidArgument("reason is required: bytes never disappear without a stated why");
        }
        ArchiveRequests.bounded(request.getReason(), 200, "reason");
        ArchiveRecord archive = archiveOrThrow(address.getAccountId(), address.getArchive());
        DriveRecord drive = driveOrThrow(archive);
        ArchiveEntryRecord entry = entryOrThrow(address);
        List<ArchiveVersionRecord> retained = ledger.allVersions(entry.entryUuid);

        Set<String> keys = new HashSet<>();
        Map<String, Long> objectSizes = new HashMap<>();
        List<ArchiveVersionRecord> rewritten = new ArrayList<>();
        long oldCurrentBytes = 0;
        long newCurrentBytes = 0;
        for (ArchiveVersionRecord row : retained) {
            VersionManifest manifest = ArchiveManifests.fromJson(row.manifest);
            boolean touched = false;
            VersionManifest.Builder rebuilt = manifest.toBuilder().clearRenditions();
            for (RenditionManifestEntry item : manifest.getRenditionsList()) {
                if (item.getRendition().getName().equals(rendition)
                        && item.getState() == RenditionState.RENDITION_STATE_PRESENT) {
                    keys.add(item.getObjectKey());
                    objectSizes.put(item.getObjectKey(), item.getSizeBytes());
                    // The tombstone: bytes gone, size/hash/key retained as
                    // provenance, the reason on the record.
                    rebuilt.addRenditions(item.toBuilder()
                            .setState(RenditionState.RENDITION_STATE_DELETED)
                            .setDeletedReason(request.getReason()));
                    touched = true;
                } else {
                    rebuilt.addRenditions(item);
                }
            }
            if (touched) {
                VersionManifest updated = rebuilt.build();
                if (row.version == entry.currentVersion) {
                    oldCurrentBytes = ArchiveManifests.totalBytes(manifest.getRenditionsList());
                    newCurrentBytes = ArchiveManifests.totalBytes(updated.getRenditionsList());
                }
                row.manifest = ArchiveManifests.toJson(updated);
                row.rootChecksum = ArchiveManifests.rootChecksum(updated.getRenditionsList());
                row.totalBytes = ArchiveManifests.totalBytes(updated.getRenditionsList());
                rewritten.add(row);
            }
        }
        if (rewritten.isEmpty()) {
            return DeleteRenditionResponse.newBuilder().build();
        }

        deleteQuietly(drive, List.copyOf(keys));
        long bytesGone = objectSizes.values().stream().mapToLong(Long::longValue).sum();
        StatsDelta delta = new StatsDelta(0, 0, -bytesGone,
                newCurrentBytes - oldCurrentBytes,
                Map.of(rendition, (long) -keys.size()),
                Map.of(rendition, -bytesGone));
        ledger.commitManifestRewrite(rewritten, address.getAccountId(), address.getArchive(),
                delta);
        return DeleteRenditionResponse.newBuilder()
                .setObjectsDeleted(keys.size())
                .setVersionsTombstoned(rewritten.size())
                .build();
    }

    PruneVersionsResponse pruneVersions(PruneVersionsRequest request) {
        EntryAddress address = ArchiveRequests.address(request.hasAddress(), request.getAddress());
        if (request.getKeepLatest() < 1) {
            throw invalidArgument(
                    "keep_latest must be at least 1: pruning everything is DeleteEntry's job");
        }
        ArchiveRecord archive = archiveOrThrow(address.getAccountId(), address.getArchive());
        DriveRecord drive = driveOrThrow(archive);
        ArchiveEntryRecord entry = entryOrThrow(address);
        List<ArchiveVersionRecord> retained = ledger.allVersions(entry.entryUuid);
        if (retained.size() <= request.getKeepLatest()) {
            return PruneVersionsResponse.newBuilder().build();
        }
        int removeCount = retained.size() - request.getKeepLatest();
        List<ArchiveVersionRecord> removed = retained.subList(0, removeCount);
        List<ArchiveVersionRecord> kept = retained.subList(removeCount, retained.size());

        // Entry-local sharing: only objects no kept manifest references may
        // be deleted; everything else survives the prune untouched.
        Map<String, RenditionManifestEntry> before =
                ArchiveManifests.referencedObjects(manifests(retained));
        Map<String, RenditionManifestEntry> after =
                ArchiveManifests.referencedObjects(manifests(kept));
        List<String> gone = ArchiveManifests.unreferencedKeys(before, after);

        deleteQuietly(drive, gone);
        StatsDelta delta = delta(0, -removeCount, before, after, 0);
        ledger.commitPrune(entry.entryUuid,
                removed.stream().map(r -> r.version).toList(),
                address.getAccountId(), address.getArchive(), delta);
        return PruneVersionsResponse.newBuilder()
                .setVersionsRemoved(removeCount)
                .setObjectsDeleted(gone.size())
                .build();
    }

    ClassifyEntryResponse classifyEntry(ClassifyEntryRequest request) {
        EntryAddress address = ArchiveRequests.address(request.hasAddress(), request.getAddress());
        FormatFact declared = ArchiveClassifications.declared(
                request.hasDeclared(), request.getDeclared());
        ArchiveRecord archive = archiveOrThrow(address.getAccountId(), address.getArchive());
        DriveRecord drive = driveOrThrow(archive);
        ArchiveEntryRecord entry = entryOrThrow(address);
        Classification stored = ArchiveClassifications.fromJson(entry.classification);
        if (declared == null && stored != null && stored.hasDeclared()) {
            // Absent declaration withdraws nothing: the standing claim is
            // re-resolved against a fresh read of the bytes.
            declared = stored.getDeclared();
        }
        ObjectStoreOrigin origin = stored != null && stored.hasOrigin()
                ? stored.getOrigin() : null;

        // Characterize the current version's primary rendition from the
        // store. A primary whose bytes are gone characterizes nothing —
        // the resolution then rests on the declaration alone.
        byte[] prefix = null;
        ArchiveVersionRecord version = versionOrThrow(entry, 0);
        VersionManifest manifest = ArchiveManifests.fromJson(version.manifest);
        RenditionManifestEntry primary = primaryOf(manifest);
        if (primary != null
                && primary.getState() == RenditionState.RENDITION_STATE_PRESENT) {
            byte[] bytes = blobStore.get(drive.bucket, primary.getObjectKey()).data();
            prefix = bytes.length <= Characterizer.PREFIX_BYTES
                    ? bytes : java.util.Arrays.copyOf(bytes, Characterizer.PREFIX_BYTES);
        }
        applyClassification(entry, declared, origin, prefix, entry.filename,
                request.hasClassifiedBy() ? request.getClassifiedBy() : null);
        ledger.mergeEntry(entry);
        return ClassifyEntryResponse.newBuilder()
                .setClassification(ArchiveClassifications.fromJson(entry.classification))
                .build();
    }

    /** The manifest's primary rendition: "original", else its first. */
    private static RenditionManifestEntry primaryOf(VersionManifest manifest) {
        RenditionManifestEntry first = null;
        for (RenditionManifestEntry item : manifest.getRenditionsList()) {
            if (item.getRendition().getName().equals("original")) {
                return item;
            }
            if (first == null) {
                first = item;
            }
        }
        return first;
    }

    /**
     * The primary rendition's byte prefix when THIS save carries it; null
     * when the primary's bytes are not in the request (identification is
     * then skipped rather than invented — ClassifyEntry re-reads from the
     * store on demand).
     */
    private static byte[] primaryPrefix(TreeMap<Slot, RenditionManifestEntry> slots,
                                        PutEntryRequest request) {
        String primary = slots.containsKey(new Slot("original", ""))
                ? "original"
                : slots.isEmpty() ? null : slots.firstKey().name();
        if (primary == null) {
            return null;
        }
        for (RenditionContent content : request.getRenditionsList()) {
            if (content.getRendition().getName().equals(primary)
                    && !content.getData().isEmpty()) {
                byte[] data = content.getData().toByteArray();
                return data.length <= Characterizer.PREFIX_BYTES
                        ? data : java.util.Arrays.copyOf(data, Characterizer.PREFIX_BYTES);
            }
        }
        return null;
    }

    /** Resolves and stamps the entry's classification columns. */
    private static void applyClassification(ArchiveEntryRecord entry, FormatFact declared,
                                            ObjectStoreOrigin origin, byte[] prefix,
                                            String filename, WriteAttribution writtenBy) {
        Classification stored = ArchiveClassifications.fromJson(entry.classification);
        if (declared == null && stored != null && stored.hasDeclared()) {
            // A save without a fresh declaration does not withdraw the
            // standing claim.
            declared = stored.getDeclared();
        }
        if (origin == null && stored != null && stored.hasOrigin()) {
            origin = stored.getOrigin();
        }
        if (declared == null && prefix == null && stored != null) {
            // Nothing new to resolve against; the stored classification
            // stands.
            return;
        }
        Classification classification = ArchiveClassifications.classify(
                declared, origin, prefix, filename, writtenBy);
        entry.classification = ArchiveClassifications.toJson(classification);
        entry.classificationState = ArchiveClassifications.stateName(classification);
    }

    /**
     * A stream wrapper capturing the first bytes as they pass — the
     * characterization prefix, read without a second trip to the store.
     */
    private static final class PrefixCapture extends java.io.FilterInputStream {
        private final byte[] head;
        private int captured;

        PrefixCapture(InputStream in, int prefixBytes) {
            super(in);
            this.head = new byte[prefixBytes];
        }

        byte[] prefix() {
            return java.util.Arrays.copyOf(head, captured);
        }

        @Override
        public int read() throws java.io.IOException {
            int b = super.read();
            if (b >= 0 && captured < head.length) {
                head[captured++] = (byte) b;
            }
            return b;
        }

        @Override
        public int read(byte[] buffer, int offset, int length) throws java.io.IOException {
            int n = super.read(buffer, offset, length);
            if (n > 0 && captured < head.length) {
                int take = Math.min(n, head.length - captured);
                System.arraycopy(buffer, offset, head, captured, take);
                captured += take;
            }
            return n;
        }
    }

    // ------------------------------------------------------------------
    // Plumbing
    // ------------------------------------------------------------------

    private ArchiveRecord archiveOrThrow(String accountId, String name) {
        return ledger.findArchive(accountId, name)
                .orElseThrow(() -> notFound("archive '" + name
                        + "' not found for account '" + accountId + "'"));
    }

    private DriveRecord driveOrThrow(ArchiveRecord archive) {
        return drives.findByName(archive.accountId, archive.driveName)
                .orElseThrow(() -> failedPrecondition("drive '" + archive.driveName
                        + "' backing archive '" + archive.name + "' is gone"));
    }

    private ArchiveEntryRecord entryOrThrow(EntryAddress address) {
        return ledger.findEntry(ArchiveIds.entryUuid(address))
                .orElseThrow(() -> notFound("entry '" + address.getEntryId()
                        + "' not found in archive '" + address.getArchive() + "'"));
    }

    private ArchiveVersionRecord versionOrThrow(ArchiveEntryRecord entry, long requested) {
        long version = requested == 0 ? entry.currentVersion : requested;
        return ledger.findVersion(entry.entryUuid, version)
                .orElseThrow(() -> notFound("entry '" + entry.entryId
                        + "' has no retained version " + version));
    }

    private RenditionManifestEntry writtenEntry(RenditionDescriptor descriptor, byte[] data,
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

    private static TreeMap<Slot, RenditionManifestEntry> slotsOf(VersionManifest current) {
        TreeMap<Slot, RenditionManifestEntry> slots = new TreeMap<>();
        if (current != null) {
            for (RenditionManifestEntry entry : current.getRenditionsList()) {
                slots.put(new Slot(entry.getRendition().getName(),
                        entry.getRendition().getSubKey()), entry);
            }
        }
        return slots;
    }

    private static List<VersionManifest> manifests(List<ArchiveVersionRecord> rows) {
        return rows.stream().map(row -> ArchiveManifests.fromJson(row.manifest)).toList();
    }

    private static VersionManifest manifestOf(List<ArchiveVersionRecord> rows, long version) {
        return rows.stream().filter(row -> row.version == version).findFirst()
                .map(row -> ArchiveManifests.fromJson(row.manifest))
                .orElseThrow(() -> failedPrecondition(
                        "the entry's current version " + version + " has no retained manifest"));
    }

    private static Instant versionCreatedAt(List<ArchiveVersionRecord> rows, long version) {
        return rows.stream().filter(row -> row.version == version).findFirst()
                .map(row -> row.createdAt).orElse(Instant.now());
    }

    /** The ownership set after a mutation: retained rows minus the dropped one, plus the new manifest. */
    private static Map<String, RenditionManifestEntry> afterOwnership(
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
    private static StatsDelta delta(long entries, long versions,
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

    private ArchiveEntryRecord entryRow(ArchiveEntryRecord existing, EntryAddress address,
                                        UUID entryUuid, PutEntryRequest request, Instant now) {
        ArchiveEntryRecord entry = existing != null ? existing : new ArchiveEntryRecord();
        if (existing == null) {
            entry.entryUuid = entryUuid;
            entry.accountId = address.getAccountId();
            entry.archive = address.getArchive();
            entry.entryId = address.getEntryId();
            entry.createdAt = now;
        }
        entry.title = blankToNull(request.getTitle(), entry.title);
        entry.filename = blankToNull(request.getFilename(), entry.filename);
        entry.contentType = blankToNull(request.getContentType(), entry.contentType);
        entry.sourceUri = blankToNull(request.getSourceUri(), entry.sourceUri);
        if (request.hasSourceModifiedAt()) {
            entry.sourceModifiedAt = Instant.ofEpochMilli(
                    Timestamps.toMillis(request.getSourceModifiedAt()));
        }
        if (!request.getMetadataMap().isEmpty()) {
            entry.metadata = mapToJson(request.getMetadataMap());
        }
        entry.updatedAt = now;
        return entry;
    }

    /** A blank request field keeps the stored value; a set one replaces it. */
    private static String blankToNull(String requested, String stored) {
        return requested.isBlank() ? stored : requested;
    }

    private static ArchiveVersionRecord versionRow(UUID entryUuid, long version,
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

    private static VersionManifest manifestProto(EntryAddress address, long version,
                                                 String root, long totalBytes,
                                                 List<RenditionManifestEntry> ordered,
                                                 Instant createdAt) {
        return VersionManifest.newBuilder()
                .setAddress(address)
                .setVersion(version)
                .setRootChecksum(root)
                .setTotalBytes(totalBytes)
                .setCreatedAt(Timestamps.fromMillis(createdAt.toEpochMilli()))
                .addAllRenditions(ordered)
                .build();
    }

    private static PutEntryResponse putResponse(UUID entryUuid, long version, String root,
                                                long totalBytes, boolean deduplicated,
                                                VersionManifest manifest) {
        return PutEntryResponse.newBuilder()
                .setEntryUuid(entryUuid.toString())
                .setVersion(version)
                .setRootChecksum(root)
                .setTotalBytes(totalBytes)
                .setDeduplicated(deduplicated)
                .setManifest(manifest)
                .build();
    }

    private void deleteQuietly(DriveRecord drive, List<String> keys) {
        for (String key : keys) {
            try {
                blobStore.delete(drive.bucket, key);
            } catch (RuntimeException e) {
                // An orphan by the standing rule; the reconciler's sweep owns it.
                LOG.warn("best-effort delete of {} failed: {}", key, e.getMessage());
            }
        }
    }

    private static String contentTypeOf(RenditionDescriptor descriptor) {
        return descriptor.getMediaType().isBlank()
                ? "application/octet-stream" : descriptor.getMediaType();
    }

    private Archive toProto(ArchiveRecord record) {
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

    private EntryInfo toProto(ArchiveEntryRecord record) {
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

    private static String mapToJson(Map<String, String> map) {
        if (map.isEmpty()) {
            return null;
        }
        try {
            return MAPPER.writeValueAsString(map);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("metadata map does not print as JSON", e);
        }
    }

    private static Map<String, String> jsonToMap(String json) {
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
