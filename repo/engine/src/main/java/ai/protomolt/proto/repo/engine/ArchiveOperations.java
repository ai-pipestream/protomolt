package ai.protomolt.proto.repo.engine;

import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.asset.bridge.Bridge;
import ai.protomolt.proto.asset.bridge.BridgeEngine;
import ai.protomolt.proto.asset.bridge.Bridges;
import ai.protomolt.proto.asset.characterize.ByteWindows;
import ai.protomolt.proto.asset.characterize.WindowCapture;
import ai.protomolt.proto.asset.v1.BridgeKind;
import ai.protomolt.proto.asset.v1.BridgeStatus;
import ai.protomolt.proto.asset.v1.Classification;
import ai.protomolt.proto.asset.v1.ClassificationState;
import ai.protomolt.proto.asset.v1.ContentProfile;
import ai.protomolt.proto.asset.v1.FormatFact;
import ai.protomolt.proto.asset.v1.ObjectStoreOrigin;
import ai.protomolt.proto.repo.archive.v1.Archive;
import ai.protomolt.proto.repo.archive.v1.ArchiveStats;
import ai.protomolt.proto.repo.archive.v1.BridgeEntryRequest;
import ai.protomolt.proto.repo.archive.v1.BridgeEntryResponse;
import ai.protomolt.proto.repo.archive.v1.BridgeOutcome;
import ai.protomolt.proto.repo.archive.v1.ClassificationStateCount;
import ai.protomolt.proto.repo.archive.v1.ClassifyEntryRequest;
import ai.protomolt.proto.repo.archive.v1.ClassifyEntryResponse;
import ai.protomolt.proto.repo.archive.v1.CreateArchiveRequest;
import ai.protomolt.proto.repo.archive.v1.CreateArchiveResponse;
import ai.protomolt.proto.repo.archive.v1.EntryAddress;
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
import ai.protomolt.proto.repo.archive.v1.PutEntryRequest;
import ai.protomolt.proto.repo.archive.v1.PutEntryResponse;
import ai.protomolt.proto.repo.archive.v1.ReadRenditionRequest;
import ai.protomolt.proto.repo.archive.v1.RenditionContent;
import ai.protomolt.proto.repo.archive.v1.RenditionDescriptor;
import ai.protomolt.proto.repo.archive.v1.RenditionManifestEntry;
import ai.protomolt.proto.repo.archive.v1.RenditionState;
import ai.protomolt.proto.repo.archive.v1.RenditionStats;
import ai.protomolt.proto.repo.archive.v1.VersionManifest;
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
import ai.protomolt.proto.repo.blob.spi.BlobStore;
import ai.protomolt.proto.repo.engine.ArchiveSlots.Slot;
import ai.protomolt.proto.repo.container.ledger.DriveLedger;
import ai.protomolt.proto.repo.container.ledger.DriveRecord;
import ai.protomolt.proto.repo.spi.RepositoryException;
import com.google.protobuf.ByteString;
import com.google.protobuf.util.Timestamps;
import jakarta.persistence.PersistenceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

import static ai.protomolt.proto.repo.engine.RepositoryErrors.aborted;
import static ai.protomolt.proto.repo.engine.RepositoryErrors.alreadyExists;
import static ai.protomolt.proto.repo.engine.RepositoryErrors.failedPrecondition;
import static ai.protomolt.proto.repo.engine.RepositoryErrors.invalidArgument;
import static ai.protomolt.proto.repo.engine.RepositoryErrors.notFound;

/**
 * Shared archive operations. Writes stage unique objects outside SQL and publish
 * their manifest references in an atomic ledger commit. Durable tracking of
 * unsuccessful candidates and admission before destructive object I/O remain
 * incomplete; the archive deletion failure tests capture those unsafe paths.
 */
public final class ArchiveOperations implements ai.protomolt.proto.repo.spi.ArchiveRepository {
    private static final Logger LOG = LoggerFactory.getLogger(ArchiveOperations.class);
    private static final int CONFLICT_RETRIES = 3;

    private final ArchiveLedger ledger;
    private final DriveLedger drives;
    private final BlobStore blobStore;
    private final BridgeEngine bridgeEngine;
    private final ArchiveObjectReader objectReader;
    private final ArchiveObjectWriter objectWriter;
    private final ArchivePutAdmission putAdmission;
    private final ArchiveGetAdmission getAdmission;

    public ArchiveOperations(ArchiveLedger ledger, DriveLedger drives, BlobStore blobStore) {
        this(ledger, drives, blobStore, BridgeEngine.standard());
    }

    public ArchiveOperations(ArchiveLedger ledger, DriveLedger drives, BlobStore blobStore,
                      BridgeEngine bridgeEngine) {
        this(ledger, drives, blobStore, bridgeEngine, null);
    }

    public ArchiveOperations(ArchiveLedger ledger, DriveLedger drives, BlobStore blobStore,
                      BridgeEngine bridgeEngine, ArchiveObjectReader objectReader) {
        this(ledger, drives, blobStore, bridgeEngine, objectReader, null);
    }

    public ArchiveOperations(ArchiveLedger ledger, DriveLedger drives, BlobStore blobStore,
                      BridgeEngine bridgeEngine, ArchiveObjectReader objectReader, ArchiveObjectWriter objectWriter) {
        this(ledger, drives, blobStore, bridgeEngine, objectReader, objectWriter, null);
    }

    /** Bounded unary composition. The host owns and drains admission before closing storage. */
    public ArchiveOperations(ArchiveLedger ledger, DriveLedger drives, BlobStore blobStore,
                      BridgeEngine bridgeEngine, ArchiveObjectReader objectReader, ArchiveObjectWriter objectWriter,
                      ArchivePutAdmission putAdmission) {
        this(ledger, drives, blobStore, bridgeEngine, objectReader, objectWriter, putAdmission, null);
    }

    /** Optional shared read construction gate; the host owns its close and drain. */
    public ArchiveOperations(ArchiveLedger ledger, DriveLedger drives, BlobStore blobStore,
                      BridgeEngine bridgeEngine, ArchiveObjectReader objectReader, ArchiveObjectWriter objectWriter,
                      ArchivePutAdmission putAdmission, ArchiveGetAdmission getAdmission) {
        if (objectWriter != null && objectReader == null)
            throw new IllegalArgumentException("Managed archive writes require original-backend reads");
        if (getAdmission != null && objectReader == null)
            throw new IllegalArgumentException("Bounded archive reads require a managed bounded reader");
        this.ledger = ledger;
        this.drives = drives;
        this.blobStore = blobStore;
        this.bridgeEngine = bridgeEngine;
        this.objectReader = objectReader;
        this.objectWriter = objectWriter;
        this.putAdmission = putAdmission;
        this.getAdmission = getAdmission;
    }

    // ------------------------------------------------------------------
    // Archives
    // ------------------------------------------------------------------

    @Override
    public CreateArchiveResponse createArchive(RepositoryCaller caller, CreateArchiveRequest request) {
        RepositoryErrors.requireProcessAuthority(caller);
        return RepositoryErrors.call(() -> createArchiveImpl(request));
    }

    private CreateArchiveResponse createArchiveImpl(CreateArchiveRequest request) {
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
        record.metadata = ArchiveRecordCodec.mapToJson(archive.getMetadataMap());
        record.createdAt = Instant.now();
        try {
            ledger.createArchive(record);
        } catch (PersistenceException e) {
            throw alreadyExists("archive '" + archive.getName()
                    + "' already exists for account '" + archive.getAccountId() + "'");
        }
        return CreateArchiveResponse.newBuilder().setArchive(ArchiveRecordCodec.toProto(record)).build();
    }

    @Override
    public GetArchiveResponse getArchive(RepositoryCaller caller, GetArchiveRequest request) {
        RepositoryErrors.requireAccountBinding(caller);
        return RepositoryErrors.call(() -> getArchiveImpl(caller, request));
    }

    private GetArchiveResponse getArchiveImpl(RepositoryCaller caller, GetArchiveRequest request) {
        ArchiveRequests.accountId(request.getAccountId());
        RepositoryErrors.requireAccount(caller, request.getAccountId());
        ArchiveRequests.archiveName(request.getArchive(), "archive");
        return GetArchiveResponse.newBuilder()
                .setArchive(ArchiveRecordCodec.toProto(archiveOrThrow(request.getAccountId(), request.getArchive())))
                .build();
    }

    @Override
    public ListArchivesResponse listArchives(RepositoryCaller caller, ListArchivesRequest request) {
        RepositoryErrors.requireAccountBinding(caller);
        return RepositoryErrors.call(() -> listArchivesImpl(caller, request));
    }

    private ListArchivesResponse listArchivesImpl(RepositoryCaller caller, ListArchivesRequest request) {
        ArchiveRequests.accountId(request.getAccountId());
        RepositoryErrors.requireAccount(caller, request.getAccountId());
        int limit = ArchiveRequests.page(request.getLimit());
        long offset = ArchiveRequests.offset(request.getContinuationToken());
        List<ArchiveRecord> page = ledger.listArchives(request.getAccountId(), limit, offset);
        ListArchivesResponse.Builder response = ListArchivesResponse.newBuilder();
        page.forEach(record -> response.addArchives(ArchiveRecordCodec.toProto(record)));
        if (page.size() == limit) {
            response.setNextContinuationToken(Long.toString(offset + limit));
        }
        return response.build();
    }

    @Override
    public GetArchiveStatsResponse stats(RepositoryCaller caller, GetArchiveStatsRequest request) {
        RepositoryErrors.requireAccountBinding(caller);
        return RepositoryErrors.call(() -> statsImpl(caller, request));
    }

    private GetArchiveStatsResponse statsImpl(RepositoryCaller caller, GetArchiveStatsRequest request) {
        ArchiveRequests.accountId(request.getAccountId());
        RepositoryErrors.requireAccount(caller, request.getAccountId());
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

    @Override
    public PutEntryResponse putEntry(RepositoryCaller caller, PutEntryRequest request) {
        RepositoryErrors.requireProcessAuthority(caller);
        try (var admission = putAdmission == null ? null : putAdmission.admit(request)) {
            return RepositoryErrors.call(() -> putEntryImpl(request));
        }
    }

    private PutEntryResponse putEntryImpl(PutEntryRequest request) {
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
            if (!seen.add(ArchiveSlots.of(descriptor))) {
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
            if (objectWriter == null) requireLegacyDestructivePath(retained);
            VersionManifest current = base == 0 ? null : ArchiveVersions.manifestOf(retained, base);

            // The new manifest: the current renditions carried by reference,
            // overlaid with what this save writes.
            TreeMap<Slot, RenditionManifestEntry> slots = ArchiveSlots.slotsOf(current);
            Instant now = Instant.now();
            for (RenditionContent content : request.getRenditionsList()) {
                RenditionDescriptor descriptor = content.getRendition();
                byte[] data = content.getData().toByteArray();
                Slot slot = ArchiveSlots.of(descriptor);
                slots.put(slot, ArchiveSlots.shareRetainedObject(
                        ArchiveVersions.writtenEntry(descriptor, data, drive, address, entryUuid,
                                request.getWrittenBy(), now), slots.get(slot)));
            }
            List<RenditionManifestEntry> ordered = new ArrayList<>(slots.values());
            String root = ArchiveManifests.rootChecksum(ordered);
            long totalBytes = ArchiveManifests.totalBytes(ordered);

            ArchiveEntryRecord entry = entryRow(existing.orElse(null), address, entryUuid,
                    request, now);
            applyClassification(entry, declared, origin,
                    ArchiveSlots.primaryWindows(slots, request), entry.filename,
                    request.getWrittenBy());
            if (current != null && root.equals(ArchiveVersions.rootChecksumOf(retained, base))) {
                // Identical content: no bytes move, no version lands; the
                // entry's metadata still takes the request's values.
                entry.currentVersion = base;
                ledger.mergeEntry(entry);
                // No new version was stored. Return its retained provenance,
                // not candidate timestamps or descriptors synthesized for this retry.
                return putResponse(entryUuid, base, root, totalBytes, true, current);
            }

            // Object IO first, outside any transaction: only keys no retained
            // manifest references yet are physically written.
            Map<String, RenditionManifestEntry> before =
                    ArchiveManifests.referencedObjects(ArchiveVersions.manifests(retained));
            Map<UUID, UUID> uploadTokens = new HashMap<>();
            for (RenditionContent content : request.getRenditionsList()) {
                RenditionDescriptor descriptor = content.getRendition();
                RenditionManifestEntry written =
                        slots.get(ArchiveSlots.of(descriptor));
                if (written.getState() == RenditionState.RENDITION_STATE_PRESENT
                        && !before.containsKey(written.getObjectKey())) {
                    byte[] data = content.getData().toByteArray();
                    if (objectWriter != null) {
                        var staged = objectWriter.stage(entryUuid, address.getAccountId(), address.getArchive(),
                                drive.bucket, written, ArchiveVersions.contentTypeOf(descriptor), data);
                        slots.put(ArchiveSlots.of(descriptor), staged.rendition());
                        uploadTokens.put(staged.objectId(), staged.leaseToken());
                    } else {
                        blobStore.put(new BlobStore.PutSpec(drive.bucket, written.getObjectKey(),
                                    ArchiveVersions.contentTypeOf(descriptor), null, written.getSha256()),
                            data);
                    }
                }
            }
            ordered = new ArrayList<>(slots.values());

            long newVersion = base + 1;
            long dropVersion = !archive.retainsVersions() && base != 0 ? base : 0;
            entry.currentVersion = newVersion;
            VersionManifest manifest = ArchiveVersions.manifestProto(address, newVersion, root, totalBytes,
                    ordered, now, entry);
            ArchiveVersionRecord versionRow = ArchiveVersions.versionRow(entryUuid, newVersion, manifest,
                    root, totalBytes, now);

            Map<String, RenditionManifestEntry> after = ArchiveVersions.afterOwnership(retained, dropVersion,
                    manifest);
            StatsDelta delta = ArchiveVersions.delta(existing.isEmpty() ? 1 : 0,
                    1 - (dropVersion != 0 ? 1 : 0),
                    before, after,
                    totalBytes - (current == null ? 0 : ArchiveManifests.totalBytes(
                            current.getRenditionsList())));
            try {
                ledger.commitSave(entry, base, versionRow, dropVersion, delta, uploadTokens);
            } catch (ArchiveLedger.VersionConflictException e) {
                if (attempt >= CONFLICT_RETRIES) {
                    throw aborted(e.getMessage());
                }
                continue;
            } catch (PersistenceException e) {
                if (objectWriter != null) throw e;
                if (attempt >= CONFLICT_RETRIES) {
                    throw aborted("entry '" + address.getEntryId()
                            + "' is being written concurrently");
                }
                continue;
            }
            if (objectWriter == null)
                deleteQuietly(drive, ArchiveManifests.unreferencedKeys(before, after));
            return putResponse(entryUuid, newVersion, root, totalBytes, false, manifest);
        }
    }

    @Override
    public UploadResult uploadStream(RepositoryCaller caller,
            EntryAddress address, RenditionDescriptor descriptor, long size, String sha256,
            WriteAttribution attribution, String filename, FormatFact declared,
            ObjectStoreOrigin origin, InputStream body) throws IOException {
        RepositoryErrors.requireProcessAuthority(caller);
        requireIngressEnabled("Streaming archive uploads");
        try {
            return RepositoryErrors.call(() -> {
                try {
                    return uploadStreamImpl(address, descriptor, size, sha256, attribution,
                            filename, declared, origin, body);
                } catch (IOException failure) {
                    throw new java.io.UncheckedIOException(failure);
                }
            });
        } catch (java.io.UncheckedIOException failure) {
            throw failure.getCause();
        }
    }

    private UploadResult uploadStreamImpl(EntryAddress rawAddress, RenditionDescriptor rawDescriptor,
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

        if (objectWriter == null) requireLegacyDestructivePath(ledger.allVersions(entryUuid));
        // Phase 1 — land the bytes, digest computed while streaming. With a
        // declared hash a fresh final key can be allocated immediately and
        // the store's checksum trailer enforces it; without one the bytes
        // stage under the entry and settle onto the final key by server-side
        // copy once the digest completes.
        MessageDigest digest = ArchiveManifests.sha256();
        WindowCapture capture = new WindowCapture(body);
        body = capture;
        String sha256;
        String objectKey;
        ArchiveObjectWriter.Staged staged = null;
        if (objectWriter != null) {
            objectKey = ArchiveKeys.streamed(drive, address.getAccountId(), address.getArchive(), entryUuid);
            var candidate = RenditionManifestEntry.newBuilder().setRendition(descriptor)
                    .setState(RenditionState.RENDITION_STATE_PRESENT).setSizeBytes(declaredSize)
                    .setSha256(expectedSha).setObjectKey(objectKey).build();
            staged = objectWriter.stageStream(entryUuid, address.getAccountId(), address.getArchive(), drive.bucket,
                    candidate, ArchiveVersions.contentTypeOf(descriptor), body);
            sha256 = staged.rendition().getSha256();
        } else if (!expectedSha.isEmpty()) {
            objectKey = ArchiveKeys.rendition(drive, address.getAccountId(),
                    address.getArchive(), entryUuid, descriptor.getName(),
                    descriptor.getSubKey(), expectedSha);
            try (InputStream in = new DigestInputStream(body, digest)) {
                blobStore.put(new BlobStore.PutSpec(drive.bucket, objectKey,
                        ArchiveVersions.contentTypeOf(descriptor), null, expectedSha), in, declaredSize);
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
                        ArchiveVersions.contentTypeOf(descriptor), null, null), in, declaredSize);
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
        // current rendition. The bytes already have a unique physical key, so a
        // ledger conflict retries against fresh state without re-uploading.
        for (int attempt = 1; ; attempt++) {
            Optional<ArchiveEntryRecord> existing = ledger.findEntry(entryUuid);
            long base = existing.map(e -> e.currentVersion).orElse(0L);
            List<ArchiveVersionRecord> retained = existing.isEmpty()
                    ? List.of() : ledger.allVersions(entryUuid);
            VersionManifest current = base == 0 ? null : ArchiveVersions.manifestOf(retained, base);

            if (objectWriter == null) requireLegacyDestructivePath(retained);
            TreeMap<Slot, RenditionManifestEntry> slots = ArchiveSlots.slotsOf(current);
            Instant now = Instant.now();
            RenditionManifestEntry.Builder written = RenditionManifestEntry.newBuilder()
                    .setRendition(descriptor)
                    .setState(RenditionState.RENDITION_STATE_PRESENT)
                    .setSizeBytes(declaredSize)
                    .setSha256(sha256)
                    .setObjectKey(objectKey)
                    .setWrittenAt(Timestamps.fromMillis(now.toEpochMilli()));
            if (staged != null) written.setStorageObjectId(staged.objectId().toString());
            if (writtenBy != null && (!writtenBy.getModule().isBlank()
                    || !writtenBy.getActor().isBlank())) {
                written.setWrittenBy(writtenBy);
            }
            slots.put(ArchiveSlots.of(descriptor), written.build());
            List<RenditionManifestEntry> ordered = new ArrayList<>(slots.values());
            String root = ArchiveManifests.rootChecksum(ordered);
            long totalBytes = ArchiveManifests.totalBytes(ordered);

            if (current != null && root.equals(ArchiveVersions.rootChecksumOf(retained, base))) {
                // The upload candidate is unique and was never published.
                // Return the retained reference, not the discarded candidate.
                var retainedSlot = ArchiveSlots.slotsOf(current)
                        .get(ArchiveSlots.of(descriptor));
                if (retainedSlot == null || retainedSlot.getState() != RenditionState.RENDITION_STATE_PRESENT
                        || !retainedSlot.getSha256().equals(sha256)
                        || retainedSlot.getSizeBytes() != declaredSize || retainedSlot.getObjectKey().isBlank()) {
                    throw failedPrecondition("Retained rendition does not match the uploaded content identity");
                }
                String retainedKey = retainedSlot.getObjectKey();
                ledger.confirmRetainedVersion(existing.orElseThrow());
                if (objectWriter == null) blobStore.delete(drive.bucket, objectKey);
                return new UploadResult(entryUuid.toString(), base, sha256, declaredSize,
                        retainedKey, root, true);
            }

            Map<String, RenditionManifestEntry> before =
                    ArchiveManifests.referencedObjects(ArchiveVersions.manifests(retained));
            long newVersion = base + 1;
            long dropVersion = !archive.retainsVersions() && base != 0 ? base : 0;
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
            String primary = ArchiveSlots.primaryName(slots);
            if (descriptor.getName().equals(primary)) {
                applyClassification(entry, declaredFormat, origin, capture.windows(),
                        filename != null && !filename.isBlank() ? filename : entry.filename,
                        writtenBy);
            }
            entry.currentVersion = newVersion;
            entry.updatedAt = now;
            VersionManifest manifest = ArchiveVersions.manifestProto(address, newVersion, root, totalBytes,
                    ordered, now, entry);
            Map<String, RenditionManifestEntry> after = ArchiveVersions.afterOwnership(retained, dropVersion,
                    manifest);
            StatsDelta delta = ArchiveVersions.delta(existing.isEmpty() ? 1 : 0,
                    1 - (dropVersion != 0 ? 1 : 0),
                    before, after,
                    totalBytes - (current == null ? 0 : ArchiveManifests.totalBytes(
                            current.getRenditionsList())));
            try {
                ledger.commitSave(entry,
                        base,
                        ArchiveVersions.versionRow(entryUuid, newVersion, manifest, root, totalBytes, now),
                        dropVersion, delta, staged == null ? Map.of() : Map.of(staged.objectId(), staged.leaseToken()));
            } catch (ArchiveLedger.VersionConflictException e) {
                if (attempt >= CONFLICT_RETRIES) {
                    throw aborted("entry '" + address.getEntryId()
                            + "' is being written concurrently");
                }
                continue;
            } catch (PersistenceException e) {
                if (objectWriter != null) throw e;
                if (attempt >= CONFLICT_RETRIES) throw aborted("entry is being written concurrently");
                continue;
            }
            if (objectWriter == null) deleteQuietly(drive, ArchiveManifests.unreferencedKeys(before, after));
            return new UploadResult(entryUuid.toString(), newVersion, sha256, declaredSize,
                    objectKey, root, false);
        }
    }

    // ------------------------------------------------------------------
    // Reads
    // ------------------------------------------------------------------

    @Override
    public GetEntryResponse getEntry(RepositoryCaller caller, GetEntryRequest request) {
        RepositoryErrors.requireAccountBinding(caller);
        try (var manifests = getAdmission == null ? null : getAdmission.manifests()) {
            return RepositoryErrors.call(() -> getEntryImpl(caller, request, manifests));
        }
    }

    private GetEntryResponse getEntryImpl(RepositoryCaller caller, GetEntryRequest request,
            ArchiveGetAdmission.ManifestScope manifests) {
        EntryAddress address = ArchiveRequests.address(request.hasAddress(), request.getAddress());
        RepositoryErrors.requireAccount(caller, address.getAccountId());
        ArchiveRecord archive = archiveOrThrow(address.getAccountId(), address.getArchive());
        ArchiveEntryRecord entry = entryOrThrow(address);
        long version = request.getVersion() == 0 ? entry.currentVersion : request.getVersion();
        VersionManifest manifest = readManifest(entry.entryUuid, version, manifests)
                .orElseThrow(() -> notFound("entry has no retained version " + version));

        Set<String> wanted = new HashSet<>(request.getRenditionsList());
        GetEntryResponse.Builder response = GetEntryResponse.newBuilder()
                .setInfo(ArchiveRecordCodec.toProto(entry))
                .setManifest(manifest);
        var selected = manifest.getRenditionsList().stream()
                .filter(item -> item.getState() == RenditionState.RENDITION_STATE_PRESENT)
                .filter(item -> wanted.isEmpty() || wanted.contains(item.getRendition().getName())).toList();
        if (getAdmission != null && selected.stream().anyMatch(item -> item.getStorageObjectId().isBlank()))
            throw failedPrecondition("Bounded archive reads require published storage bindings for every selected rendition");
        try (var scope = getAdmission == null ? null : getAdmission.admit(response.build(), selected)) {
            for (RenditionManifestEntry item : selected) {
                BlobStore.GetResult got;
                try {
                    got = readObject(archive, entry, version, item);
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
            var result = response.build();
            if (scope != null) scope.verify(result);
            return result;
        }
    }

    /**
     * Streams one present rendition object, whole or as a byte range, with the same
     * caller checks as {@link #getEntry}: an account binding, and the entry's account. A name and sub-key the version does not
     * record is NOT_FOUND; a rendition whose bytes were deleted is FAILED_PRECONDITION
     * naming the recorded reason; a range outside the object is INVALID_ARGUMENT.
     * Bound renditions read the exact published object version under a read pin held
     * until the body closes; legacy manifests read their drive's current object. The
     * returned body checks length, and the manifest SHA-256 for a whole-object read.
     */
    @Override
    public RenditionStream readRendition(RepositoryCaller caller, ReadRenditionRequest request) {
        RepositoryErrors.requireAccountBinding(caller);
        try (var manifests = getAdmission == null ? null : getAdmission.manifests()) {
            return RepositoryErrors.call(() -> readRenditionImpl(caller, request, manifests));
        }
    }

    private RenditionStream readRenditionImpl(RepositoryCaller caller, ReadRenditionRequest request,
            ArchiveGetAdmission.ManifestScope manifests) {
        EntryAddress address = ArchiveRequests.address(request.hasAddress(), request.getAddress());
        RepositoryErrors.requireAccount(caller, address.getAccountId());
        String name = ArchiveRequests.renditionName(request.getRendition(), "rendition");
        if (!request.getSubKey().isEmpty()) ArchiveRequests.renditionName(request.getSubKey(), "sub_key");
        if (request.getVersion() < 0) throw invalidArgument("version must not be negative");
        if (request.getOffset() < 0 || request.getLength() < 0)
            throw invalidArgument("offset and length must not be negative");
        ArchiveRecord archive = archiveOrThrow(address.getAccountId(), address.getArchive());
        ArchiveEntryRecord entry = entryOrThrow(address);
        long version = request.getVersion() == 0 ? entry.currentVersion : request.getVersion();
        VersionManifest manifest = readManifest(entry.entryUuid, version, manifests)
                .orElseThrow(() -> notFound("entry has no retained version " + version));
        RenditionManifestEntry item = manifest.getRenditionsList().stream()
                .filter(candidate -> candidate.getRendition().getName().equals(name)
                        && candidate.getRendition().getSubKey().equals(request.getSubKey()))
                .findFirst()
                .orElseThrow(() -> notFound("version " + version + " of entry '" + address.getEntryId()
                        + "' records no rendition '" + name + "'"
                        + (request.getSubKey().isEmpty() ? "" : " with sub-key '" + request.getSubKey() + "'")));
        if (item.getState() != RenditionState.RENDITION_STATE_PRESENT)
            throw failedPrecondition("rendition '" + name + "' of entry '" + address.getEntryId() + "' is "
                    + item.getState().name() + (item.getDeletedReason().isEmpty() ? "" : ": " + item.getDeletedReason()));
        long size = item.getSizeBytes();
        if (size < 0) throw new RepositoryException(
                RepositoryException.Code.DATA_LOSS, "Negative published archive size");
        long offset = request.getOffset();
        if (offset > size || request.getLength() > size - offset)
            throw invalidArgument("Requested byte range [" + offset + ", +" + request.getLength()
                    + ") is outside rendition size " + size);
        long length = request.getLength() == 0 ? size - offset : request.getLength();
        boolean whole = offset == 0 && length == size;
        long providerLength = whole ? -1 : length;
        InputStream body;
        if (!item.getStorageObjectId().isEmpty()) {
            if (objectReader == null)
                throw failedPrecondition("Original archive backend resolution is not configured");
            body = objectReader.open(entry, version, item, offset, providerLength);
        } else {
            // Legacy manifests have no recorded backend identity; read the drive's current object.
            try {
                body = blobStore.openStream(driveOrThrow(archive).bucket, item.getObjectKey(), offset, providerLength);
            } catch (UnsupportedOperationException unsupported) {
                throw new RepositoryException(RepositoryException.Code.FAILED_PRECONDITION,
                        "Archive blob store does not support streamed reads", unsupported);
            } catch (BlobStore.BlobNotFoundException missing) {
                throw failedPrecondition("rendition '" + name + "' of entry '" + address.getEntryId()
                        + "' is unavailable: object " + item.getObjectKey() + " is missing");
            }
        }
        return new RenditionStream(version, item, offset, length, whole,
                new VerifiedRenditionStream(body, length, whole ? item.getSha256() : null));
    }

    @Override
    public GetEntryManifestResponse getManifest(RepositoryCaller caller, GetEntryManifestRequest request) {
        RepositoryErrors.requireAccountBinding(caller);
        try (var manifests = getAdmission == null ? null : getAdmission.manifests()) {
            return RepositoryErrors.call(() -> getManifestImpl(caller, request, manifests));
        }
    }

    private GetEntryManifestResponse getManifestImpl(RepositoryCaller caller,
            GetEntryManifestRequest request, ArchiveGetAdmission.ManifestScope manifests) {
        EntryAddress address = ArchiveRequests.address(request.hasAddress(), request.getAddress());
        RepositoryErrors.requireAccount(caller, address.getAccountId());
        archiveOrThrow(address.getAccountId(), address.getArchive());
        ArchiveEntryRecord entry = entryOrThrow(address);
        long version = request.getVersion() == 0 ? entry.currentVersion : request.getVersion();
        return GetEntryManifestResponse.newBuilder()
                .setInfo(ArchiveRecordCodec.toProto(entry))
                .setManifest(readManifest(entry.entryUuid, version, manifests)
                        .orElseThrow(() -> notFound("entry has no retained version " + version)))
                .build();
    }

    @Override
    public ListEntriesResponse listEntries(RepositoryCaller caller, ListEntriesRequest request) {
        RepositoryErrors.requireAccountBinding(caller);
        try (var manifests = getAdmission == null || !request.getIncludeManifests() ? null : getAdmission.manifests()) {
            return RepositoryErrors.call(() -> listEntriesImpl(caller, request, manifests));
        }
    }

    private ListEntriesResponse listEntriesImpl(RepositoryCaller caller, ListEntriesRequest request,
            ArchiveGetAdmission.ManifestScope manifests) {
        ArchiveRequests.accountId(request.getAccountId());
        RepositoryErrors.requireAccount(caller, request.getAccountId());
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
        for (ArchiveEntryRecord entry : page) {
            response.addEntries(ArchiveRecordCodec.toProto(entry));
            if (request.getIncludeManifests()) {
                // Same order as the entries, and one manifest per entry even
                // when a row's current version has gone missing: a listing
                // whose two lists drift apart cannot be zipped.
                response.addManifests(readManifest(entry.entryUuid, entry.currentVersion, manifests)
                        .orElseGet(VersionManifest::getDefaultInstance));
            }
        }
        if (page.size() == limit) {
            response.setNextContinuationToken(Long.toString(offset + limit));
        }
        return response.build();
    }

    @Override
    public ListVersionsResponse listVersions(RepositoryCaller caller, ListVersionsRequest request) {
        RepositoryErrors.requireAccountBinding(caller);
        try (var manifests = getAdmission == null ? null : getAdmission.manifests()) {
            return RepositoryErrors.call(() -> listVersionsImpl(caller, request, manifests));
        }
    }

    private ListVersionsResponse listVersionsImpl(RepositoryCaller caller, ListVersionsRequest request,
            ArchiveGetAdmission.ManifestScope manifests) {
        EntryAddress address = ArchiveRequests.address(request.hasAddress(), request.getAddress());
        RepositoryErrors.requireAccount(caller, address.getAccountId());
        archiveOrThrow(address.getAccountId(), address.getArchive());
        ArchiveEntryRecord entry = entryOrThrow(address);
        int limit = ArchiveRequests.page(request.getLimit());
        long offset = ArchiveRequests.offset(request.getContinuationToken());
        ListVersionsResponse.Builder response = ListVersionsResponse.newBuilder();
        int count;
        if (manifests == null) {
            var page = ledger.listVersions(entry.entryUuid, limit, offset);
            page.forEach(row -> response.addVersions(ArchiveManifests.fromJson(row.manifest)));
            count = page.size();
        } else {
            var page = ledger.listManifests(entry.entryUuid, limit, offset, manifests.remainingBytes());
            for (var row : page) {
                manifests.consumed(row.utf8Bytes());
                response.addVersions(ArchiveManifests.fromJson(row.json()));
            }
            count = page.size();
        }
        if (count == limit) {
            response.setNextContinuationToken(Long.toString(offset + limit));
        }
        return response.build();
    }

    private Optional<VersionManifest> readManifest(UUID entry, long version, ArchiveGetAdmission.ManifestScope scope) {
        if (scope == null) return ledger.findVersion(entry, version).map(row -> ArchiveManifests.fromJson(row.manifest));
        return ledger.findManifest(entry, version, scope.remainingBytes()).map(row -> {
            scope.consumed(row.utf8Bytes());
            return ArchiveManifests.fromJson(row.json());
        });
    }

    @Override
    public ClassifyEntryResponse classifyEntry(RepositoryCaller caller, ClassifyEntryRequest request) {
        RepositoryErrors.requireProcessAuthority(caller);
        return RepositoryErrors.call(() -> classifyEntryImpl(request));
    }

    private ClassifyEntryResponse classifyEntryImpl(ClassifyEntryRequest request) {
        EntryAddress address = ArchiveRequests.address(request.hasAddress(), request.getAddress());
        FormatFact declared = ArchiveClassifications.declared(
                request.hasDeclared(), request.getDeclared());
        ArchiveRecord archive = archiveOrThrow(address.getAccountId(), address.getArchive());
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
        ByteWindows windows = null;
        ArchiveVersionRecord version = versionOrThrow(entry, 0);
        VersionManifest manifest = ArchiveManifests.fromJson(version.manifest);
        RenditionManifestEntry primary = ArchiveSlots.primaryOf(manifest);
        if (primary != null
                && primary.getState() == RenditionState.RENDITION_STATE_PRESENT) {
            windows = ByteWindows.ofWhole(
                    readObject(archive, entry, version.version, primary).data());
        }
        applyClassification(entry, declared, origin, windows, entry.filename,
                request.hasClassifiedBy() ? request.getClassifiedBy() : null);
        ledger.mergeEntry(entry);
        return ClassifyEntryResponse.newBuilder()
                .setClassification(ArchiveClassifications.fromJson(entry.classification))
                .build();
    }

    // ------------------------------------------------------------------
    // Bridging
    // ------------------------------------------------------------------

    @Override
    public BridgeEntryResponse bridgeEntry(RepositoryCaller caller, BridgeEntryRequest request) {
        RepositoryErrors.requireProcessAuthority(caller);
        requireIngressEnabled("Archive bridge generation");
        return RepositoryErrors.call(() -> bridgeEntryImpl(request));
    }

    private void requireIngressEnabled(String operation) {
        if (putAdmission != null) throw new ai.protomolt.proto.repo.spi.RepositoryException(
                ai.protomolt.proto.repo.spi.RepositoryException.Code.UNSUPPORTED,
                operation + " is not enabled in bounded unary composition");
    }

    private BridgeEntryResponse bridgeEntryImpl(BridgeEntryRequest request) {
        EntryAddress address = ArchiveRequests.address(request.hasAddress(), request.getAddress());
        ArchiveRecord archive = archiveOrThrow(address.getAccountId(), address.getArchive());
        ArchiveEntryRecord entry = entryOrThrow(address);

        if (objectWriter == null) requireLegacyDestructivePath(ledger.allVersions(entry.entryUuid));
        Classification classification = ArchiveClassifications.fromJson(entry.classification);
        ClassificationState state = classification == null
                ? ClassificationState.CLASSIFICATION_STATE_UNCLASSIFIED
                : classification.getState();
        if (!Bridges.bridgeable(state)) {
            throw failedPrecondition("entry '" + address.getEntryId() + "' is "
                    + ArchiveClassifications.stateName(state)
                    + "; bridging needs a classification that names exactly one format");
        }
        FormatFact format = Bridges.formatOfRecord(classification);
        List<BridgeKind> applicable = Bridges.applicableTo(format);
        List<BridgeKind> wanted = request.getBridgesList().isEmpty()
                ? applicable : request.getBridgesList();
        for (BridgeKind kind : request.getBridgesList()) {
            if (!applicable.contains(kind)) {
                throw invalidArgument("bridge " + kind.name() + " does not apply to a "
                        + format.getFormatCase().name().toLowerCase(Locale.ROOT) + " asset");
            }
        }

        ArchiveVersionRecord version = versionOrThrow(entry, 0);
        VersionManifest manifest = ArchiveManifests.fromJson(version.manifest);
        RenditionManifestEntry primary = ArchiveSlots.primaryOf(manifest);
        if (primary == null || primary.getState() != RenditionState.RENDITION_STATE_PRESENT) {
            throw failedPrecondition("entry '" + address.getEntryId()
                    + "' has no present primary rendition to bridge from");
        }

        List<BridgeOutcome.Builder> outcomes = new ArrayList<>();
        Map<RenditionDescriptor, Derived> produced = new LinkedHashMap<>();
        // The run list can grow: a document the prose bridge finds nothing
        // in is a scan, and OCR applies only once that has been found.
        List<BridgeKind> run = new ArrayList<>(wanted);
        // The blob store hands back bytes, not a stream, so the original is
        // read whole — once for the whole run, however many bridges read it.
        // Streaming reads are the store's own follow-on; bridging gains them
        // for free when they land.
        byte[] original = null;
        for (int at = 0; at < run.size(); at++) {
            BridgeKind kind = run.get(at);
            BridgeOutcome.Builder outcome = BridgeOutcome.newBuilder()
                    .setBridge(kind)
                    .setRendition(Bridges.renditionName(kind));
            outcomes.add(outcome);
            Optional<Bridge> bridge = bridgeEngine.forKind(kind, format);
            if (bridge.isEmpty()) {
                outcome.setStatus(BridgeStatus.BRIDGE_STATUS_DEFERRED)
                        .setDetail(Bridges.deferralReason(kind, format));
                continue;
            }
            if (original == null) {
                original = readObject(archive, entry, version.version, primary).data();
            }
            Bridge.Derivation derivation;
            try {
                derivation = bridge.get().derive(new ByteArrayInputStream(original),
                        new Bridge.Context(format, entry.filename));
            } catch (IOException | RuntimeException e) {
                outcome.setStatus(BridgeStatus.BRIDGE_STATUS_FAILED)
                        .setDetail(e.getMessage() == null ? e.toString() : e.getMessage());
                continue;
            }
            produced.put(ArchiveVersions.derivedDescriptor(kind), new Derived(derivation, outcome));
            escalate(kind, derivation, format, run);
        }

        long landed = produced.isEmpty()
                ? entry.currentVersion
                : landDerived(address, archive, driveOrThrow(archive), produced, request.getBridgedBy(), entry.mutationRevision);
        return BridgeEntryResponse.newBuilder()
                .setVersion(landed)
                .addAllOutcomes(outcomes.stream().map(BridgeOutcome.Builder::build).toList())
                .build();
    }

    /**
     * The one escalation the routing rule cannot make on its own: a document
     * whose prose bridge produced nothing is a scan, and "scanned" is a
     * finding rather than a property of the format. OCR joins the run only
     * once the finding exists, and only where this host can run it.
     */
    private void escalate(BridgeKind ran, Bridge.Derivation derivation, FormatFact format,
                          List<BridgeKind> run) {
        if (ran != BridgeKind.BRIDGE_KIND_DOCUMENT_TEXT
                || derivation.content().length != 0
                || run.contains(BridgeKind.BRIDGE_KIND_OCR_TEXT)
                || bridgeEngine.forKind(BridgeKind.BRIDGE_KIND_OCR_TEXT, format).isEmpty()) {
            return;
        }
        run.add(BridgeKind.BRIDGE_KIND_OCR_TEXT);
    }

    /** One bridge's product on its way into a manifest slot. */
    private record Derived(Bridge.Derivation derivation, BridgeOutcome.Builder outcome) {
    }

    /**
     * Lands every produced rendition in ONE new version beside the original,
     * which is carried by reference and never rewritten. Identical output
     * preserves the root checksum and retained physical references; no version
     * lands when all produced content is unchanged.
     */
    private long landDerived(EntryAddress address, ArchiveRecord archive, DriveRecord drive,
                             Map<RenditionDescriptor, Derived> produced,
                             WriteAttribution bridgedBy, long sourceRevision) {
        UUID entryUuid = ArchiveIds.entryUuid(address);
        for (int attempt = 1; ; attempt++) {
            ArchiveEntryRecord entry = entryOrThrow(address);
            if (entry.mutationRevision != sourceRevision)
                throw aborted("Archive bridge source changed while deriving content");
            long base = entry.currentVersion;
            List<ArchiveVersionRecord> retained = ledger.allVersions(entryUuid);
            if (objectWriter == null) requireLegacyDestructivePath(retained);
            VersionManifest current = ArchiveVersions.manifestOf(retained, base);

            TreeMap<Slot, RenditionManifestEntry> slots = ArchiveSlots.slotsOf(current);
            Instant now = Instant.now();
            Map<String, byte[]> bytesByKey = new LinkedHashMap<>();
            for (Map.Entry<RenditionDescriptor, Derived> item : produced.entrySet()) {
                RenditionDescriptor descriptor = item.getKey();
                byte[] data = item.getValue().derivation().content();
                RenditionManifestEntry written = ArchiveVersions.writtenEntry(descriptor, data, drive, address,
                        entryUuid, bridgedBy, now);
                written = ArchiveSlots.shareRetainedObject(written,
                        slots.get(ArchiveSlots.of(descriptor)));
                ContentProfile profile = item.getValue().derivation().profile();
                if (profile != null) {
                    written = written.toBuilder().setContentProfile(profile).build();
                }
                slots.put(ArchiveSlots.of(descriptor), written);
                if (written.getState() == RenditionState.RENDITION_STATE_PRESENT) {
                    bytesByKey.put(written.getObjectKey(), data);
                }
            }
            List<RenditionManifestEntry> ordered = new ArrayList<>(slots.values());
            String root = ArchiveManifests.rootChecksum(ordered);
            long totalBytes = ArchiveManifests.totalBytes(ordered);

            boolean unchanged = root.equals(ArchiveVersions.rootChecksumOf(retained, base));
            for (Derived item : produced.values()) {
                item.outcome().setStatus(unchanged
                        ? BridgeStatus.BRIDGE_STATUS_UNCHANGED
                        : BridgeStatus.BRIDGE_STATUS_PRODUCED);
                if (item.derivation().degraded()) {
                    item.outcome().setDetail(String.join("; ", item.derivation().warnings()));
                }
            }
            if (unchanged) {
                // Every bridge reproduced what the entry already holds.
                ledger.confirmRetainedVersion(entry);
                return base;
            }

            Map<String, RenditionManifestEntry> before =
                    ArchiveManifests.referencedObjects(ArchiveVersions.manifests(retained));
            Map<UUID, UUID> uploadTokens = new HashMap<>();
            for (Map.Entry<String, byte[]> object : bytesByKey.entrySet()) {
                if (!before.containsKey(object.getKey())) {
                    RenditionManifestEntry written = ordered.stream()
                            .filter(item -> object.getKey().equals(item.getObjectKey()))
                            .findFirst().orElseThrow();
                    if (objectWriter != null) {
                        var staged = objectWriter.stage(entryUuid, address.getAccountId(), address.getArchive(), drive.bucket,
                                written, ArchiveVersions.contentTypeOf(written.getRendition()), object.getValue());
                        slots.put(ArchiveSlots.of(written.getRendition()), staged.rendition());
                        uploadTokens.put(staged.objectId(), staged.leaseToken());
                    } else blobStore.put(new BlobStore.PutSpec(drive.bucket, object.getKey(),
                                    ArchiveVersions.contentTypeOf(written.getRendition()), null,
                                    written.getSha256()),
                            object.getValue());
                }
            }

            ordered = new ArrayList<>(slots.values());

            long newVersion = base + 1;
            long dropVersion = !archive.retainsVersions() && base != 0 ? base : 0;
            entry.currentVersion = newVersion;
            entry.updatedAt = now;
            VersionManifest manifest = ArchiveVersions.manifestProto(address, newVersion, root, totalBytes,
                    ordered, now, entry);
            Map<String, RenditionManifestEntry> after = ArchiveVersions.afterOwnership(retained, dropVersion,
                    manifest);
            StatsDelta delta = ArchiveVersions.delta(0, 1 - (dropVersion != 0 ? 1 : 0), before, after,
                    totalBytes - ArchiveManifests.totalBytes(current.getRenditionsList()));
            try {
                ledger.commitSave(entry, base,
                        ArchiveVersions.versionRow(entryUuid, newVersion, manifest, root, totalBytes, now),
                        dropVersion, delta, uploadTokens);
            } catch (ArchiveLedger.VersionConflictException e) {
                if (attempt >= CONFLICT_RETRIES) {
                    throw aborted("entry '" + address.getEntryId()
                            + "' is being written concurrently");
                }
                continue;
            } catch (PersistenceException e) {
                if (objectWriter != null) throw e;
                if (attempt >= CONFLICT_RETRIES) throw aborted("entry is being written concurrently");
                continue;
            }
            if (objectWriter == null) deleteQuietly(drive, ArchiveManifests.unreferencedKeys(before, after));
            return newVersion;
        }
    }

    /** Resolves and stamps the entry's classification columns. */
    private static void applyClassification(ArchiveEntryRecord entry, FormatFact declared,
                                            ObjectStoreOrigin origin, ByteWindows windows,
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
        if (declared == null && windows == null && stored != null) {
            // Nothing new to resolve against; the stored classification
            // stands.
            return;
        }
        Classification classification = ArchiveClassifications.classify(
                declared, origin, windows, filename, writtenBy);
        entry.classification = ArchiveClassifications.toJson(classification);
        entry.classificationState = ArchiveClassifications.stateName(classification);
    }

    // ------------------------------------------------------------------
    // Plumbing
    // ------------------------------------------------------------------

    private ArchiveRecord archiveOrThrow(String accountId, String name) {
        return ledger.findArchive(accountId, name)
                .orElseThrow(() -> notFound("archive '" + name
                        + "' not found for account '" + accountId + "'"));
    }

    private BlobStore.GetResult readObject(ArchiveRecord archive, ArchiveEntryRecord entry,
            long version, RenditionManifestEntry rendition) {
        if (!rendition.getStorageObjectId().isEmpty()) {
            if (objectReader == null)
                throw failedPrecondition("Original archive backend resolution is not configured");
            return objectReader.read(entry, version, rendition);
        }
        // Legacy manifests have no recorded backend identity. This path is not
        // evidence of their original location and must not be used for bound objects.
        return blobStore.get(driveOrThrow(archive).bucket, rendition.getObjectKey());
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

    private static void requireLegacyDestructivePath(List<ArchiveVersionRecord> versions) {
        for (var version : versions) {
            if (ArchiveManifests.fromJson(version.manifest).getRenditionsList().stream()
                    .anyMatch(rendition -> !rendition.getStorageObjectId().isEmpty())) {
                throw failedPrecondition("Bound archive objects require durable destructive admission");
            }
        }
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
            entry.metadata = ArchiveRecordCodec.mapToJson(request.getMetadataMap());
        }
        entry.updatedAt = now;
        return entry;
    }

    /** A blank request field keeps the stored value; a set one replaces it. */
    private static String blankToNull(String requested, String stored) {
        return requested.isBlank() ? stored : requested;
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
}
