package ai.protomolt.proto.repo.container.archive;

import ai.protomolt.proto.repo.archive.v1.*;
import ai.protomolt.proto.validate.ProtoValidator;
import jakarta.persistence.EntityManager;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/** Logical archive changes inside admission's transaction. Never performs byte I/O. */
final class ArchiveDestructiveMutations {
    private static final ProtoValidator VALIDATOR = ProtoValidator.create();

    private ArchiveDestructiveMutations() {}

    static ArchiveMutationLedger.LogicalOutcome apply(EntityManager em, ArchiveEntryRecord entry,
            ArchiveMutationCommand command) {
        if (entry == null) {
            if (!command.request().hasDeleteEntry()) throw new ArchiveMutationLedger.EntryMissingException();
            return new ArchiveMutationLedger.LogicalOutcome(false, 0, 0, Set.of());
        }
        var versions = em.createQuery("SELECT v FROM ArchiveVersionRecord v WHERE v.entryUuid=:entry ORDER BY v.version",
                ArchiveVersionRecord.class).setParameter("entry", entry.entryUuid).getResultList();
        Map<Long, Set<UUID>> pins = new HashMap<>();
        for (Object value : em.createNativeQuery("SELECT version,object_id FROM archive_version_object_refs WHERE entry_uuid=:entry")
                .setParameter("entry", entry.entryUuid).getResultList()) {
            var row = (Object[]) value;
            pins.computeIfAbsent(((Number) row[0]).longValue(), ignored -> new HashSet<>()).add((UUID) row[1]);
        }
        Map<Long, VersionManifest> manifests = new HashMap<>();
        for (var version : versions) {
            var manifest = ArchiveManifests.fromJson(version.manifest);
            if (!VALIDATOR.validate(manifest).valid() || !manifest.getAddress().equals(command.address())
                    || manifest.getVersion() != version.version
                    || !manifest.getRootChecksum().equals(version.rootChecksum)
                    || manifest.getTotalBytes() != version.totalBytes
                    || !manifest.getRootChecksum().equals(ArchiveManifests.rootChecksum(manifest.getRenditionsList()))
                    || manifest.getTotalBytes() != ArchiveManifests.totalBytes(manifest.getRenditionsList()))
                throw new IllegalStateException("Stored archive manifest does not match its version");
            var objects = objects(List.of(manifest));
            if (!objects.keySet().equals(pins.getOrDefault(version.version, Set.of())))
                throw new IllegalStateException("Stored archive manifest and object references differ");
            manifests.put(version.version, manifest);
        }
        var before = objects(new ArrayList<>(manifests.values()));
        verifyBindings(em, entry, before);
        var current = manifests.get(entry.currentVersion);
        if (current == null && !versions.isEmpty()) throw new IllegalStateException("Current archive version is missing");
        long currentBefore = current == null ? 0 : current.getTotalBytes();
        long removed = 0;
        long tombstoned = 0;
        boolean deleted = false;
        switch (command.request().getMutationCase()) {
            case DELETE_ENTRY -> {
                removed = versions.size();
                for (var version : versions) em.remove(version);
                em.remove(entry);
                manifests.clear();
                deleted = true;
            }
            case PRUNE_VERSIONS -> {
                int removeCount = Math.max(0, versions.size() - command.request().getPruneVersions().getKeepLatest());
                for (int i = 0; i < removeCount; i++) {
                    var version = versions.get(i);
                    em.remove(version);
                    manifests.remove(version.version);
                    removed++;
                }
            }
            case DELETE_RENDITION -> {
                var request = command.request().getDeleteRendition();
                for (var version : versions) {
                    var original = manifests.get(version.version);
                    var updated = original.toBuilder();
                    Set<UUID> removedPins = new HashSet<>();
                    for (int i = 0; i < original.getRenditionsCount(); i++) {
                        var item = original.getRenditions(i);
                        if (item.getState() == RenditionState.RENDITION_STATE_PRESENT
                                && item.getRendition().getName().equals(request.getRendition())) {
                            removedPins.add(UUID.fromString(item.getStorageObjectId()));
                            updated.setRenditions(i, item.toBuilder().setState(RenditionState.RENDITION_STATE_DELETED)
                                    .setDeletedReason(request.getReason()));
                        }
                    }
                    if (!removedPins.isEmpty()) {
                        updated.setRootChecksum(ArchiveManifests.rootChecksum(updated.getRenditionsList()))
                                .setTotalBytes(ArchiveManifests.totalBytes(updated.getRenditionsList()));
                        var rewritten = updated.build();
                        if (!VALIDATOR.validate(rewritten).valid()) throw new IllegalStateException("Invalid archive tombstone manifest");
                        version.manifest = ArchiveManifests.toJson(rewritten);
                        version.rootChecksum = rewritten.getRootChecksum();
                        version.totalBytes = rewritten.getTotalBytes();
                        manifests.put(version.version, rewritten);
                        for (UUID object : removedPins) {
                            em.createNativeQuery("DELETE FROM archive_version_object_refs WHERE entry_uuid=:entry AND version=:version AND object_id=:id")
                                    .setParameter("entry", entry.entryUuid).setParameter("version", version.version)
                                    .setParameter("id", object).executeUpdate();
                        }
                        tombstoned++;
                    }
                }
            }
            default -> throw new IllegalArgumentException("Unsupported archive mutation");
        }
        var after = objects(new ArrayList<>(manifests.values()));
        var currentAfter = manifests.get(entry.currentVersion);
        var targets = new HashSet<>(before.keySet());
        targets.removeAll(after.keySet());
        ArchiveLedger.applyDelta(em, entry.accountId, entry.archive, delta(deleted ? -1 : 0, -removed,
                before, after, (currentAfter == null ? 0 : currentAfter.getTotalBytes()) - currentBefore));
        return new ArchiveMutationLedger.LogicalOutcome(deleted, removed, tombstoned, targets);
    }

    private static Map<UUID, RenditionManifestEntry> objects(List<VersionManifest> manifests) {
        Map<UUID, RenditionManifestEntry> result = new HashMap<>();
        for (var manifest : manifests) {
            for (var item : manifest.getRenditionsList()) {
                if (item.getState() != RenditionState.RENDITION_STATE_PRESENT) continue;
                if (item.getStorageObjectId().isEmpty())
                    throw new ArchiveMutationLedger.MigrationRequiredException();
                var previous = result.putIfAbsent(UUID.fromString(item.getStorageObjectId()), item);
                if (previous != null && (!previous.getObjectKey().equals(item.getObjectKey())
                        || !previous.getSha256().equals(item.getSha256()) || previous.getSizeBytes() != item.getSizeBytes()
                        || !previous.getRendition().getName().equals(item.getRendition().getName())
                        || !previous.getRendition().getSubKey().equals(item.getRendition().getSubKey())))
                    throw new IllegalStateException("Archive object identity has conflicting manifest facts");
            }
        }
        return result;
    }

    private static void verifyBindings(EntityManager em, ArchiveEntryRecord entry, Map<UUID, RenditionManifestEntry> objects) {
        // Fetch only retained objects, not every historical upload for this entry.
        var rows = em.createNativeQuery("""
                SELECT DISTINCT b.object_id,b.account_id,b.archive,b.object_key,u.expected_size,u.sha256,u.state
                FROM archive_version_object_refs r JOIN archive_object_bindings b USING(object_id)
                JOIN archive_object_uploads u USING(object_id)
                WHERE r.entry_uuid=:entry AND b.entry_uuid=:entry
                """).setParameter("entry", entry.entryUuid).getResultList();
        if (rows.size() != objects.size()) throw new IllegalStateException("Archive object bindings differ from retained manifests");
        for (Object value : rows) {
            var row = (Object[]) value;
            var item = objects.get((UUID) row[0]);
            if (item == null || !entry.accountId.equals(row[1]) || !entry.archive.equals(row[2])
                    || !item.getObjectKey().equals(row[3]) || item.getSizeBytes() != ((Number) row[4]).longValue()
                    || !item.getSha256().equals(row[5]) || !"LIVE".equals(row[6]))
                throw new IllegalStateException("Archive manifest differs from its original byte binding");
        }
    }

    private static ArchiveLedger.StatsDelta delta(long entries, long versions,
            Map<UUID, RenditionManifestEntry> before, Map<UUID, RenditionManifestEntry> after, long currentBytes) {
        long retainedBytes = 0;
        Map<String, Long> counts = new HashMap<>();
        Map<String, Long> bytes = new HashMap<>();
        for (var object : before.entrySet()) {
            if (after.containsKey(object.getKey())) continue;
            var item = object.getValue();
            retainedBytes = Math.subtractExact(retainedBytes, item.getSizeBytes());
            counts.merge(item.getRendition().getName(), -1L, Math::addExact);
            bytes.merge(item.getRendition().getName(), -item.getSizeBytes(), Math::addExact);
        }
        return new ArchiveLedger.StatsDelta(entries, versions, retainedBytes, currentBytes, counts, bytes);
    }

}
