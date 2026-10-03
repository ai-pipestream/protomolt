package ai.protomolt.proto.repo.container.archive;

import ai.protomolt.proto.repo.archive.v1.RenditionManifestEntry;
import ai.protomolt.proto.repo.archive.v1.RenditionState;
import ai.protomolt.proto.repo.archive.v1.VersionManifest;
import ai.protomolt.proto.validate.ProtoValidator;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import jakarta.persistence.EntityManager;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

/** Binding checks and pins inside the owning archive version transaction. */
final class ArchiveVersionBindings {
    private ArchiveVersionBindings() {}

    static boolean hasBindings(String json) {
        var builder = VersionManifest.newBuilder();
        try { JsonFormat.parser().merge(json, builder); }
        catch (InvalidProtocolBufferException invalid) { throw new IllegalArgumentException("Invalid archive manifest", invalid); }
        return builder.getRenditionsList().stream().anyMatch(item -> !item.getStorageObjectId().isEmpty());
    }

    static void publish(EntityManager em, ArchiveEntryRecord entry, ArchiveVersionRecord version, Map<UUID, UUID> tokens) {
        var builder = VersionManifest.newBuilder();
        try { JsonFormat.parser().merge(version.manifest, builder); }
        catch (InvalidProtocolBufferException invalid) { throw new IllegalArgumentException("Invalid archive manifest", invalid); }
        var manifest = builder.build();
        var knownKeys = new java.util.HashSet<>(em.createNativeQuery(
                "SELECT object_key FROM archive_object_bindings WHERE entry_uuid=:entry")
                .setParameter("entry", entry.entryUuid).getResultList());
        var bound = new TreeMap<UUID, RenditionManifestEntry>();
        for (var rendition : manifest.getRenditionsList()) {
            if (rendition.getStorageObjectId().isEmpty() && knownKeys.contains(rendition.getObjectKey()))
                throw new IllegalArgumentException("Managed archive key cannot lose its storage binding");
            if (!rendition.getStorageObjectId().isEmpty()) {
                UUID id = UUID.fromString(rendition.getStorageObjectId());
                var previous = bound.putIfAbsent(id, rendition);
                if (previous != null) throw new IllegalArgumentException("Duplicate archive object binding");
            }
        }
        if (bound.isEmpty()) {
            if (!tokens.isEmpty()) throw new IllegalArgumentException("Upload tokens have no bound manifest references");
            return; // Legacy manifests do not acquire managed pins implicitly.
        }
        if (!ProtoValidator.create().validate(manifest).valid()) throw new IllegalArgumentException("Invalid bound archive manifest");
        var keys = new java.util.HashSet<String>();
        for (var rendition : manifest.getRenditionsList()) {
            if (rendition.getState() == RenditionState.RENDITION_STATE_PRESENT && !keys.add(rendition.getObjectKey()))
                throw new IllegalArgumentException("Duplicate archive physical key");
        }
        if (!entry.entryUuid.equals(version.entryUuid) || manifest.getVersion() != version.version
                || !manifest.getAddress().getAccountId().equals(entry.accountId)
                || !manifest.getAddress().getArchive().equals(entry.archive)
                || !manifest.getAddress().getEntryId().equals(entry.entryId))
            throw new IllegalArgumentException("Archive manifest identity differs from its ledger version");
        if (!bound.keySet().containsAll(tokens.keySet())) throw new IllegalArgumentException("Unknown archive upload token");
        for (var item : bound.entrySet()) {
            UUID id = item.getKey();
            var rendition = item.getValue();
            var binding = ArchiveObjectLedger.find(em, id)
                    .orElseThrow(() -> new IllegalArgumentException("Archive storage binding is missing"));
            var location = binding.location();
            if (!location.entryUuid().equals(entry.entryUuid) || !location.accountId().equals(entry.accountId)
                    || !location.archive().equals(entry.archive) || !location.objectKey().equals(rendition.getObjectKey()))
                throw new IllegalArgumentException("Archive storage binding scope or key differs");
            var upload = ArchiveUploadLedger.lock(em, id);
            if (!rendition.getSha256().equals(upload.sha256()) || rendition.getSizeBytes() != upload.expectedSize())
                throw new IllegalArgumentException("Archive storage binding byte identity differs");
            if (rendition.getState() == RenditionState.RENDITION_STATE_DELETED) {
                if (tokens.containsKey(id)) throw new IllegalArgumentException("Tombstone cannot publish an upload attempt");
                continue;
            }
            if ("VERIFIED".equals(upload.state())) {
                ArchiveUploadLedger.requireOwner(em, upload, tokens.get(id));
                em.createNativeQuery("UPDATE archive_object_uploads SET state='LIVE' WHERE object_id=:id")
                        .setParameter("id", id).executeUpdate();
            } else if ("LIVE".equals(upload.state())) {
                Number pins = (Number) em.createNativeQuery("SELECT count(*) FROM archive_version_object_refs WHERE object_id=:id")
                        .setParameter("id", id).getSingleResult();
                if (pins.longValue() == 0) throw new IllegalArgumentException("Unreferenced archive object cannot be republished");
            } else throw new IllegalArgumentException("Archive object has not been verified");
            em.createNativeQuery("INSERT INTO archive_version_object_refs(entry_uuid,version,object_id) VALUES (:entry,:version,:id)")
                    .setParameter("entry", entry.entryUuid).setParameter("version", version.version).setParameter("id", id).executeUpdate();
        }
    }
}
