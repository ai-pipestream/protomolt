package ai.protomolt.proto.repo.engine;

import ai.protomolt.proto.repo.archive.v1.RenditionManifestEntry;
import ai.protomolt.proto.repo.archive.v1.RenditionState;
import ai.protomolt.proto.repo.blob.spi.BlobStore;
import ai.protomolt.proto.repo.container.archive.ArchiveEntryRecord;
import ai.protomolt.proto.repo.container.archive.ArchiveManifests;
import ai.protomolt.proto.repo.container.archive.ArchiveObjectLedger;
import ai.protomolt.proto.repo.spi.RepositoryException;
import java.util.Objects;
import java.util.UUID;
import static ai.protomolt.proto.repo.engine.RepositoryErrors.failedPrecondition;

/** Reads published objects by immutable backend identity after caller authorization. */
public final class ArchiveObjectReader {
    /**
     * Host-owned clients are borrowed, never closed by the reader. Resolve the exact
     * persisted generation and realm, or fail. Current defaults are not substitutes.
     * Provider configuration and credential rotation remain host responsibilities.
     */
    @FunctionalInterface
    public interface BackendResolver {
        BlobStore resolve(String generation, String storageRealm);
    }

    private final ArchiveObjectLedger objects;
    private final BackendResolver backends;

    public ArchiveObjectReader(ArchiveObjectLedger objects, BackendResolver backends) {
        this.objects = Objects.requireNonNull(objects);
        this.backends = Objects.requireNonNull(backends);
    }

    public BlobStore.GetResult read(ArchiveEntryRecord entry, long version, RenditionManifestEntry manifest) {
        if (manifest.getState() != RenditionState.RENDITION_STATE_PRESENT)
            throw failedPrecondition("Only present archive objects may be read");
        UUID objectId;
        try { objectId = UUID.fromString(manifest.getStorageObjectId()); }
        catch (IllegalArgumentException invalid) { throw failedPrecondition("Invalid archive storage binding identity"); }
        var readable = objects.readable(entry.entryUuid, version, objectId)
                .orElseThrow(() -> failedPrecondition("Archive version has no published storage reference"));
        var binding = readable.binding();
        var location = binding.location();
        if (!location.accountId().equals(entry.accountId) || !location.archive().equals(entry.archive)
                || !location.objectKey().equals(manifest.getObjectKey())
                || readable.size() != manifest.getSizeBytes() || !readable.sha256().equals(manifest.getSha256()))
            throw failedPrecondition("Archive manifest disagrees with its published storage binding");
        var store = backends.resolve(location.backendGeneration(), binding.storageRealm());
        if (store == null) throw failedPrecondition("Original archive backend is not available");
        BlobStore.GetResult result;
        try {
            result = store.get(location.bucket(), location.objectKey(), readable.providerVersion());
        } catch (BlobStore.BlobNotFoundException missing) {
            throw new RepositoryException(RepositoryException.Code.DATA_LOSS,
                    "Published archive object is missing from its original backend", missing);
        }
        if (result.data().length != readable.size()
                || !ArchiveManifests.sha256Hex(result.data()).equals(readable.sha256()))
            throw new RepositoryException(RepositoryException.Code.DATA_LOSS,
                    "Archive object bytes disagree with their published size or checksum");
        return result;
    }
}
