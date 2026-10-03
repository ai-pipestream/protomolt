package ai.protomolt.proto.repo.engine;

import ai.protomolt.proto.repo.archive.v1.RenditionManifestEntry;
import ai.protomolt.proto.repo.archive.v1.RenditionState;
import ai.protomolt.proto.repo.blob.spi.BlobCapability;
import ai.protomolt.proto.repo.blob.spi.BlobStore;
import ai.protomolt.proto.repo.container.archive.ArchiveManifests;
import ai.protomolt.proto.repo.container.archive.ArchiveObjectLedger;
import ai.protomolt.proto.repo.container.archive.ArchiveUploadLedger;
import java.time.Duration;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

/** Internal unary admission; the host owns backend qualification and client lifetime. */
public final class ArchiveObjectWriter {
    private final ArchiveUploadLedger uploads;
    private final BlobStore store;
    private final String generation;
    private final Duration lease;

    public ArchiveObjectWriter(ArchiveUploadLedger uploads, BlobStore store, String generation,
            Set<BlobCapability> capabilities, Duration lease) {
        this.uploads = Objects.requireNonNull(uploads);
        this.store = Objects.requireNonNull(store);
        if (generation == null || !generation.matches("[A-Za-z0-9][A-Za-z0-9._:-]{0,127}"))
            throw new IllegalArgumentException("Invalid archive backend generation");
        if (!Set.copyOf(capabilities).containsAll(Set.of(BlobCapability.NON_EXPIRING_WRITES, BlobCapability.PHYSICAL_RECLAMATION)))
            throw new UnsupportedOperationException("Managed archive writes require retention and physical reclamation");
        if (lease == null || lease.compareTo(Duration.ofSeconds(1)) < 0 || lease.compareTo(Duration.ofDays(1)) > 0)
            throw new IllegalArgumentException("Archive upload lease must be between one second and one day");
        this.generation = generation;
        this.lease = lease;
    }

    public record Staged(RenditionManifestEntry rendition, UUID objectId, UUID leaseToken) {}

    /**
     * Failure leaves its durable reservation for recovery. Never delete a candidate
     * here: a lost provider acknowledgement cannot prove the write did not happen.
     * Verification is byte integrity only, not schema admission or semantic review.
     */
    public Staged stage(UUID entry, String account, String archive, String namespace,
            RenditionManifestEntry rendition, String contentType, byte[] bytes) {
        Objects.requireNonNull(bytes);
        if (rendition.getState() != RenditionState.RENDITION_STATE_PRESENT || !rendition.getStorageObjectId().isEmpty()
                || rendition.getSizeBytes() != bytes.length
                || !rendition.getSha256().equals(ArchiveManifests.sha256Hex(bytes)))
            throw new IllegalArgumentException("Archive candidate does not match its bytes");
        if (Thread.currentThread().isInterrupted())
            throw RepositoryErrors.aborted("Archive upload interrupted before admission");
        var admission = uploads.begin(new ArchiveObjectLedger.Location(entry, account, archive, generation,
                namespace, rendition.getObjectKey()), bytes.length, contentType, lease);
        var uploaded = store.put(new BlobStore.PutSpec(namespace, rendition.getObjectKey(), contentType,
                null, rendition.getSha256()), bytes);
        if (Thread.currentThread().isInterrupted())
            throw RepositoryErrors.aborted("Archive upload interrupted before verification");
        var upload = admission.upload();
        uploads.verify(upload.objectId(), upload.leaseToken(), bytes.length, rendition.getSha256(),
                uploaded.versionId(), uploaded.eTag());
        return new Staged(rendition.toBuilder().setStorageObjectId(upload.objectId().toString()).build(),
                upload.objectId(), upload.leaseToken());
    }
}
