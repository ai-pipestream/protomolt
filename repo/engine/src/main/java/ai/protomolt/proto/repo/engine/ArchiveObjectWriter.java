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

/** Byte admission; the host owns backend qualification and client lifetime. */
public final class ArchiveObjectWriter {
    private final ArchiveUploadLedger uploads;
    private final BlobStore store;
    private final String generation;
    private final Duration lease;
    private final Set<BlobCapability> capabilities;

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
        this.capabilities = Set.copyOf(capabilities);
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

    /** Stream directly to an admitted immutable key, without copy or buffering. */
    public Staged stageStream(UUID entry, String account, String archive, String namespace,
            RenditionManifestEntry rendition, String contentType, java.io.InputStream body) throws java.io.IOException {
        if (!capabilities.contains(BlobCapability.STREAMING_WRITE))
            throw new UnsupportedOperationException("Selected archive backend cannot stream writes");
        if (rendition.getState() != RenditionState.RENDITION_STATE_PRESENT || !rendition.getStorageObjectId().isEmpty()
                || rendition.getSizeBytes() <= 0)
            throw new IllegalArgumentException("Invalid streamed archive candidate");
        String expected = rendition.getSha256();
        if (!expected.isEmpty() && !expected.matches("[0-9a-f]{64}"))
            throw new IllegalArgumentException("Invalid expected archive checksum");
        if (Thread.currentThread().isInterrupted())
            throw new ai.protomolt.proto.repo.spi.RepositoryException(ai.protomolt.proto.repo.spi.RepositoryException.Code.CANCELLED,
                    "Archive upload interrupted before admission");
        var admission = uploads.begin(new ArchiveObjectLedger.Location(entry, account, archive, generation,
                namespace, rendition.getObjectKey()), rendition.getSizeBytes(), contentType, lease);
        var upload = admission.upload();
        var input = new ArchiveUploadInput(body, rendition.getSizeBytes(), lease,
                () -> uploads.renew(upload.objectId(), upload.leaseToken(), lease));
        BlobStore.PutResult result;
        String sha;
        try {
            result = store.put(new BlobStore.PutSpec(namespace, rendition.getObjectKey(), contentType, null,
                    expected.isEmpty() ? null : expected), input, rendition.getSizeBytes());
            sha = input.finish();
        } catch (RuntimeException failure) {
            if (input.lengthFailure() != null && failure != input.lengthFailure())
                throw new ai.protomolt.proto.repo.spi.RepositoryException(ai.protomolt.proto.repo.spi.RepositoryException.Code.INVALID_ARGUMENT,
                        input.lengthFailure().getMessage(), failure);
            throw failure;
        }
        if (!expected.isEmpty() && !expected.equals(sha))
            throw RepositoryErrors.invalidArgument("Archive upload checksum differs from expected_sha256");
        if (result == null) throw new IllegalStateException("Archive backend returned no write receipt");
        uploads.verify(upload.objectId(), upload.leaseToken(), rendition.getSizeBytes(), sha, result.versionId(), result.eTag());
        return new Staged(rendition.toBuilder().setStorageObjectId(upload.objectId().toString()).setSha256(sha).build(),
                upload.objectId(), upload.leaseToken());
    }
}
