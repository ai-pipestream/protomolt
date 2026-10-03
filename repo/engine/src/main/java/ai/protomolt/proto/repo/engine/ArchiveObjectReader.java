package ai.protomolt.proto.repo.engine;

import ai.protomolt.proto.repo.archive.v1.RenditionManifestEntry;
import ai.protomolt.proto.repo.archive.v1.RenditionState;
import ai.protomolt.proto.repo.blob.spi.BlobStore;
import ai.protomolt.proto.repo.container.archive.ArchiveEntryRecord;
import ai.protomolt.proto.repo.container.archive.ArchiveManifests;
import ai.protomolt.proto.repo.container.archive.ArchiveReadLedger;
import ai.protomolt.proto.repo.spi.RepositoryException;
import java.util.Objects;
import java.util.UUID;
import static ai.protomolt.proto.repo.engine.RepositoryErrors.failedPrecondition;

/** Reads published objects by immutable backend identity after caller authorization. */
public final class ArchiveObjectReader implements AutoCloseable {
    /**
     * Host-owned clients are borrowed, never closed by the reader. Resolve the exact
     * persisted generation and realm, or fail. Current defaults are not substitutes.
     * Provider configuration and credential rotation remain host responsibilities.
     */
    @FunctionalInterface
    public interface BackendResolver {
        BlobStore resolve(String generation, String storageRealm);
    }

    private final ArchiveReadLedger reads;
    private final BackendResolver backends;
    private final Object lifecycle = new Object();
    private boolean closed;
    private boolean fenceComplete;
    private int activeReads;

    public ArchiveObjectReader(ArchiveReadLedger reads, BackendResolver backends) {
        this.reads = Objects.requireNonNull(reads);
        this.backends = Objects.requireNonNull(backends);
    }

    public BlobStore.GetResult read(ArchiveEntryRecord entry, long version, RenditionManifestEntry manifest) {
        synchronized (lifecycle) {
            if (closed) throw new RepositoryException(RepositoryException.Code.CANCELLED, "Archive reader is closed");
            activeReads++;
        }
        try { return readPinned(entry, version, manifest); }
        finally {
            synchronized (lifecycle) { activeReads--; lifecycle.notifyAll(); }
        }
    }

    /**
     * Stop local admission, then persist its fence. SQL failure leaves local
     * admission closed and must be retried before releasing borrowed resources.
     * The ledger and clients must stay open until awaitIdle succeeds.
     */
    @Override public void close() {
        synchronized (lifecycle) { closed = true; lifecycle.notifyAll(); }
        reads.fence();
        synchronized (lifecycle) { fenceComplete = true; lifecycle.notifyAll(); }
    }

    /**
     * Wait for entered reads, including resolver, provider I/O, verification and
     * attempted pin release. This proves local quiescence, not absence of durable
     * pins after a failed release. Timeout or interruption grants no cleanup rights.
     */
    public boolean awaitIdle(java.time.Duration timeout) throws InterruptedException {
        Objects.requireNonNull(timeout);
        if (timeout.isNegative()) throw new IllegalArgumentException("Drain timeout must not be negative");
        long budget = timeout.toNanos(), remaining = budget, started = System.nanoTime();
        synchronized (lifecycle) {
            if (!closed) throw new IllegalStateException("Close the reader before awaiting idle");
            if (!fenceComplete) throw new IllegalStateException("Complete the reader fence before awaiting idle");
            while (activeReads != 0) {
                if (remaining <= 0) return false;
                java.util.concurrent.TimeUnit.NANOSECONDS.timedWait(lifecycle, remaining);
                remaining = budget - (System.nanoTime() - started);
            }
            return true;
        }
    }

    private BlobStore.GetResult readPinned(ArchiveEntryRecord entry, long version, RenditionManifestEntry manifest) {
        if (manifest.getState() != RenditionState.RENDITION_STATE_PRESENT)
            throw failedPrecondition("Only present archive objects may be read");
        UUID objectId;
        try { objectId = UUID.fromString(manifest.getStorageObjectId()); }
        catch (IllegalArgumentException invalid) { throw failedPrecondition("Invalid archive storage binding identity"); }
        try (var pin = reads.acquire(entry.entryUuid, version, objectId)
                .orElseThrow(() -> failedPrecondition("Archive version has no published storage reference"))) {
            var readable = pin.readable();
            var binding = readable.binding();
            var location = binding.location();
            if (!location.accountId().equals(entry.accountId) || !location.archive().equals(entry.archive)
                    || !location.objectKey().equals(manifest.getObjectKey())
                    || readable.size() != manifest.getSizeBytes() || !readable.sha256().equals(manifest.getSha256()))
                throw failedPrecondition("Archive manifest disagrees with its published storage binding");
            if (readable.size() > Integer.MAX_VALUE)
                throw new RepositoryException(RepositoryException.Code.RESOURCE_EXHAUSTED,
                        "Archive object exceeds the byte-array read limit");
            var store = backends.resolve(location.backendGeneration(), binding.storageRealm());
            if (store == null) throw failedPrecondition("Original archive backend is not available");
            BlobStore.GetResult result;
            try {
                result = store.getBounded(location.bucket(), location.objectKey(), readable.providerVersion(), (int) readable.size());
            } catch (BlobStore.BlobReadLimitException oversized) {
                throw new RepositoryException(RepositoryException.Code.DATA_LOSS,
                        "Archive object exceeds its published size", oversized);
            } catch (UnsupportedOperationException unsupported) {
                throw new RepositoryException(RepositoryException.Code.FAILED_PRECONDITION,
                        "Archive backend does not support bounded reads", unsupported);
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
}
