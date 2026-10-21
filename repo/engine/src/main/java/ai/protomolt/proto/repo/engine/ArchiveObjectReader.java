package ai.protomolt.proto.repo.engine;

import ai.protomolt.proto.repo.archive.v1.RenditionManifestEntry;
import ai.protomolt.proto.repo.archive.v1.RenditionState;
import ai.protomolt.proto.repo.blob.spi.BlobStore;
import ai.protomolt.proto.repo.container.archive.ArchiveEntryRecord;
import ai.protomolt.proto.repo.container.archive.ArchiveManifests;
import ai.protomolt.proto.repo.container.archive.ArchiveReadLedger;
import ai.protomolt.proto.repo.spi.RepositoryException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import java.util.UUID;
import static ai.protomolt.proto.repo.engine.RepositoryErrors.failedPrecondition;

/** Reads published objects by immutable backend identity after caller authorization. */
public final class ArchiveObjectReader implements AutoCloseable {
    /**
     * Host-owned clients are borrowed, never closed by the reader. Resolve the exact
     * persisted generation and realm, or raise {@link UnservedBackendGenerationException};
     * the reader reports that refusal as FAILED_PRECONDITION with the exception as cause.
     * Current defaults are not substitutes. Provider configuration and credential rotation
     * remain host responsibilities.
     */
    @FunctionalInterface
    public interface BackendResolver {
        BlobStore resolve(String generation, String storageRealm);
    }

    /** Constant for callers and transports; the resolver's detail stays on the cause chain. */
    static final String BACKEND_NOT_CONFIGURED = "Original archive backend is not configured on this host";

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
     * Opens a streamed read of {@code length} bytes from {@code offset}, or of the
     * whole object when {@code length} is negative, from the exact published version.
     * The storage read pin and this reader's active-read count are held until the
     * returned stream closes, so {@link #awaitIdle} waits for open streams. The
     * stream is not verified here; callers check its length and digest.
     */
    public InputStream open(ArchiveEntryRecord entry, long version, RenditionManifestEntry manifest,
            long offset, long length) {
        synchronized (lifecycle) {
            if (closed) throw new RepositoryException(RepositoryException.Code.CANCELLED, "Archive reader is closed");
            activeReads++;
        }
        boolean handedOff = false;
        try {
            var opened = openPinned(entry, version, manifest, offset, length);
            handedOff = true;
            return opened;
        } finally {
            if (!handedOff) endRead();
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

    /** Persist local drain evidence before the host releases its shared resources. */
    public void attestLocalQuiescence() {
        synchronized (lifecycle) {
            if (!closed || !fenceComplete || activeReads != 0)
                throw new IllegalStateException("Reader must finish shutdown before attesting quiescence");
        }
        reads.attestLocalQuiescence();
    }

    private void endRead() {
        synchronized (lifecycle) { activeReads--; lifecycle.notifyAll(); }
    }

    private ArchiveReadLedger.Pin pin(ArchiveEntryRecord entry, long version, RenditionManifestEntry manifest) {
        if (manifest.getState() != RenditionState.RENDITION_STATE_PRESENT)
            throw failedPrecondition("Only present archive objects may be read");
        UUID objectId;
        try { objectId = UUID.fromString(manifest.getStorageObjectId()); }
        catch (IllegalArgumentException invalid) { throw failedPrecondition("Invalid archive storage binding identity"); }
        var pin = reads.acquire(entry.entryUuid, version, objectId)
                .orElseThrow(() -> failedPrecondition("Archive version has no published storage reference"));
        try {
            var readable = pin.readable();
            var location = readable.binding().location();
            if (!location.accountId().equals(entry.accountId) || !location.archive().equals(entry.archive)
                    || !location.objectKey().equals(manifest.getObjectKey())
                    || readable.size() != manifest.getSizeBytes() || !readable.sha256().equals(manifest.getSha256()))
                throw failedPrecondition("Archive manifest disagrees with its published storage binding");
            return pin;
        } catch (RuntimeException | Error failure) {
            pin.close();
            throw failure;
        }
    }

    private BlobStore store(ArchiveReadLedger.Pin pin) {
        var binding = pin.readable().binding();
        BlobStore store;
        try { store = backends.resolve(binding.location().backendGeneration(), binding.storageRealm()); }
        catch (UnservedBackendGenerationException unserved) {
            throw new RepositoryException(RepositoryException.Code.FAILED_PRECONDITION, BACKEND_NOT_CONFIGURED, unserved);
        }
        if (store == null) throw failedPrecondition("Original archive backend is not available");
        return store;
    }

    private InputStream openPinned(ArchiveEntryRecord entry, long version, RenditionManifestEntry manifest,
            long offset, long length) {
        var pin = pin(entry, version, manifest);
        try {
            var readable = pin.readable();
            var location = readable.binding().location();
            var store = store(pin);
            InputStream body;
            try {
                body = store.openStream(location.bucket(), location.objectKey(), readable.providerVersion(), offset, length);
            } catch (UnsupportedOperationException unsupported) {
                throw new RepositoryException(RepositoryException.Code.FAILED_PRECONDITION,
                        "Archive backend does not support streamed reads", unsupported);
            } catch (BlobStore.BlobNotFoundException missing) {
                throw new RepositoryException(RepositoryException.Code.DATA_LOSS,
                        "Published archive object is missing from its original backend", missing);
            }
            return new FilterInputStream(body) {
                private boolean released;
                @Override public void close() throws IOException {
                    if (released) return;
                    released = true;
                    try { super.close(); }
                    finally {
                        try { pin.close(); }
                        finally { endRead(); }
                    }
                }
            };
        } catch (RuntimeException | Error failure) {
            pin.close();
            throw failure;
        }
    }

    private BlobStore.GetResult readPinned(ArchiveEntryRecord entry, long version, RenditionManifestEntry manifest) {
        try (var pin = pin(entry, version, manifest)) {
            var readable = pin.readable();
            var location = readable.binding().location();
            if (readable.size() > Integer.MAX_VALUE)
                throw new RepositoryException(RepositoryException.Code.RESOURCE_EXHAUSTED,
                        "Archive object exceeds the byte-array read limit");
            BlobStore store = store(pin);
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
