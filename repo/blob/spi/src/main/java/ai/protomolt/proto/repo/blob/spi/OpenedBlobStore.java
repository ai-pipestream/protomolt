package ai.protomolt.proto.repo.blob.spi;

import java.util.Objects;
import java.util.Set;

/** A selected store and its caller-owned lifetime, separate from the byte operations. */
public final class OpenedBlobStore implements AutoCloseable {
    private final BlobStore store;
    private final AutoCloseable lifetime;
    private final Set<BlobCapability> capabilities;
    private boolean closed;

    public OpenedBlobStore(BlobStore store, AutoCloseable lifetime) {
        this(store, lifetime, Set.of());
    }

    public OpenedBlobStore(BlobStore store, AutoCloseable lifetime, Set<BlobCapability> capabilities) {
        this.store = Objects.requireNonNull(store, "store");
        this.lifetime = Objects.requireNonNull(lifetime, "lifetime");
        this.capabilities = Set.copyOf(capabilities);
    }

    public Set<BlobCapability> capabilities() { return capabilities; }

    public synchronized BlobStore store() {
        if (closed) throw new IllegalStateException("Storage handle is closed");
        return store;
    }

    /** Cleanup is attempted once; its failure remains observable to the closing caller. */
    @Override
    public synchronized void close() throws Exception {
        if (closed) return;
        closed = true;
        lifetime.close();
    }
}
