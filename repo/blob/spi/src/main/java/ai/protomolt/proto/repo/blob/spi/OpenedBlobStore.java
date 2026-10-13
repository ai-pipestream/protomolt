package ai.protomolt.proto.repo.blob.spi;

import java.util.Objects;
import java.util.Set;

/** A selected store and its caller-owned lifetime, separate from the byte operations. */
public final class OpenedBlobStore implements AutoCloseable {
    private final BlobStore store;
    private final AutoCloseable lifetime;
    private final Set<BlobCapability> capabilities;
    private final NamespaceProvisioner namespaces;
    private final ObjectReclaimer reclaimer;
    private final ExactVersionReclaimer exactVersionReclaimer;
    private boolean closed;

    public OpenedBlobStore(BlobStore store, AutoCloseable lifetime) {
        this(store, lifetime, Set.of());
    }

    public OpenedBlobStore(BlobStore store, AutoCloseable lifetime, Set<BlobCapability> capabilities) {
        this(store, lifetime, capabilities, name -> {
            throw new UnsupportedOperationException("Namespace provisioning is unsupported");
        });
    }

    public OpenedBlobStore(BlobStore store, AutoCloseable lifetime, Set<BlobCapability> capabilities,
            NamespaceProvisioner namespaces) {
        this(store, lifetime, capabilities, namespaces, unsupportedReclaimer(capabilities));
    }

    /** An advertised exact-version capability requires explicitly forwarding its port. */
    public OpenedBlobStore(BlobStore store, AutoCloseable lifetime, Set<BlobCapability> capabilities,
            NamespaceProvisioner namespaces, ObjectReclaimer reclaimer) {
        this(store, lifetime, capabilities, namespaces, reclaimer,
                unsupportedExactVersionReclaimer(capabilities));
    }

    public OpenedBlobStore(BlobStore store, AutoCloseable lifetime, Set<BlobCapability> capabilities,
            NamespaceProvisioner namespaces, ObjectReclaimer reclaimer,
            ExactVersionReclaimer exactVersionReclaimer) {
        this.store = Objects.requireNonNull(store, "store");
        this.lifetime = Objects.requireNonNull(lifetime, "lifetime");
        this.capabilities = Set.copyOf(capabilities);
        this.namespaces = Objects.requireNonNull(namespaces, "namespaces");
        this.reclaimer = Objects.requireNonNull(reclaimer, "reclaimer");
        this.exactVersionReclaimer = Objects.requireNonNull(exactVersionReclaimer, "exactVersionReclaimer");
    }

    public synchronized ObjectReclaimer reclaimer() {
        if (closed) throw new IllegalStateException("Storage handle is closed");
        return reclaimer;
    }

    public synchronized ExactVersionReclaimer exactVersionReclaimer() {
        if (closed) throw new IllegalStateException("Storage handle is closed");
        return exactVersionReclaimer;
    }

    private static ObjectReclaimer unsupportedReclaimer(Set<BlobCapability> capabilities) {
        if (capabilities.contains(BlobCapability.PHYSICAL_RECLAMATION))
            throw new IllegalArgumentException("PHYSICAL_RECLAMATION requires a reclaimer port");
        return (bucket, key) -> { throw new UnsupportedOperationException("Physical object reclamation is unsupported"); };
    }

    private static ExactVersionReclaimer unsupportedExactVersionReclaimer(Set<BlobCapability> capabilities) {
        if (capabilities.contains(BlobCapability.EXACT_VERSION_RECLAMATION))
            throw new IllegalArgumentException("EXACT_VERSION_RECLAMATION requires a reclaimer port");
        return (bucket, key, versionId) -> { throw new UnsupportedOperationException("Exact-version reclamation is unsupported"); };
    }

    public synchronized void ensureNamespace(String namespace) {
        if (closed) throw new IllegalStateException("Storage handle is closed");
        namespaces.ensureNamespace(namespace);
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
