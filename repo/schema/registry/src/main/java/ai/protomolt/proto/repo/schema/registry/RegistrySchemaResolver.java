package ai.protomolt.proto.repo.schema.registry;

import ai.protomolt.proto.registry.SchemaRegistryStore;
import ai.protomolt.proto.repo.admission.DocumentSchemaAdmission;
import ai.protomolt.proto.repo.admission.DocumentSchemaArtifactCache;
import ai.protomolt.proto.repo.v1.RepositorySchemaAsset;
import com.google.protobuf.ByteString;
import java.util.LinkedHashMap;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Host-scoped registry adapter. The borrowed store and owned cache belong to one
 * authenticated registry/security context; this object is not an authorization
 * service. It caches descriptor bytes only, never discovery or admission results.
 */
public final class RegistrySchemaResolver implements AutoCloseable {
    /** Host-owned metadata and optional source bundle, stable through the admission attempt. */
    public record Selected(RepositorySchemaAsset metadata, Optional<ByteString> source) {
        public Selected { Objects.requireNonNull(metadata); Objects.requireNonNull(source); }
    }

    /**
     * Authorize and select an exact definition for every occurrence, including cache
     * hits. The host supplies genuine compiler provenance; this adapter never infers
     * it from a registry ID or treats a metadata claim as permission. Failures throw.
     */
    @FunctionalInterface public interface Selector {
        Selected select(DocumentSchemaAdmission.Selection occurrence);
    }

    public static final class MissingDescriptor extends IllegalStateException {
        private MissingDescriptor() { super("Selected descriptor is absent from the registry"); }
    }

    private final SchemaRegistryStore store;
    private final DocumentSchemaArtifactCache cache;
    private final Semaphore attempts;
    private final int maxArtifactsPerAttempt;
    private final AtomicBoolean closed = new AtomicBoolean();

    public RegistrySchemaResolver(SchemaRegistryStore store, DocumentSchemaArtifactCache.Limits cacheLimits,
            int maxConcurrentAttempts, int maxArtifactsPerAttempt) {
        this.store = Objects.requireNonNull(store);
        if (maxConcurrentAttempts < 1 || maxArtifactsPerAttempt < 1 || maxArtifactsPerAttempt > 64)
            throw new IllegalArgumentException("Invalid registry resolution limits");
        if (!store.supportsDescriptorSets()) throw new UnsupportedOperationException("Registry cannot supply descriptor artifacts");
        cache = new DocumentSchemaArtifactCache(cacheLimits);
        attempts = new Semaphore(maxConcurrentAttempts);
        this.maxArtifactsPerAttempt = maxArtifactsPerAttempt;
    }

    /** Nonblocking admission of a thread-confined resolution scope. */
    public Attempt open(Selector selector, Runnable control) {
        Objects.requireNonNull(selector); Objects.requireNonNull(control).run();
        requireOpen();
        if (!attempts.tryAcquire()) throw new IllegalStateException("Registry resolution capacity exhausted");
        try {
            requireOpen(); control.run();
            return new Attempt(selector, control);
        } catch (RuntimeException | Error failure) { attempts.release(); throw failure; }
    }

    public long cachedBytes() { return cache.ownedBytes(); }
    private void requireOpen() { if (closed.get()) throw new IllegalStateException("Registry resolver closed"); }

    /** Stop new selections; live attempts retain their pins until explicitly closed. Does not close the store. */
    @Override public void close() { closed.set(true); cache.close(); }

    /**
     * Keep open through admission's owned copies. Returned definitions borrow pinned
     * descriptors and host-owned metadata/source; do not retain them after close.
     * Synchronous store calls check cancellation before and after I/O. The store must
     * bound its own allocation and I/O timeout; this adapter cannot interrupt an
     * arbitrary synchronous provider. Use the owning thread for selection and close.
     */
    public final class Attempt implements DocumentSchemaAdmission.Resolution {
        private final Thread thread = Thread.currentThread();
        private final Selector selector;
        private final Runnable control;
        private final LinkedHashMap<String, DocumentSchemaArtifactCache.Lease> retained = new LinkedHashMap<>();
        private boolean ended;
        private Attempt(Selector selector, Runnable control) { this.selector = selector; this.control = control; }

        @Override public DocumentSchemaAdmission.Definition select(DocumentSchemaAdmission.Selection occurrence) {
            requireThread();
            if (ended) throw new IllegalStateException("Registry resolution attempt closed");
            requireOpen(); control.run();
            var selected = Objects.requireNonNull(selector.select(Objects.requireNonNull(occurrence)), "Selected schema");
            control.run(); requireOpen();
            var metadata = selected.metadata();
            if (metadata.getSerializedSize() > 512 * 1024 || !metadata.getTypeUrl().equals(occurrence.typeUrl()))
                throw new IllegalArgumentException("Selected metadata differs from occurrence or exceeds limit");
            if (selected.source().filter(value -> value.isEmpty() || value.size() > 16 * 1024 * 1024).isPresent()
                    || metadata.getCompilation().hasSourceArtifactSha256() != selected.source().isPresent())
                throw new IllegalArgumentException("Selected source differs from provenance or exceeds limit");
            String digest = metadata.getArtifactSha256();
            var lease = retained.get(digest);
            if (lease == null) {
                if (retained.size() >= maxArtifactsPerAttempt)
                    throw new IllegalStateException("Resolution artifact count exhausted");
                lease = cache.acquire(digest, control).orElse(null);
                if (lease == null) {
                    var bytes = store.descriptorSet(digest).orElseThrow(MissingDescriptor::new);
                    control.run(); requireOpen();
                    if (bytes.isEmpty() || bytes.size() > 16 * 1024 * 1024)
                        throw new IllegalArgumentException("Registry descriptor exceeds admission bounds");
                    lease = cache.put(digest, bytes, control);
                }
                boolean transferred = false;
                try {
                    control.run(); requireOpen();
                    retained.put(digest, lease); transferred = true;
                } finally { if (!transferred) lease.close(); }
            }
            control.run(); requireOpen();
            // Admission still validates metadata, source digest, descriptor closure and candidate.
            return new DocumentSchemaAdmission.Definition(metadata, lease.bytes(), selected.source());
        }

        private void requireThread() {
            if (Thread.currentThread() != thread) throw new IllegalStateException("Resolution attempt belongs to another thread");
        }
        @Override public void close() {
            requireThread();
            if (ended) return;
            ended = true;
            retained.values().forEach(DocumentSchemaArtifactCache.Lease::close);
            retained.clear(); attempts.release();
        }
    }
}
