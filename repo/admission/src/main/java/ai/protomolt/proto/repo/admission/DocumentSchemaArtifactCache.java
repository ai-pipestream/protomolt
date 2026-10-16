package ai.protomolt.proto.repo.admission;

import com.google.protobuf.ByteString;
import java.util.LinkedHashMap;
import java.util.Objects;
import java.util.Optional;

/**
 * Host-scoped, content-addressed schema bytes. This cache supplies neither discovery
 * nor authorization, validation verdicts or archival retention. Hosts must authorize
 * selection before lookup and use separate instances for different security contexts.
 * Serialized-byte limits do not bound parsed descriptors or other JVM heap overhead.
 */
public final class DocumentSchemaArtifactCache implements AutoCloseable {
    public record Limits(long maxBytes, int maxEntries, int maxArtifactBytes) {
        public Limits {
            if (maxBytes < 1 || maxEntries < 1 || maxArtifactBytes < 1 || maxArtifactBytes > maxBytes)
                throw new IllegalArgumentException("Invalid schema cache limits");
        }
    }

    public static final class CapacityExceeded extends IllegalStateException {
        private CapacityExceeded() { super("Schema cache capacity exhausted"); }
    }

    private static final class Entry {
        final ByteString bytes;
        int pins;
        Entry(ByteString bytes) { this.bytes = bytes; }
    }

    private final Limits limits;
    private final LinkedHashMap<String, Entry> entries = new LinkedHashMap<>(16, .75f, true);
    private long ownedBytes;
    private int pending;
    private boolean closed;

    public DocumentSchemaArtifactCache(Limits limits) { this.limits = Objects.requireNonNull(limits); }

    /** Includes in-flight copies and pinned entries after shutdown. */
    public synchronized long ownedBytes() { return ownedBytes; }

    /** Lookup is by verified artifact bytes, not type URL, registry ID or discovery grant. */
    public Optional<Lease> acquire(String sha256, Runnable control) {
        requireDigest(sha256); Objects.requireNonNull(control).run();
        Lease lease;
        synchronized (this) {
            requireOpen();
            var entry = entries.get(sha256);
            lease = entry == null ? null : pin(entry);
        }
        try {
            control.run();
            return Optional.ofNullable(lease);
        } catch (RuntimeException | Error failure) {
            if (lease != null) lease.close();
            throw failure;
        }
    }

    /**
     * Copy and verify borrowed immutable input. The caller owns input allocation;
     * the cache reserves its own capacity before copying. Concurrent insertions are
     * bounded independently; this primitive does not coalesce external registry I/O.
     * Insertion needs twice the artifact size for the temporary array and retained
     * copy; after insertion only the retained size remains reserved. Use acquire
     * for an already selected digest instead of reinserting it at full capacity.
     */
    public Lease put(String sha256, ByteString bytes, Runnable control) {
        requireDigest(sha256); Objects.requireNonNull(bytes); Objects.requireNonNull(control).run();
        int size = bytes.size();
        long copyingBytes = 2L * size;
        synchronized (this) {
            requireOpen();
            if (size > limits.maxArtifactBytes()) throw new CapacityExceeded();
            makeRoom(copyingBytes);
            ownedBytes += copyingBytes;
            pending++;
        }
        boolean transferred = false;
        try {
            control.run();
            // Copy only after reservation; never retain an input slice's larger backing buffer.
            var copy = ByteString.copyFrom(bytes.toByteArray());
            if (!DocumentSchemaOccurrences.sha256(copy, control).equals(sha256))
                throw new IllegalArgumentException("Schema artifact digest mismatch");
            control.run();
            synchronized (this) {
                requireOpen();
                var existing = entries.get(sha256);
                if (existing != null) return pin(existing);
                var entry = new Entry(copy);
                entries.put(sha256, entry);
                pending--;
                ownedBytes -= size;
                transferred = true;
                return pin(entry);
            }
        } finally {
            if (!transferred) synchronized (this) { pending--; ownedBytes -= copyingBytes; }
        }
    }

    private void makeRoom(long bytes) {
        var iterator = entries.entrySet().iterator();
        while ((bytes > limits.maxBytes() - ownedBytes || entries.size() + pending >= limits.maxEntries())
                && iterator.hasNext()) {
            var entry = iterator.next().getValue();
            if (entry.pins == 0) { iterator.remove(); ownedBytes -= entry.bytes.size(); }
        }
        if (bytes > limits.maxBytes() - ownedBytes || entries.size() + pending >= limits.maxEntries())
            throw new CapacityExceeded();
    }

    private Lease pin(Entry entry) { entry.pins++; return new Lease(entry); }
    private void requireOpen() { if (closed) throw new IllegalStateException("Schema cache closed"); }
    private static void requireDigest(String value) {
        Objects.requireNonNull(value);
        if (value.length() != 64 || !value.chars().allMatch(c -> c >= '0' && c <= '9' || c >= 'a' && c <= 'f'))
            throw new IllegalArgumentException("Expected lowercase SHA-256");
    }

    /** Reject new work immediately; existing leases remain valid until their owners close them. */
    @Override public synchronized void close() {
        closed = true;
        var iterator = entries.values().iterator();
        while (iterator.hasNext()) {
            var entry = iterator.next();
            if (entry.pins == 0) { iterator.remove(); ownedBytes -= entry.bytes.size(); }
        }
    }

    /** Borrow bytes only while this lease is open, including through admission's budgeted copy. */
    public final class Lease implements AutoCloseable {
        private Entry entry;
        private Lease(Entry entry) { this.entry = entry; }
        public ByteString bytes() {
            synchronized (DocumentSchemaArtifactCache.this) {
                if (entry == null) throw new IllegalStateException("Schema cache lease closed");
                return entry.bytes;
            }
        }
        @Override public void close() {
            synchronized (DocumentSchemaArtifactCache.this) {
                if (entry == null) return;
                entry.pins--;
                if (closed && entry.pins == 0) {
                    entries.values().remove(entry);
                    ownedBytes -= entry.bytes.size();
                }
                entry = null;
            }
        }
    }
}
