package ai.protomolt.proto.repo.blob.spi;

/**
 * Shared, nonblocking accounting for active payload bytes. Hosts share one instance
 * across cooperating readers/writers. This measures reservations, not JVM heap:
 * SDK buffers, object overhead and externally retained arrays are not included.
 */
public final class PayloadBudget {
    private final long capacity;
    private long reserved;

    public PayloadBudget(long capacity) {
        if (capacity <= 0) throw new IllegalArgumentException("Payload capacity must be positive");
        this.capacity = capacity;
    }

    public long capacity() { return capacity; }
    public synchronized long reservedBytes() { return reserved; }

    /** Reserve before allocation/I/O; capacity exhaustion never waits holding another lease. */
    public synchronized Lease reserve(long bytes) {
        if (bytes < 0) throw new IllegalArgumentException("Payload reservation must not be negative");
        if (bytes > capacity - reserved) throw new CapacityExceededException();
        var lease = new Lease(this, bytes);
        reserved += bytes;
        return lease;
    }

    public static final class CapacityExceededException extends IllegalStateException {
        private CapacityExceededException() { super("Payload byte capacity exhausted"); }
    }

    /** Ownership may cross threads. Close only after the last user/worker relinquishes its bytes. */
    public static final class Lease implements AutoCloseable {
        private final PayloadBudget owner;
        private final long bytes;
        // Accessed only under owner's monitor, making accounting and idempotent close atomic.
        private boolean closed;

        private Lease(PayloadBudget owner, long bytes) { this.owner = owner; this.bytes = bytes; }
        public long bytes() { return bytes; }
        @Override public void close() {
            synchronized (owner) {
                if (closed) return;
                closed = true;
                owner.reserved -= bytes;
            }
        }
    }
}
