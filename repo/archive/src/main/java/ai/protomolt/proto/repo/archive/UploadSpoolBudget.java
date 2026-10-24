package ai.protomolt.proto.repo.archive;

import io.grpc.Status;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The local disk bytes one host lets primary-rendition uploads stage before
 * their single {@code UploadHeader} goes out. An upload that passes the
 * in-memory characterization window reserves its declared {@code size_bytes}
 * (plus the one over-length byte the spool keeps) before it writes a spool file,
 * and releases it when the spool is deleted. A reservation the remaining budget
 * does not cover fails fast with {@code RESOURCE_EXHAUSTED}.
 */
public final class UploadSpoolBudget {

    /** Default capacity: 4 GiB of staged upload bytes per host. */
    public static final long DEFAULT_CAPACITY_BYTES = 4L * 1024 * 1024 * 1024;

    private final long capacityBytes;
    private final AtomicLong reservedBytes = new AtomicLong();

    /**
     * @param capacityBytes the most spool bytes held at once; positive
     */
    public UploadSpoolBudget(long capacityBytes) {
        if (capacityBytes <= 0) {
            throw new IllegalArgumentException("capacityBytes must be positive: " + capacityBytes);
        }
        this.capacityBytes = capacityBytes;
    }

    /** The configured capacity in bytes. */
    public long capacityBytes() {
        return capacityBytes;
    }

    /** The bytes currently reserved. */
    public long reservedBytes() {
        return reservedBytes.get();
    }

    /**
     * Reserves {@code bytes} of spool space.
     *
     * @param bytes the bytes to reserve; positive
     * @return the lease that gives the bytes back once
     * @throws io.grpc.StatusRuntimeException {@code RESOURCE_EXHAUSTED} when the
     *         remaining budget does not cover {@code bytes}
     */
    public Lease reserve(long bytes) {
        if (bytes <= 0) {
            throw new IllegalArgumentException("bytes must be positive: " + bytes);
        }
        while (true) {
            long held = reservedBytes.get();
            if (bytes > capacityBytes - held) {
                throw Status.RESOURCE_EXHAUSTED
                        .withDescription("Upload spool budget exhausted: " + bytes + " bytes requested, "
                                + (capacityBytes - held) + " of " + capacityBytes + " available")
                        .asRuntimeException();
            }
            if (reservedBytes.compareAndSet(held, held + bytes)) {
                return new Lease(bytes);
            }
        }
    }

    /** Reserved spool bytes; {@link #close()} gives them back once. */
    public final class Lease implements AutoCloseable {
        private final long bytes;
        private final AtomicBoolean released = new AtomicBoolean();

        private Lease(long bytes) {
            this.bytes = bytes;
        }

        @Override
        public void close() {
            if (released.compareAndSet(false, true)) {
                reservedBytes.addAndGet(-bytes);
            }
        }
    }
}
