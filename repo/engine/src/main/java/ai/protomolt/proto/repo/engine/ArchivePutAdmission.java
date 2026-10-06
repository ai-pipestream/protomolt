package ai.protomolt.proto.repo.engine;

import ai.protomolt.proto.repo.archive.v1.PutEntryRequest;
import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.RepositoryException;
import java.time.Duration;
import java.util.Objects;

/**
 * Shared admission for already-decoded unary archive requests. This bounds active
 * calls and reserves payload/copy allowances before the engine allocates copies.
 * Transport decoding, retained historical manifests and SDK heap require separate
 * host bounds; this is not a total JVM heap limit or a streaming capability.
 */
public final class ArchivePutAdmission implements AutoCloseable {
    public record Limits(int maxObjectBytes, int maxRequestBytes, int maxRenditions) {
        public Limits {
            if (maxObjectBytes < 1 || maxRequestBytes < maxObjectBytes || maxRequestBytes > 256 * 1024 * 1024
                    || maxRenditions < 1 || maxRenditions > 256)
                throw new IllegalArgumentException("Invalid bounded archive put limits");
        }
    }

    private final Limits limits;
    private final PayloadBudget budget;
    private final int maxActive;
    private int active;
    private boolean closed;

    public ArchivePutAdmission(Limits limits, PayloadBudget budget, int maxActive) {
        this.limits = Objects.requireNonNull(limits); this.budget = Objects.requireNonNull(budget);
        if (maxActive < 1 || maxActive > 1024) throw new IllegalArgumentException("Archive put concurrency must be between 1 and 1024");
        this.maxActive = maxActive;
    }

    public Scope admit(PutEntryRequest request) {
        Objects.requireNonNull(request);
        if (Thread.currentThread().isInterrupted()) throw new RepositoryException(RepositoryException.Code.CANCELLED,
                "Archive put interrupted before admission");
        if (request.getRenditionsCount() > limits.maxRenditions()) throw invalid("Archive rendition count exceeds limit");
        long payload = 0;
        for (var rendition : request.getRenditionsList()) {
            int bytes = rendition.getData().size();
            if (bytes > limits.maxObjectBytes() || bytes > limits.maxRequestBytes() - payload)
                throw invalid("Archive inline payload exceeds limit");
            payload += bytes;
        }
        int serialized = request.getSerializedSize();
        if (serialized <= 0 || serialized > limits.maxRequestBytes()) throw invalid("Archive request exceeds limit");
        // Request payload plus up to three engine copies and one provider-copy
        // allowance. Reservations measure admission, not exact SDK/JVM allocation.
        long bytes = serialized + 4 * payload;
        synchronized (this) {
            if (closed) throw new RepositoryException(RepositoryException.Code.UNAVAILABLE, "Archive put admission is closed");
            if (active >= maxActive) throw exhausted();
            PayloadBudget.Lease lease;
            try { lease = budget.reserve(bytes); }
            catch (PayloadBudget.CapacityExceededException capacity) {
                throw new RepositoryException(RepositoryException.Code.RESOURCE_EXHAUSTED, "Archive payload capacity exhausted", capacity);
            }
            active++;
            return new Scope(lease);
        }
    }

    public final class Scope implements AutoCloseable {
        private final PayloadBudget.Lease lease;
        private boolean released;
        private Scope(PayloadBudget.Lease lease) { this.lease = lease; }
        @Override public void close() {
            synchronized (ArchivePutAdmission.this) {
                if (released) return;
                released = true; lease.close(); active--;
                ArchivePutAdmission.this.notifyAll();
            }
        }
    }

    @Override public synchronized void close() { closed = true; }

    /** Closing stops new admission; accepted provider calls must finish before their scopes close. */
    public synchronized boolean awaitIdle(Duration timeout) throws InterruptedException {
        Objects.requireNonNull(timeout);
        if (!closed) throw new IllegalStateException("Close archive put admission before awaiting idle");
        if (timeout.isNegative()) throw new IllegalArgumentException("Negative archive drain timeout");
        long remaining = timeout.toNanos(), start = System.nanoTime();
        while (active != 0) {
            if (remaining <= 0) return false;
            java.util.concurrent.TimeUnit.NANOSECONDS.timedWait(this, remaining);
            remaining = timeout.toNanos() - (System.nanoTime() - start);
        }
        return true;
    }

    private static RepositoryException invalid(String message) {
        return new RepositoryException(RepositoryException.Code.INVALID_ARGUMENT, message);
    }
    private static RepositoryException exhausted() {
        return new RepositoryException(RepositoryException.Code.RESOURCE_EXHAUSTED, "Archive put concurrency exhausted");
    }
}
