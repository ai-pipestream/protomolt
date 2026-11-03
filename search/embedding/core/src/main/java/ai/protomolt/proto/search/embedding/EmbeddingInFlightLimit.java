package ai.protomolt.proto.search.embedding;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

/**
 * Interruptible RPC admission shared by a provider family through a static instance.
 * Overlapping operations must request the same positive cap; conflicting configuration
 * fails explicitly. The cap can change once all admitted and waiting calls have left.
 * No threads or request payload queue are allocated here; waiting happens on caller threads.
 */
public final class EmbeddingInFlightLimit {
    private final ReentrantLock lock = new ReentrantLock(true);
    private final Condition available = lock.newCondition();
    private final long admissionNanos;
    private int participants;
    private int inFlight;
    private int limit;

    /** Creates a limit with a 30 second admission timeout, separate from the RPC deadline. */
    public EmbeddingInFlightLimit() {
        this(java.time.Duration.ofSeconds(30));
    }

    EmbeddingInFlightLimit(java.time.Duration admissionTimeout) {
        admissionNanos = admissionTimeout.toNanos();
        if (admissionNanos <= 0) {
            throw new IllegalArgumentException("Admission timeout must be positive");
        }
    }

    /** Executes one RPC, releasing admission on success, failure or cancellation. */
    public <T> T execute(int requestedLimit, Supplier<T> operation) {
        if (requestedLimit < 1) {
            throw new IllegalArgumentException("Embedding in-flight cap must be positive");
        }
        try {
            enter(requestedLimit);
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for embedding admission", failure);
        }
        try {
            return operation.get();
        } finally {
            lock.lock();
            try {
                inFlight--;
                participants--;
                available.signalAll();
            } finally {
                lock.unlock();
            }
        }
    }

    private void enter(int requestedLimit) throws InterruptedException {
        lock.lockInterruptibly();
        try {
            if (participants != 0 && limit != requestedLimit) {
                throw new IllegalStateException("Conflicting process-wide embedding in-flight caps: active "
                        + limit + ", requested " + requestedLimit);
            }
            limit = requestedLimit;
            participants++;
            try {
                long remaining = admissionNanos;
                while (inFlight >= limit) {
                    if (remaining <= 0) {
                        throw new IllegalStateException("Timed out waiting for embedding admission",
                                new java.util.concurrent.TimeoutException("Embedding admission timeout"));
                    }
                    remaining = available.awaitNanos(remaining);
                }
                inFlight++;
            } catch (InterruptedException | RuntimeException failure) {
                participants--;
                available.signalAll();
                throw failure;
            }
        } finally {
            lock.unlock();
        }
    }
}
