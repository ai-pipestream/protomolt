package ai.protomolt.proto.search.embedding;

import java.time.Duration;
import java.util.concurrent.TimeoutException;
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

    /** Admission wait used when a caller does not choose its own: 30 seconds. */
    public static final Duration DEFAULT_ADMISSION_TIMEOUT = Duration.ofSeconds(30);

    /**
     * Longest configurable admission wait in whole seconds: {@value}, the largest whole
     * number of seconds whose nanosecond count fits in a {@code long}.
     */
    public static final long MAX_ADMISSION_TIMEOUT_SECONDS = Long.MAX_VALUE / 1_000_000_000L;

    /** Creates a limit with a 30 second admission timeout, separate from the RPC deadline. */
    public EmbeddingInFlightLimit() {
        this(DEFAULT_ADMISSION_TIMEOUT);
    }

    EmbeddingInFlightLimit(Duration admissionTimeout) {
        admissionNanos = requirePositive(admissionTimeout);
    }

    /** Executes one RPC with this limit's admission timeout, releasing admission on every exit. */
    public <T> T execute(int requestedLimit, Supplier<T> operation) {
        return execute(requestedLimit, admissionNanos, operation);
    }

    /**
     * Executes one RPC, waiting at most {@code admissionTimeout} for admission. The timeout
     * belongs to this call only; unlike the cap, overlapping callers need not agree on it.
     */
    public <T> T execute(int requestedLimit, Duration admissionTimeout, Supplier<T> operation) {
        return execute(requestedLimit, requirePositive(admissionTimeout), operation);
    }

    private <T> T execute(int requestedLimit, long waitNanos, Supplier<T> operation) {
        if (requestedLimit < 1) {
            throw new IllegalArgumentException("Embedding in-flight cap must be positive");
        }
        try {
            enter(requestedLimit, waitNanos);
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

    /**
     * Parses a configured admission timeout in whole seconds. {@code null} means
     * {@link #DEFAULT_ADMISSION_TIMEOUT}; anything else must be a positive integer.
     *
     * @param configured the configured value, or {@code null} when unset
     * @param source the property and environment names, for the error message
     * @throws IllegalStateException when the value is not a positive integer no greater than
     *     {@link #MAX_ADMISSION_TIMEOUT_SECONDS}
     */
    public static Duration admissionTimeoutSeconds(String configured, String source) {
        if (configured == null) {
            return DEFAULT_ADMISSION_TIMEOUT;
        }
        long seconds;
        try {
            seconds = Long.parseLong(configured.trim());
        } catch (NumberFormatException failure) {
            throw new IllegalStateException("Invalid " + source + ": " + configured, failure);
        }
        if (seconds < 1) {
            throw new IllegalStateException(source + " must be a positive number of seconds: " + configured);
        }
        if (seconds > MAX_ADMISSION_TIMEOUT_SECONDS) {
            throw new IllegalStateException(source + " must be at most " + MAX_ADMISSION_TIMEOUT_SECONDS
                    + " seconds: " + configured);
        }
        return Duration.ofSeconds(seconds);
    }

    private static long requirePositive(Duration admissionTimeout) {
        long nanos;
        try {
            nanos = admissionTimeout.toNanos();
        } catch (ArithmeticException overflow) {
            throw new IllegalArgumentException("Admission timeout is too long to represent in nanoseconds: "
                    + admissionTimeout, overflow);
        }
        if (nanos <= 0) {
            throw new IllegalArgumentException("Admission timeout must be positive");
        }
        return nanos;
    }

    private void enter(int requestedLimit, long waitNanos) throws InterruptedException {
        lock.lockInterruptibly();
        try {
            if (participants != 0 && limit != requestedLimit) {
                throw new IllegalStateException("Conflicting process-wide embedding in-flight caps: active "
                        + limit + ", requested " + requestedLimit);
            }
            limit = requestedLimit;
            participants++;
            try {
                long remaining = waitNanos;
                while (inFlight >= limit) {
                    if (remaining <= 0) {
                        throw new IllegalStateException("Timed out waiting for embedding admission",
                                new TimeoutException("Embedding admission timeout"));
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
