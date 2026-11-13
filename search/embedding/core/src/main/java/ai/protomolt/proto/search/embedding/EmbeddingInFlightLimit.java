package ai.protomolt.proto.search.embedding;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

/**
 * Interruptible RPC admission shared by a provider family through a static instance.
 * Overlapping operations must request the same positive cap; conflicting configuration
 * fails explicitly. The cap can change once all admitted and waiting calls have left.
 * No threads or request payload queue are allocated here; waiting happens on caller threads.
 */
public final class EmbeddingInFlightLimit {
    private static final String POSITIVE_CAP_MESSAGE = "Embedding in-flight cap must be positive";
    private static final String POSITIVE_TIMEOUT_MESSAGE = "Admission timeout must be positive";
    private static final String INTERRUPTED_ADMISSION_MESSAGE =
            "Interrupted while waiting for embedding admission";
    private static final String TIMEOUT_ADMISSION_MESSAGE =
            "Timed out waiting for embedding admission";
    private static final String TIMEOUT_CAUSE_MESSAGE = "Embedding admission timeout";

    private final ReentrantLock lock = new ReentrantLock(true);
    private final Condition available = lock.newCondition();
    private final long admissionNanos;
    private int participants;
    private int inFlight;
    private int limit;

    /**
     * One admitted in-flight slot. Closing the permit releases the slot; closing it more
     * than once is a no-op.
     */
    public final class Permit implements AutoCloseable {
        private final AtomicBoolean released = new AtomicBoolean();

        private Permit() {
        }

        @Override
        public void close() {
            if (released.compareAndSet(false, true)) {
                releaseSlot();
            }
        }
    }

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

    /**
     * Executes one RPC with this limit's admission timeout, releasing admission on every exit.
     *
     * @param <T> result type of {@code operation}
     * @param requestedLimit positive process-wide cap that all active and waiting callers share
     * @param operation admitted operation to execute while holding the slot
     * @return the value returned by {@code operation}
     */
    public <T> T execute(int requestedLimit, Supplier<T> operation) {
        return execute(requestedLimit, admissionNanos, operation);
    }

    /**
     * Executes one RPC, waiting at most {@code admissionTimeout} for admission. The timeout
     * belongs to this call only; unlike the cap, overlapping callers need not agree on it.
     *
     * @param <T> result type of {@code operation}
     * @param requestedLimit positive process-wide cap that all active and waiting callers share
     * @param admissionTimeout maximum time to wait for a slot on this call
     * @param operation admitted operation to execute while holding the slot
     * @return the value returned by {@code operation}
     */
    public <T> T execute(int requestedLimit, Duration admissionTimeout, Supplier<T> operation) {
        return execute(requestedLimit, requirePositive(admissionTimeout), operation);
    }

    /**
     * Acquires one in-flight slot, waiting at most {@code admissionTimeout} on the calling
     * thread. The returned {@link Permit} must be closed when the admitted operation finishes.
     *
     * @param requestedLimit positive process-wide cap that all active and waiting callers share
     * @param admissionTimeout maximum time to wait for a slot on this call
     * @return the admitted slot permit
     * @throws IllegalArgumentException when {@code requestedLimit} or {@code admissionTimeout}
     *     is not positive, or {@code admissionTimeout} overflows nanoseconds
     * @throws IllegalStateException on conflicting cap configuration, admission timeout, or
     *     calling-thread interruption
     */
    public Permit acquire(int requestedLimit, Duration admissionTimeout) {
        return acquireNanos(requestedLimit, requirePositive(admissionTimeout), () -> false)
                .orElseThrow();
    }

    /**
     * Acquires one in-flight slot, waiting at most {@code admissionTimeout} unless
     * {@code cancelled} reports {@code true} before a slot is claimed. When {@code cancelled}
     * becomes {@code true}, returns {@link Optional#empty()} without claiming a slot or
     * disturbing the calling thread's interrupt status.
     *
     * @param requestedLimit positive process-wide cap that all active and waiting callers share
     * @param admissionTimeout maximum time to wait for a slot on this call
     * @param cancelled non-blocking predicate checked before waiting and on every wakeup
     * @return the admitted slot permit, or empty when {@code cancelled} reported {@code true}
     * @throws IllegalArgumentException when {@code requestedLimit} or {@code admissionTimeout}
     *     is not positive, or {@code admissionTimeout} overflows nanoseconds
     * @throws IllegalStateException on conflicting cap configuration, admission timeout, or
     *     calling-thread interruption
     */
    public Optional<Permit> acquire(
            int requestedLimit,
            Duration admissionTimeout,
            BooleanSupplier cancelled) {
        Objects.requireNonNull(cancelled, "cancelled");
        return acquireNanos(requestedLimit, requirePositive(admissionTimeout), cancelled);
    }

    /**
     * Returns the number of admitted operations currently holding a slot.
     *
     * @return active in-flight count
     */
    public int inFlight() {
        lock.lock();
        try {
            return inFlight;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Returns the number of callers currently waiting for an in-flight slot.
     *
     * @return waiting caller count
     */
    public int waiting() {
        lock.lock();
        try {
            return participants - inFlight;
        } finally {
            lock.unlock();
        }
    }

    private <T> T execute(int requestedLimit, long waitNanos, Supplier<T> operation) {
        Objects.requireNonNull(operation, "operation");
        try (Permit _ = acquireNanos(requestedLimit, waitNanos, () -> false).orElseThrow()) {
            return operation.get();
        }
    }

    private Optional<Permit> acquireNanos(
            int requestedLimit,
            long waitNanos,
            BooleanSupplier cancelled) {
        if (requestedLimit < 1) {
            throw new IllegalArgumentException(POSITIVE_CAP_MESSAGE);
        }
        boolean admitted;
        try {
            admitted = enter(requestedLimit, waitNanos, cancelled);
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(INTERRUPTED_ADMISSION_MESSAGE, failure);
        }
        return admitted ? Optional.of(new Permit()) : Optional.empty();
    }

    private void releaseSlot() {
        lock.lock();
        try {
            inFlight--;
            participants--;
            available.signalAll();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Parses a configured admission timeout in whole seconds. {@code null} means
     * {@link #DEFAULT_ADMISSION_TIMEOUT}; anything else must be a positive integer.
     *
     * @param configured the configured value, or {@code null} when unset
     * @param source the property and environment names, for the error message
     * @return the resolved positive admission timeout
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
        Objects.requireNonNull(admissionTimeout, "admissionTimeout");
        long nanos;
        try {
            nanos = admissionTimeout.toNanos();
        } catch (ArithmeticException overflow) {
            throw new IllegalArgumentException("Admission timeout is too long to represent in nanoseconds: "
                    + admissionTimeout, overflow);
        }
        if (nanos <= 0) {
            throw new IllegalArgumentException(POSITIVE_TIMEOUT_MESSAGE);
        }
        return nanos;
    }

    private boolean enter(int requestedLimit, long waitNanos, BooleanSupplier cancelled)
            throws InterruptedException {
        lock.lockInterruptibly();
        try {
            if (cancelled.getAsBoolean()) {
                return false;
            }
            if (participants != 0 && limit != requestedLimit) {
                throw new IllegalStateException("Conflicting process-wide embedding in-flight caps: active "
                        + limit + ", requested " + requestedLimit);
            }
            limit = requestedLimit;
            participants++;
            try {
                long remaining = waitNanos;
                while (inFlight >= limit) {
                    if (cancelled.getAsBoolean()) {
                        participants--;
                        available.signalAll();
                        return false;
                    }
                    if (remaining <= 0) {
                        throw new IllegalStateException(TIMEOUT_ADMISSION_MESSAGE,
                                new TimeoutException(TIMEOUT_CAUSE_MESSAGE));
                    }
                    remaining = available.awaitNanos(remaining);
                }
                if (cancelled.getAsBoolean()) {
                    participants--;
                    available.signalAll();
                    return false;
                }
                inFlight++;
                return true;
            } catch (InterruptedException | RuntimeException | Error failure) {
                participants--;
                available.signalAll();
                throw failure;
            }
        } finally {
            lock.unlock();
        }
    }
}
