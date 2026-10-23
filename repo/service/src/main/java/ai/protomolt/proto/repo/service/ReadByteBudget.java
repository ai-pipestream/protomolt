package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.repo.admission.DocumentAdmissionReservations;
import ai.protomolt.proto.repo.spi.RepositoryException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The host's fail-fast byte budget for buffered document bytes
 * ({@code DOCUMENT_PLATFORM_READ_BUDGET_BYTES}). A reservation that does not fit the
 * remaining budget is refused at once with {@code RESOURCE_EXHAUSTED}, never queued
 * and never allocated, so concurrent operations stay within a fixed byte ceiling.
 * Closing a lease returns its bytes; close is idempotent and nonthrowing per the
 * reservation contract.
 *
 * <p>The host wires one instance for verified reads ({@code "Read buffer budget
 * exhausted"}) and a separate instance of the same configured size via
 * {@link #forSaves(long)} for save-side admission buffering ({@code "Save buffer
 * budget exhausted"}), so concurrent reads and saves can hold up to twice
 * {@code DOCUMENT_PLATFORM_READ_BUDGET_BYTES} in combined reservations.
 */
final class ReadByteBudget implements DocumentAdmissionReservations {
    static final String READ_BUFFER_LABEL = "Read buffer";
    static final String SAVE_BUFFER_LABEL = "Save buffer";

    private final long budgetBytes;
    private final String bufferLabel;
    private final AtomicLong remaining;

    ReadByteBudget(long budgetBytes) {
        this(budgetBytes, READ_BUFFER_LABEL);
    }

    ReadByteBudget(long budgetBytes, String bufferLabel) {
        if (budgetBytes < 1) throw new IllegalArgumentException("budgetBytes < 1: " + budgetBytes);
        if (bufferLabel == null || bufferLabel.isBlank()) {
            throw new IllegalArgumentException("bufferLabel must not be blank");
        }
        this.budgetBytes = budgetBytes;
        this.bufferLabel = bufferLabel.trim();
        this.remaining = new AtomicLong(budgetBytes);
    }

    /** Creates a save-side byte budget reporting {@code "Save buffer budget exhausted"} on refusal. */
    static ReadByteBudget forSaves(long budgetBytes) {
        return new ReadByteBudget(budgetBytes, SAVE_BUFFER_LABEL);
    }

    @Override
    public Lease reserve(long bytes) {
        if (bytes < 0) throw new IllegalArgumentException("bytes < 0: " + bytes);
        long available;
        do {
            available = remaining.get();
            if (bytes > available) {
                throw new RepositoryException(RepositoryException.Code.RESOURCE_EXHAUSTED,
                        bufferLabel + " budget exhausted: " + bytes + " bytes requested, "
                                + available + " of " + budgetBytes + " available");
            }
        } while (!remaining.compareAndSet(available, available - bytes));
        var released = new AtomicBoolean();
        return () -> {
            if (released.compareAndSet(false, true)) remaining.addAndGet(bytes);
        };
    }

    long remainingBytes() {
        return remaining.get();
    }
}
