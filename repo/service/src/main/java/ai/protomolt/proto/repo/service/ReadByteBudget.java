package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.repo.admission.DocumentAdmissionReservations;
import ai.protomolt.proto.repo.spi.RepositoryException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The host's fail-fast byte budget for read-side buffering
 * ({@code DOCUMENT_PLATFORM_READ_BUDGET_BYTES}). A reservation that does not fit the
 * remaining budget is refused at once with {@code RESOURCE_EXHAUSTED}, never queued
 * and never allocated, so concurrent verified reads stay within a fixed byte ceiling
 * instead of buffering whatever the catalog records. Closing a lease returns its
 * bytes; close is idempotent and nonthrowing per the reservation contract.
 */
final class ReadByteBudget implements DocumentAdmissionReservations {
    private final long budgetBytes;
    private final AtomicLong remaining;

    ReadByteBudget(long budgetBytes) {
        if (budgetBytes < 1) throw new IllegalArgumentException("budgetBytes < 1: " + budgetBytes);
        this.budgetBytes = budgetBytes;
        this.remaining = new AtomicLong(budgetBytes);
    }

    @Override
    public Lease reserve(long bytes) {
        if (bytes < 0) throw new IllegalArgumentException("bytes < 0: " + bytes);
        long available;
        do {
            available = remaining.get();
            if (bytes > available) {
                throw new RepositoryException(RepositoryException.Code.RESOURCE_EXHAUSTED,
                        "Read buffer budget exhausted: " + bytes + " bytes requested, "
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
