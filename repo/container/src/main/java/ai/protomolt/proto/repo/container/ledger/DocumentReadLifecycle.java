package ai.protomolt.proto.repo.container.ledger;

import java.time.Duration;
import java.util.Objects;

/**
 * Host-driven cleanup and shutdown for one document reader incarnation. Owns no
 * thread, database pool or provider client. The host retains those resources until
 * shutdownStep returns true; false, interruption and exceptions leave shutdown retryable.
 */
public final class DocumentReadLifecycle {
    public interface Reader {
        /** Stop admission without asserting that actual provider workers stopped. */
        void close();
        /** After close, wait for operations and actual workers, not just their futures. */
        boolean awaitIdle(Duration timeout) throws InterruptedException;
    }

    private final DocumentReadLedger ledger;
    private final Reader reader;
    private final int batchSize;
    private boolean stopping;
    private boolean stopped;

    public DocumentReadLifecycle(DocumentReadLedger ledger, Reader reader, int batchSize) {
        this.ledger = Objects.requireNonNull(ledger); this.reader = Objects.requireNonNull(reader);
        if (batchSize < 1 || batchSize > DocumentReadRecovery.MAX_BATCH_SIZE)
            throw new IllegalArgumentException("Reader lifecycle batch is outside recovery bounds");
        this.batchSize = batchSize;
    }

    /** One bounded cleanup pass during service. Failures remain visible and retryable. */
    public synchronized int tick() {
        if (stopping) throw new IllegalStateException("Reader lifecycle is stopping");
        return ledger.releaseDrained(batchSize);
    }

    /**
     * Stops admission, drains actual work and caller batch uses, then performs at
     * most one combined pin/session recovery budget and one local reconciliation batch. Repeat
     * until true. A positive recovery count requires another pass to observe zero.
     * waitBudget bounds local waits together, not SQL statement/network duration;
     * configure the ledger's Tx with appropriate SQL timeouts independently.
     */
    public synchronized boolean shutdownStep(Duration waitBudget) throws InterruptedException {
        Objects.requireNonNull(waitBudget);
        if (waitBudget.isNegative()) throw new IllegalArgumentException("Shutdown wait must not be negative");
        long budget = waitBudget.toNanos(), start = System.nanoTime();
        if (stopped) return true;
        stopping = true;
        reader.close();
        ledger.closeForShutdown();
        if (!reader.awaitIdle(remaining(budget, start))) return false;
        if (!ledger.awaitLocalDrain(remaining(budget, start))) return false;
        ledger.attestLocalQuiescence();
        int observed = ledger.recoverQuiescedPins(batchSize);
        ledger.reconcileDrained(batchSize);
        stopped = observed == 0 && ledger.outstandingReads() == 0;
        return stopped;
    }

    private static Duration remaining(long budget, long start) {
        return Duration.ofNanos(Math.max(0, budget - (System.nanoTime() - start)));
    }
}
