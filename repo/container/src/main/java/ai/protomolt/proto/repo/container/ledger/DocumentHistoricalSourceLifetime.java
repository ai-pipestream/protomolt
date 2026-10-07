package ai.protomolt.proto.repo.container.ledger;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/** Admission and actual-work barrier; cancellation of a future is not completion of its Work. */
final class DocumentHistoricalSourceLifetime implements AutoCloseable {
    private final DocumentPublicationScopeCalls calls = new DocumentPublicationScopeCalls();
    private final Runnable release;
    private boolean closing;
    private boolean releasing;
    private boolean drained;
    private Throwable failure;

    DocumentHistoricalSourceLifetime(Runnable release) { this.release = Objects.requireNonNull(release); }

    Work enter() { return new Work(calls.enter()); }

    final class Work implements AutoCloseable {
        private final DocumentPublicationScopeCalls.Call call;
        private boolean ended;
        private Work(DocumentPublicationScopeCalls.Call call) { this.call = call; }
        synchronized void requireActive() {
            if (ended) throw new IllegalStateException("Historical source work has ended");
        }
        @Override public void close() {
            synchronized (this) {
                if (ended) return;
                ended = true;
            }
            call.close();
            releaseIfIdle();
        }
    }

    @Override public void close() {
        calls.close();
        synchronized (this) { closing = true; }
        releaseIfIdle();
    }

    private void releaseIfIdle() {
        synchronized (this) {
            if (!closing || releasing || drained || !calls.isIdle()) return;
            releasing = true;
        }
        // Never hold the lifetime monitor while entering the reader ledger's monitor.
        Throwable failed = null;
        try { release.run(); }
        catch (RuntimeException | Error cause) { failed = cause; throw cause; }
        finally {
            synchronized (this) {
                releasing = false; failure = failed; drained = failed == null;
                notifyAll();
            }
        }
    }

    boolean awaitDrained(Duration timeout) throws InterruptedException {
        Objects.requireNonNull(timeout);
        if (timeout.isNegative()) throw new IllegalArgumentException("Negative source drain wait");
        long budget = timeout.toNanos(), start = System.nanoTime();
        synchronized (this) {
            if (!closing) throw new IllegalStateException("Close source admission before awaiting drain");
            while (!drained) {
                if (failure != null) throw new IllegalStateException("Historical source release failed", failure);
                long remaining = budget - (System.nanoTime() - start);
                if (remaining <= 0) return false;
                TimeUnit.NANOSECONDS.timedWait(this, remaining);
            }
            return true;
        }
    }
}
