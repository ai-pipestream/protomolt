package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.RepositoryException;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/** Closeable admission barrier for owned resolver or registration scopes. */
final class DocumentPublicationScopeCalls {
    private int active;
    private boolean closed;

    synchronized Call enter() {
        if (closed) throw new RepositoryException(RepositoryException.Code.UNAVAILABLE, "Publication runtime is closed");
        active++;
        return new Call();
    }

    synchronized void close() { closed = true; }
    synchronized boolean isIdle() { return active == 0; }

    synchronized boolean awaitIdle(Duration timeout) throws InterruptedException {
        Objects.requireNonNull(timeout);
        if (timeout.isNegative()) throw new IllegalArgumentException("Negative shutdown wait");
        long budget = timeout.toNanos(), start = System.nanoTime();
        while (active != 0) {
            long remaining = budget - (System.nanoTime() - start);
            if (remaining <= 0) return false;
            TimeUnit.NANOSECONDS.timedWait(this, remaining);
        }
        return true;
    }

    final class Call implements AutoCloseable {
        private boolean ended;
        @Override public void close() {
            synchronized (DocumentPublicationScopeCalls.this) {
                if (ended) return;
                ended = true; active--;
                DocumentPublicationScopeCalls.this.notifyAll();
            }
        }
    }
}
