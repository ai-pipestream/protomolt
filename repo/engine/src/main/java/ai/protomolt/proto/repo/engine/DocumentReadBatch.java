package ai.protomolt.proto.repo.engine;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.codec.PartObject;
import java.util.List;
import java.util.concurrent.CancellationException;

/**
 * Ordered verified fragments and their payload reservation. Keep open through
 * source reuse/staging, then close. Arrays or lists retained after close are outside
 * budget accounting. Closing never closes the borrowed provider.
 */
public final class DocumentReadBatch implements AutoCloseable {
    private final PayloadBudget.Lease reservation;
    private List<PartObject> parts;
    private int workers;
    private boolean closed;

    DocumentReadBatch(PayloadBudget.Lease reservation) { this.reservation = reservation; }

    synchronized void enterWorker() {
        if (closed) throw new CancellationException("Document read batch is closed");
        workers++;
    }

    synchronized void exitWorker() {
        if (workers <= 0) throw new IllegalStateException("Unbalanced document read worker");
        workers--;
        if (closed && workers == 0) reservation.close();
    }

    synchronized void complete(List<PartObject> parts) {
        if (closed) throw new IllegalStateException("Document read batch is closed");
        if (this.parts != null) throw new IllegalStateException("Document read batch already completed");
        this.parts = List.copyOf(parts);
    }

    public synchronized List<PartObject> parts() {
        if (closed) throw new IllegalStateException("Document read batch is closed");
        if (parts == null) throw new IllegalStateException("Document read batch is incomplete");
        return parts;
    }

    @Override public synchronized void close() {
        if (closed) return;
        closed = true;
        parts = null;
        if (workers == 0) reservation.close();
    }
}
