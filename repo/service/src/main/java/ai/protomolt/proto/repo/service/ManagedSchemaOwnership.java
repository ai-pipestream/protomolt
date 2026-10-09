package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.repo.container.ledger.DocumentPublicationRuntime;
import java.time.Duration;
import java.util.Objects;

/** Transfers schema workers only when the complete host composition can be exposed. */
final class ManagedSchemaOwnership implements DocumentPublicationRuntime.ExternalWorkers {
    private final ManagedSchemaAccess schemas;
    private boolean transferred;
    private boolean closed;
    private boolean schemaAdmissionClosed;

    ManagedSchemaOwnership(ManagedSchemaAccess schemas) { this.schemas = Objects.requireNonNull(schemas); }

    synchronized void transfer() {
        if (closed || transferred) throw new IllegalStateException("Schema lifecycle ownership already settled");
        transferred = true;
    }

    @Override public synchronized void closeAdmission() {
        closed = true;
        if (transferred && !schemaAdmissionClosed) {
            // ManagedSchemaAccess.close is nonblocking. Serialize this transition,
            // but record success only after it returns so failures remain retryable.
            schemas.close();
            schemaAdmissionClosed = true;
        }
    }

    @Override public boolean awaitIdle(Duration timeout) throws InterruptedException {
        synchronized (this) {
            // Before transfer the host has exposed no calls and owns no schema workers.
            // Any work already using this access still belongs to the constructing caller.
            if (!transferred) return true;
        }
        return schemas.awaitIdle(timeout);
    }
}
