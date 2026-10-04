package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.admission.DocumentSchemaAdmission;
import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.v1.Document;
import java.util.List;

/**
 * Current-runtime validation using retained schema assets. Close after the last
 * consumer releases its document; retained references after close leave budget
 * accounting. Reservations measure serialized payload copies, not JVM heap.
 */
public final class DocumentHistoricalValidation implements AutoCloseable {
    private DocumentSchemaAdmission.Proof proof;
    private final List<PayloadBudget.Lease> leases;
    DocumentHistoricalValidation(DocumentSchemaAdmission.Proof proof, List<PayloadBudget.Lease> leases) {
        this.proof = java.util.Objects.requireNonNull(proof); this.leases = List.copyOf(leases);
    }
    private DocumentSchemaAdmission.Proof requireOpen() {
        if (proof == null) throw new IllegalStateException("Historical validation is closed");
        return proof;
    }
    public synchronized Document document() { return requireOpen().document(); }
    public synchronized String validationProfile() { return requireOpen().validationProfile(); }
    public synchronized String policySha256() { return requireOpen().policySha256(); }
    public synchronized com.google.protobuf.ByteString commandSha256() { return requireOpen().commandSha256(); }
    @Override public synchronized void close() {
        if (proof == null) return;
        proof = null;
        leases.forEach(PayloadBudget.Lease::close);
    }
}
