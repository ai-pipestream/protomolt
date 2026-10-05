package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.admission.DocumentSchemaAdmission;
import ai.protomolt.proto.repo.admission.DocumentSchemaMaterialization;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import ai.protomolt.proto.repo.v1.RepositoryResolvedSchema;
import com.google.protobuf.Any;
import com.google.protobuf.DynamicMessage;

/** Owns copied decoded content and one revision-pin use until close. No admission verdict. */
public final class DocumentHistoricalMaterialization implements AutoCloseable {
    /** Borrowed content: keep the owning result open until the last consumer finishes. */
    public record View(Any original, DynamicMessage value, RepositoryResolvedSchema schema,
            DocumentSchemaAdmission.Selection occurrence) {}
    private DocumentSchemaMaterialization.Result result;
    private final DocumentReadLedger.PinnedHistory history;
    private final DocumentReadLedger.PinnedRead<?>.Use pin;
    DocumentHistoricalMaterialization(DocumentReadLedger.PinnedHistory history, DocumentReadLedger.PinnedRead<?>.Use pin,
            DocumentSchemaMaterialization.Result result) {
        this.history = history; this.pin = pin; this.result = result;
    }
    /** Reauthorizes once per content view, before exposing any decoded fields or schema details. */
    public synchronized View view(RepositoryReadControl control) {
        if (result == null) throw new IllegalStateException("Historical materialization is closed");
        pin.plan();
        history.authorizeDelivery(control);
        pin.plan();
        return new View(result.original(), result.value(), result.schema(), result.occurrence());
    }
    @Override public synchronized void close() {
        if (result == null) return;
        try { result.close(); }
        finally { result = null; pin.close(); }
    }
}
