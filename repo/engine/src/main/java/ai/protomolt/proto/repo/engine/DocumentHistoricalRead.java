package ai.protomolt.proto.repo.engine;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.container.ledger.DocumentHistoricalValidation;
import ai.protomolt.proto.repo.v1.Document;
import com.google.protobuf.ByteString;

/** Validated historical document with owned payload reservations and physical read protection. */
public final class DocumentHistoricalRead implements ai.protomolt.proto.repo.spi.HistoricalDocumentRepository.ValidatedRead {
    private DocumentReadBatch bytes;
    private final DocumentHistoricalValidation validation;
    private final PayloadBudget.Lease copies;
    private final java.util.UUID revision;
    private final ai.protomolt.proto.repo.v1.NodeAddress address;
    private final ai.protomolt.proto.repo.v1.DocumentManifest manifest;
    private final ai.protomolt.proto.repo.v1.HistoricalDocumentMetadata metadata;
    private final long publicationRevision;
    private final ai.protomolt.proto.repo.container.ledger.DocumentReadLedger.PinnedHistory history;
    DocumentHistoricalRead(DocumentReadBatch bytes, DocumentHistoricalValidation validation, PayloadBudget.Lease copies,
            java.util.UUID revision, ai.protomolt.proto.repo.v1.NodeAddress address,
            ai.protomolt.proto.repo.v1.DocumentManifest manifest, long publicationRevision,
            ai.protomolt.proto.repo.v1.HistoricalDocumentMetadata metadata,
            ai.protomolt.proto.repo.container.ledger.DocumentReadLedger.PinnedHistory history) {
        this.bytes = bytes; this.validation = validation; this.copies = copies;
        this.revision = revision; this.address = address;
        this.metadata = java.util.Objects.requireNonNull(metadata);
        this.manifest = manifest; this.publicationRevision = publicationRevision;
        this.history = java.util.Objects.requireNonNull(history);
    }
    private void requireOpen() {
        if (bytes == null) throw new IllegalStateException("Historical read is closed");
    }
    /** Borrowed immutable document; retaining it after close is outside payload accounting. */
    public synchronized Document document() { requireOpen(); return validation.document(); }
    public synchronized java.util.UUID revision() { requireOpen(); return revision; }
    public synchronized ai.protomolt.proto.repo.v1.NodeAddress address() { requireOpen(); return address; }
    public synchronized ai.protomolt.proto.repo.v1.DocumentManifest manifest() { requireOpen(); return manifest; }
    public synchronized ai.protomolt.proto.repo.v1.HistoricalDocumentMetadata metadata() { requireOpen(); return metadata; }
    public synchronized long publicationRevision() { requireOpen(); return publicationRevision; }
    public synchronized String validationProfile() { requireOpen(); return validation.validationProfile(); }
    public synchronized String policySha256() { requireOpen(); return validation.policySha256(); }
    public synchronized ByteString commandSha256() { requireOpen(); return validation.commandSha256(); }
    @Override public synchronized void authorizeDelivery(ai.protomolt.proto.repo.spi.RepositoryReadControl control) {
        requireOpen(); history.authorizeDelivery(control);
    }
    @Override public synchronized void close() {
        if (bytes == null) return;
        validation.close(); copies.close(); bytes.close(); bytes = null;
    }
}
