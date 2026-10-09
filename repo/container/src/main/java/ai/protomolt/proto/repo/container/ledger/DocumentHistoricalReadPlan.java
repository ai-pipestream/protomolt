package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.v1.DocumentManifest;
import ai.protomolt.proto.repo.v1.NodeAddress;
import java.util.List;
import java.util.UUID;

/** Immutable historical bindings, not a permission grant; issued only under current READ authorization. */
public final class DocumentHistoricalReadPlan {
    public record Entry(int revisionOrdinal, UUID objectId, DocumentPublicationLedger.BoundPart part) {}
    private final NodeAddress address;
    private final UUID revision;
    private final long publicationRevision;
    private final DocumentManifest manifest;
    private final ai.protomolt.proto.repo.v1.HistoricalDocumentMetadata metadata;
    private final List<Entry> entries;

    DocumentHistoricalReadPlan(NodeAddress address, UUID revision, long publicationRevision,
            DocumentManifest manifest, ai.protomolt.proto.repo.v1.HistoricalDocumentMetadata metadata, List<Entry> entries) {
        this.address = address; this.revision = revision; this.publicationRevision = publicationRevision;
        this.manifest = manifest; this.metadata = java.util.Objects.requireNonNull(metadata); this.entries = List.copyOf(entries);
    }
    public NodeAddress address() { return address; }
    public UUID revision() { return revision; }
    public long publicationRevision() { return publicationRevision; }
    public DocumentManifest manifest() { return manifest; }
    public ai.protomolt.proto.repo.v1.HistoricalDocumentMetadata metadata() { return metadata; }
    /** Full manifest ordinals, including gaps for empty or deleted slots. */
    public List<Entry> entries() { return entries; }
}
