package ai.pipestream.proto.search.door;

import ai.pipestream.proto.index.spi.CatalogIndexingHintSource;
import ai.pipestream.proto.index.spi.ChunkingPolicy;
import ai.pipestream.proto.index.spi.IndexFieldKind;
import ai.pipestream.proto.index.spi.IndexMapping;
import ai.pipestream.proto.index.spi.IndexMappingFactory;
import ai.pipestream.proto.index.spi.ResolvedFieldHint;
import ai.pipestream.proto.repo.v1.Document;

/**
 * The out-of-the-box mapping subject for repository documents: identity plus
 * the folded search metadata, with the storage and provenance planes kept
 * out of the index. This is the subject the document platform serves by
 * default; custom subjects are additional {@link ServedMapping}s on the
 * door's configuration.
 */
public final class RepoDocumentMapping {

    /** The subject name the door serves this mapping under: {@value}. */
    public static final String SUBJECT = "repo-document";

    /** The index field name of the folded body text: {@value}. */
    public static final String BODY_FIELD = "search_metadata_body";

    private RepoDocumentMapping() {
    }

    /**
     * The repo-document subject without a chunk lane: lexical search over
     * identity, title, and body.
     */
    public static ServedMapping served() {
        return served(null);
    }

    /**
     * The repo-document subject with a chunk lane over the folded body.
     *
     * @param policy the chunking policy, or {@code null} for no vector lane
     */
    public static ServedMapping served(ChunkingPolicy policy) {
        return new ServedMapping(
                mapping(),
                "doc_id",
                message -> ((Document) message).getDocId(),
                policy == null ? null : new ServedMapping.ChunkLane(
                        policy,
                        BODY_FIELD,
                        message -> ((Document) message).getSearchMetadata().getBody()));
    }

    /**
     * The repo-document index mapping: {@code doc_id} as identity,
     * {@code search_metadata} title and body as stored searchable text,
     * storage and provenance planes skipped.
     */
    public static IndexMapping mapping() {
        CatalogIndexingHintSource catalog = new CatalogIndexingHintSource();
        catalog.put(Document.getDescriptor().getFullName(), "doc_id",
                ResolvedFieldHint.of(IndexFieldKind.KEYWORD));
        catalog.put(Document.getDescriptor().getFullName(), "search_metadata",
                ResolvedFieldHint.of(IndexFieldKind.TEXT));
        catalog.put("ai.pipestream.proto.repo.v1.SearchMetadata", "title",
                ResolvedFieldHint.builder(IndexFieldKind.TEXT).stored(true).build());
        catalog.put("ai.pipestream.proto.repo.v1.SearchMetadata", "body",
                ResolvedFieldHint.builder(IndexFieldKind.TEXT).stored(true).build());
        // Facetable doc values back the metric dimensions over this subject
        // (group-by and date grains read them; without them the metric
        // executor refuses loudly naming the hint).
        for (String facet : new String[] {"document_type", "language", "category"}) {
            catalog.put("ai.pipestream.proto.repo.v1.SearchMetadata", facet,
                    ResolvedFieldHint.builder(IndexFieldKind.KEYWORD).facetable(true).build());
        }
        catalog.put("ai.pipestream.proto.repo.v1.SearchMetadata", "processed_date",
                ResolvedFieldHint.builder(IndexFieldKind.DATE).facetable(true).build());
        // Storage and provenance planes stay out of the index.
        for (String skipped : new String[] {
                "parser_results", "blob_bag", "structured_data", "ownership",
                "doc_id_derivation"}) {
            catalog.put(Document.getDescriptor().getFullName(), skipped,
                    ResolvedFieldHint.skipped());
        }
        for (String skipped : new String[] {"semantic_results", "custom_fields", "metadata"}) {
            catalog.put("ai.pipestream.proto.repo.v1.SearchMetadata", skipped,
                    ResolvedFieldHint.skipped());
        }
        return IndexMappingFactory.defaults(catalog).create(Document.getDescriptor());
    }
}
