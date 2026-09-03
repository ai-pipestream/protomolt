package ai.protomolt.proto.search.service;

import ai.protomolt.proto.search.embedding.VectorizationPolicy;
import ai.protomolt.proto.search.index.spi.CatalogIndexingHintSource;
import ai.protomolt.proto.search.index.spi.ChunkingPolicy;
import ai.protomolt.proto.search.index.spi.IndexFieldKind;
import ai.protomolt.proto.search.index.spi.IndexMapping;
import ai.protomolt.proto.search.index.spi.IndexMappingFactory;
import ai.protomolt.proto.search.index.spi.ResolvedFieldHint;
import ai.protomolt.proto.repo.v1.Document;
import java.util.Set;

/**
 * The out-of-the-box mapping subject for repository documents: identity plus
 * the folded search metadata, with the storage and provenance planes kept
 * out of the index. This is the subject the document platform serves by
 * default; custom subjects are additional {@link ServedMapping}s on the
 * service's configuration.
 */
public final class RepoDocumentMapping {

    /** The subject name the service serves this mapping under: {@value}. */
    public static final String SUBJECT = "repo-document";

    /** The index field name of the folded body text: {@value}. */
    public static final String BODY_FIELD = "search_metadata_body";

    /**
     * The sensitivity class the repo document's body declares: {@value}. It names the
     * screening policy that applies to that text rather than a restriction on it, but a
     * chunk lane over the body still sends it to an embedding provider, so a deployment
     * serving this mapping with a lane must permit the class — see
     * {@link VectorizationPolicy}.
     */
    public static final String BODY_SENSITIVITY = "screened";

    /**
     * The narrowest vectorization policy a chunk lane over this mapping needs: unclassified
     * content plus {@link #BODY_SENSITIVITY}. A deployment still states the decision by
     * passing this, which is the point; the constant only spares it from spelling the class
     * name and getting it wrong.
     *
     * @return the policy permitting this mapping's body class
     */
    public static VectorizationPolicy laneVectorization() {
        return VectorizationPolicy.permitting(Set.of(BODY_SENSITIVITY));
    }

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
        catalog.put("ai.protomolt.proto.repo.v1.SearchMetadata", "title",
                ResolvedFieldHint.builder(IndexFieldKind.TEXT).stored(true).build());
        catalog.put("ai.protomolt.proto.repo.v1.SearchMetadata", "body",
                ResolvedFieldHint.builder(IndexFieldKind.TEXT).stored(true).build());
        // Facetable doc values back the metric dimensions over this subject
        // (group-by and date grains read them; without them the metric
        // executor refuses loudly naming the hint).
        for (String facet : new String[] {"document_type", "language", "category"}) {
            catalog.put("ai.protomolt.proto.repo.v1.SearchMetadata", facet,
                    ResolvedFieldHint.builder(IndexFieldKind.KEYWORD).facetable(true).build());
        }
        catalog.put("ai.protomolt.proto.repo.v1.SearchMetadata", "processed_date",
                ResolvedFieldHint.builder(IndexFieldKind.DATE).facetable(true).build());
        // Storage and provenance planes stay out of the index.
        for (String skipped : new String[] {
                "parser_results", "blob_bag", "structured_data", "ownership",
                "doc_id_derivation"}) {
            catalog.put(Document.getDescriptor().getFullName(), skipped,
                    ResolvedFieldHint.skipped());
        }
        for (String skipped : new String[] {"semantic_results", "custom_fields", "metadata"}) {
            catalog.put("ai.protomolt.proto.repo.v1.SearchMetadata", skipped,
                    ResolvedFieldHint.skipped());
        }
        return IndexMappingFactory.defaults(catalog).create(Document.getDescriptor());
    }
}
