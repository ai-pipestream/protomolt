package ai.pipestream.proto.platform;

import ai.pipestream.proto.metric.Aggregate;
import ai.pipestream.proto.metric.FieldMetric;
import ai.pipestream.proto.metric.MemberRole;
import ai.pipestream.proto.metric.TimeGrain;
import ai.pipestream.proto.metric.spi.CatalogMetricHintSource;
import ai.pipestream.proto.metric.spi.MetricMapping;
import ai.pipestream.proto.metric.spi.MetricMappings;
import ai.pipestream.proto.repo.v1.Document;
import ai.pipestream.proto.search.service.RepoDocumentMapping;

/**
 * The out-of-the-box metric mapping over the repo-document search subject:
 * a {@code documents} COUNT measure over the identity field, dimensions
 * over the folded search metadata's document type, language, and category,
 * and a processed-date time dimension defaulting to daily grain. Every
 * dimension field carries a facetable hint in
 * {@link RepoDocumentMapping#mapping()}; the two declarations move
 * together, because the executor reads the doc values that hint writes.
 */
final class RepoDocumentMetrics {

    private static final String SEARCH_METADATA_TYPE =
            "ai.pipestream.proto.repo.v1.SearchMetadata";

    private RepoDocumentMetrics() {
    }

    private static FieldMetric dimension() {
        return FieldMetric.newBuilder()
                .setRole(MemberRole.MEMBER_ROLE_DIMENSION)
                .build();
    }

    /** The repo-document metric mapping, keyed to the search subject. */
    static MetricMapping mapping() {
        CatalogMetricHintSource catalog = new CatalogMetricHintSource()
                .put(Document.getDescriptor().getFullName(), "doc_id",
                        FieldMetric.newBuilder()
                                .setRole(MemberRole.MEMBER_ROLE_MEASURE)
                                .setAggregate(Aggregate.AGGREGATE_COUNT)
                                .setName("documents")
                                .build())
                .put(SEARCH_METADATA_TYPE, "document_type", dimension())
                .put(SEARCH_METADATA_TYPE, "language", dimension())
                .put(SEARCH_METADATA_TYPE, "category", dimension())
                .put(SEARCH_METADATA_TYPE, "processed_date",
                        FieldMetric.newBuilder()
                                .setRole(MemberRole.MEMBER_ROLE_DIMENSION)
                                .setDefaultGrain(TimeGrain.TIME_GRAIN_DAY)
                                .build());
        return MetricMappings.build(
                RepoDocumentMapping.SUBJECT, Document.getDescriptor(), catalog);
    }
}
