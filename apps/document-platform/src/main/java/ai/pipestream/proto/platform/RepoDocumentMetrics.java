package ai.pipestream.proto.platform;

import ai.pipestream.proto.metric.Aggregate;
import ai.pipestream.proto.metric.FieldMetric;
import ai.pipestream.proto.metric.MemberRole;
import ai.pipestream.proto.metric.spi.CatalogMetricHintSource;
import ai.pipestream.proto.metric.spi.MetricMapping;
import ai.pipestream.proto.metric.spi.MetricMappings;
import ai.pipestream.proto.repo.v1.Document;
import ai.pipestream.proto.search.door.RepoDocumentMapping;

/**
 * The out-of-the-box metric mapping over the repo-document search subject:
 * a {@code documents} COUNT measure over the identity field. The metric
 * declarations reach only the top-level {@link Document} fields today, and
 * the metric-worthy fields (document type, language, category, processed
 * date) live nested under {@code search_metadata} — dimensions over them
 * arrive with nested member support in the metric SPI, not here.
 */
final class RepoDocumentMetrics {

    private RepoDocumentMetrics() {
    }

    /** The repo-document metric mapping, keyed to the search subject. */
    static MetricMapping mapping() {
        CatalogMetricHintSource catalog = new CatalogMetricHintSource()
                .put(Document.getDescriptor().getFullName(), "doc_id",
                        FieldMetric.newBuilder()
                                .setRole(MemberRole.MEMBER_ROLE_MEASURE)
                                .setAggregate(Aggregate.AGGREGATE_COUNT)
                                .setName("documents")
                                .build());
        return MetricMappings.build(
                RepoDocumentMapping.SUBJECT, Document.getDescriptor(), catalog);
    }
}
