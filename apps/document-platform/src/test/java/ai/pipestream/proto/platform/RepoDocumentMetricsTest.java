package ai.pipestream.proto.platform;

import static org.assertj.core.api.Assertions.assertThat;

import ai.pipestream.proto.index.spi.IndexMapping;
import ai.pipestream.proto.index.spi.ResolvedFieldHint;
import ai.pipestream.proto.metric.Aggregate;
import ai.pipestream.proto.metric.MemberRole;
import ai.pipestream.proto.metric.TimeGrain;
import ai.pipestream.proto.metric.spi.MetricMapping;
import ai.pipestream.proto.metric.spi.MetricMapping.FieldKind;
import ai.pipestream.proto.search.service.RepoDocumentMapping;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * The platform's out-of-the-box metric mapping: keyed to the search
 * subject it aggregates over, the documents count as its measure,
 * dimensions on the flattened search-metadata fields, and every dimension
 * backed by the doc values the search mapping's hints declare.
 */
class RepoDocumentMetricsTest {

    @Test
    void theMappingCountsDocumentsUnderTheSearchSubject() {
        MetricMapping mapping = RepoDocumentMetrics.mapping();
        assertThat(mapping.subject()).isEqualTo(RepoDocumentMapping.SUBJECT);
        assertThat(mapping.member("documents")).hasValueSatisfying(member -> {
            assertThat(member.role()).isEqualTo(MemberRole.MEMBER_ROLE_MEASURE);
            assertThat(member.aggregate()).isEqualTo(Aggregate.AGGREGATE_COUNT);
            assertThat(member.fieldName()).isEqualTo("doc_id");
        });
    }

    @Test
    void theDimensionsLandOnTheFlattenedSearchMetadataFields() {
        MetricMapping mapping = RepoDocumentMetrics.mapping();
        for (String name : new String[] {"document_type", "language", "category"}) {
            assertThat(mapping.member(name)).as(name).hasValueSatisfying(member -> {
                assertThat(member.role()).isEqualTo(MemberRole.MEMBER_ROLE_DIMENSION);
                assertThat(member.kind()).isEqualTo(FieldKind.KEYWORD);
                assertThat(member.fieldName()).isEqualTo("search_metadata_" + name);
            });
        }
        assertThat(mapping.member("processed_date")).hasValueSatisfying(member -> {
            assertThat(member.kind()).isEqualTo(FieldKind.DATE);
            assertThat(member.fieldName()).isEqualTo("search_metadata_processed_date");
            assertThat(member.defaultGrain()).isEqualTo(TimeGrain.TIME_GRAIN_DAY);
        });
    }

    @Test
    void everyDimensionIsBackedByDocValuesInTheSearchMapping() {
        // The executor reads facetable or sortable doc values; a dimension
        // whose field lacks them would refuse every query at runtime, so
        // the two declarations are pinned together here.
        Map<String, ResolvedFieldHint> hints = new HashMap<>();
        for (IndexMapping.IndexedField field : RepoDocumentMapping.mapping().indexable()) {
            hints.put(field.fieldName(), field.hint());
        }
        RepoDocumentMetrics.mapping().members().values().stream()
                .filter(member -> member.role() == MemberRole.MEMBER_ROLE_DIMENSION)
                .forEach(member -> {
                    ResolvedFieldHint hint = hints.get(member.fieldName());
                    assertThat(hint).as(member.fieldName()).isNotNull();
                    assertThat(hint.facetable() || hint.sortable())
                            .as(member.fieldName() + " needs facetable or sortable")
                            .isTrue();
                });
    }
}
