package ai.pipestream.proto.platform;

import static org.assertj.core.api.Assertions.assertThat;

import ai.pipestream.proto.metric.Aggregate;
import ai.pipestream.proto.metric.MemberRole;
import ai.pipestream.proto.metric.spi.MetricMapping;
import ai.pipestream.proto.search.door.RepoDocumentMapping;
import org.junit.jupiter.api.Test;

/**
 * The platform's out-of-the-box metric mapping: keyed to the search
 * subject it aggregates over, with the documents count as its measure.
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
}
