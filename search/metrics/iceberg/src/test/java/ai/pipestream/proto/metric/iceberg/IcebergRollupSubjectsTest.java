package ai.pipestream.proto.metric.iceberg;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.pipestream.proto.lake.iceberg.LocalFileIO;
import ai.pipestream.proto.metric.Aggregate;
import ai.pipestream.proto.metric.MemberRef;
import ai.pipestream.proto.metric.MemberRole;
import ai.pipestream.proto.metric.MetricBackend;
import ai.pipestream.proto.metric.MetricRow;
import ai.pipestream.proto.metric.QueryMetricsRequest;
import ai.pipestream.proto.metric.QueryMetricsResponse;
import ai.pipestream.proto.metric.spi.MetricQueries;
import ai.pipestream.proto.metric.spi.MetricRefusal;
import ai.pipestream.proto.metric.spi.MetricSubjectResolver;
import ai.pipestream.proto.metric.spi.RollupSink;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Rollup tables as metric subjects: the sink stamps the declaration onto
 * the table, the resolver reads it back and serves {@code rollup:<table>}
 * with honest re-aggregation — COUNT and SUM columns sum, an AVG column
 * is not a member at all, and querying the rollup at a coarser grain
 * answers exactly the numbers the source would.
 */
class IcebergRollupSubjectsTest {

    @TempDir
    static Path warehouse;

    static JdbcCatalog catalog;
    static IcebergRollupSubjects resolver;

    @BeforeAll
    static void boot() {
        catalog = new JdbcCatalog();
        catalog.initialize("rollups", Map.of(
                CatalogProperties.URI, "jdbc:sqlite:" + warehouse.resolve("catalog.db"),
                CatalogProperties.WAREHOUSE_LOCATION, warehouse.toString(),
                CatalogProperties.FILE_IO_IMPL, LocalFileIO.class.getName()));
        catalog.createNamespace(Namespace.of("protomolt"));
        resolver = new IcebergRollupSubjects(catalog, "protomolt");

        // The rollup: documents and avg_ratio by (segment, language).
        IcebergRollupSink sink = new IcebergRollupSink(catalog, "protomolt");
        sink.replace("orders", "docs_by_segment_language",
                List.of("segment", "language"),
                List.of(new RollupSink.MeasureColumn("documents", Aggregate.AGGREGATE_COUNT),
                        new RollupSink.MeasureColumn("avg_ratio", Aggregate.AGGREGATE_AVG)),
                List.of(
                        rollupRow("smb", "en", 3.0, 0.5),
                        rollupRow("smb", "de", 2.0, 0.25),
                        rollupRow("mid", "en", 5.0, 0.75)));
    }

    @AfterAll
    static void shutdown() throws Exception {
        catalog.close();
    }

    static MetricRow rollupRow(String segment, String language,
            double documents, double avgRatio) {
        return MetricRow.newBuilder()
                .putDimensions("segment", segment)
                .putDimensions("language", language)
                .putMeasures("documents", documents)
                .putMeasures("avg_ratio", avgRatio)
                .build();
    }

    @Test
    void aRollupResolvesWithHonestReaggregationOnly() {
        MetricSubjectResolver.Resolved resolved =
                resolver.resolve("rollup:docs_by_segment_language");
        assertThat(resolved).isNotNull();
        assertThat(resolved.mapping().subject())
                .isEqualTo("rollup:docs_by_segment_language");
        assertThat(resolved.mapping().members().get("segment").role())
                .isEqualTo(MemberRole.MEMBER_ROLE_DIMENSION);
        assertThat(resolved.mapping().members().get("language").role())
                .isEqualTo(MemberRole.MEMBER_ROLE_DIMENSION);
        // COUNT re-serves as SUM: summing counts is counting.
        assertThat(resolved.mapping().members().get("documents").aggregate())
                .isEqualTo(Aggregate.AGGREGATE_SUM);
        // An average of averages is a wrong answer: not a member at all.
        assertThat(resolved.mapping().members()).doesNotContainKey("avg_ratio");
        assertThat(resolved.executor().backend())
                .isEqualTo(MetricBackend.METRIC_BACKEND_ICEBERG);
    }

    @Test
    void queryingTheRollupAtACoarserGrainSumsTheCounts() {
        MetricSubjectResolver.Resolved resolved =
                resolver.resolve("rollup:docs_by_segment_language");
        QueryMetricsResponse bySegment = MetricQueries.query(
                resolved.mapping(),
                Map.of(resolved.executor().backend(), resolved.executor()),
                QueryMetricsRequest.newBuilder()
                        .setMappingSubject("rollup:docs_by_segment_language")
                        .addMeasures("documents")
                        .addDimensions(MemberRef.newBuilder().setName("segment"))
                        .setLimit(10)
                        .build());
        // Hand-checked: smb = 3 + 2, mid = 5.
        assertThat(bySegment.getRowsList()).hasSize(2);
        assertThat(bySegment.getRows(0).getDimensionsMap())
                .containsEntry("segment", "mid");
        assertThat(bySegment.getRows(0).getMeasuresMap())
                .containsEntry("documents", 5.0);
        assertThat(bySegment.getRows(1).getDimensionsMap())
                .containsEntry("segment", "smb");
        assertThat(bySegment.getRows(1).getMeasuresMap())
                .containsEntry("documents", 5.0);

        QueryMetricsResponse total = MetricQueries.query(
                resolved.mapping(),
                Map.of(resolved.executor().backend(), resolved.executor()),
                QueryMetricsRequest.newBuilder()
                        .setMappingSubject("rollup:docs_by_segment_language")
                        .addMeasures("documents")
                        .setLimit(10)
                        .build());
        assertThat(total.getRows(0).getMeasuresMap()).containsEntry("documents", 10.0);
    }

    @Test
    void namesOutsideThePrefixAndMissingTablesAnswerHonestly() {
        assertThat(resolver.resolve("repo-document")).isNull();
        assertThatThrownBy(() -> resolver.resolve("rollup:nope"))
                .isInstanceOfSatisfying(MetricRefusal.class, refusal -> {
                    assertThat(refusal.code()).isEqualTo(MetricRefusal.MISSING_TABLE);
                    assertThat(refusal.getMessage()).contains("rollup:nope"
                            .substring("rollup:".length()));
                });
    }

    @Test
    void aTableWithoutADeclarationRefusesInsteadOfGuessing() {
        // A lake table the sink did not write: real, but not a rollup.
        ai.pipestream.proto.lake.iceberg.IcebergSink.ensureTable(catalog,
                org.apache.iceberg.catalog.TableIdentifier.of("protomolt", "foreign_table"),
                MetricRow.getDescriptor());
        assertThatThrownBy(() -> resolver.resolve("rollup:foreign_table"))
                .isInstanceOfSatisfying(MetricRefusal.class, refusal -> {
                    assertThat(refusal.code()).isEqualTo(MetricRefusal.UNKNOWN_SUBJECT);
                    assertThat(refusal.getMessage()).contains("no rollup declaration");
                });
    }
}
