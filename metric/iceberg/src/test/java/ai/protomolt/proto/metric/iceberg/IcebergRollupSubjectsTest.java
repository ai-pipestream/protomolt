package ai.protomolt.proto.metric.iceberg;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.protomolt.proto.iceberg.IcebergSink;
import ai.protomolt.proto.iceberg.LocalFileIO;
import ai.protomolt.proto.metric.Aggregate;
import ai.protomolt.proto.metric.MemberRef;
import ai.protomolt.proto.metric.MemberRole;
import ai.protomolt.proto.metric.MetricBackend;
import ai.protomolt.proto.metric.MetricRow;
import ai.protomolt.proto.metric.QueryMetricsRequest;
import ai.protomolt.proto.metric.QueryMetricsResponse;
import ai.protomolt.proto.metric.spi.MetricQueries;
import ai.protomolt.proto.metric.spi.MetricRefusal;
import ai.protomolt.proto.metric.spi.MetricSubjectResolver;
import ai.protomolt.proto.metric.spi.RollupSink;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

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

    /**
     * The declaration is read off the table, so it is only as well-formed as whatever last
     * wrote that table's properties: a hand-edited value, a half-finished migration, a
     * sink from another build. Each of these used to leave the parse by an index or enum
     * error, which tells an operator nothing about which table to go and look at.
     */
    @ParameterizedTest
    @CsvSource(delimiter = '|', value = {
        "no_colon          | hits",
        "empty_member      | :AGGREGATE_COUNT",
        "leading_comma     | ',hits:AGGREGATE_COUNT'",
        "unknown_aggregate | hits:AGGREGATE_MEDIAN",
        "lowercase         | hits:count",
        "empty_aggregate   | 'hits:'",
    })
    void aMalformedMeasureDeclarationRefusesNamingTheEntry(String table, String measures) {
        declare(table, "", measures);

        assertThatThrownBy(() -> resolver.resolve("rollup:" + table))
                .isInstanceOfSatisfying(MetricRefusal.class, refusal -> {
                    assertThat(refusal.code()).isEqualTo(MetricRefusal.UNKNOWN_SUBJECT);
                    assertThat(refusal.getMessage()).contains(table);
                });
    }

    /** Stamps a rollup declaration onto a table directly, well-formed or not. */
    private static void declare(String tableName, String dimensions, String measures) {
        TableIdentifier id = TableIdentifier.of("protomolt", tableName);
        IcebergSink.ensureTable(catalog, id, MetricRow.getDescriptor());
        catalog.loadTable(id).updateProperties()
                .set(IcebergRollupSink.PROPERTY_DIMENSIONS, dimensions)
                .set(IcebergRollupSink.PROPERTY_MEASURES, measures)
                .commit();
    }

    @Test
    void aTableWithoutADeclarationRefusesInsteadOfGuessing() {
        // A lake table the sink did not write: real, but not a rollup.
        IcebergSink.ensureTable(catalog,
                TableIdentifier.of("protomolt", "foreign_table"),
                MetricRow.getDescriptor());
        assertThatThrownBy(() -> resolver.resolve("rollup:foreign_table"))
                .isInstanceOfSatisfying(MetricRefusal.class, refusal -> {
                    assertThat(refusal.code()).isEqualTo(MetricRefusal.UNKNOWN_SUBJECT);
                    assertThat(refusal.getMessage()).contains("no rollup declaration");
                });
    }
}
