package ai.pipestream.proto.platform;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.pipestream.proto.lake.iceberg.IcebergSink;
import ai.pipestream.proto.lake.iceberg.LocalFileIO;
import ai.pipestream.proto.metric.DescribeMappingRequest;
import ai.pipestream.proto.metric.MemberRef;
import ai.pipestream.proto.metric.MetricBackend;
import ai.pipestream.proto.metric.MetricServiceGrpc;
import ai.pipestream.proto.metric.QueryMetricsRequest;
import ai.pipestream.proto.metric.QueryMetricsResponse;
import ai.pipestream.proto.repo.v1.Document;
import ai.pipestream.proto.repo.v1.SearchMetadata;
import ai.pipestream.proto.search.service.RepoDocumentMapping;
import com.google.protobuf.Timestamp;
import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.nio.file.Path;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The lake engine mounted through the platform's own configuration
 * family: the Iceberg sink writes the {@code repo-document} table into a
 * one-container lake (a sqlite JDBC catalog over a local warehouse), the
 * platform boots with {@code DOCUMENT_PLATFORM_METRICS_ICEBERG_*} set,
 * and the metric service answers {@code METRIC_BACKEND_ICEBERG} queries
 * over the table while refusing an unset backend by naming both mounted
 * engines. A lake without the subject's table refuses by name instead of
 * answering zero.
 */
class PlatformLakeMetricsTest {

    @TempDir
    Path work;

    private static Document document(String docId, String type, String processedAt) {
        return Document.newBuilder()
                .setDocId(docId)
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .setTitle("Title of " + docId)
                        .setBody("A corpus the lake engine aggregates.")
                        .setDocumentType(type)
                        .setProcessedDate(Timestamp.newBuilder()
                                .setSeconds(Instant.parse(processedAt).getEpochSecond())))
                .build();
    }

    /** Writes the corpus the way the lake gets written for real: the sink. */
    private static void seedLake(Path lake, List<Document> corpus) throws Exception {
        try (JdbcCatalog catalog = new JdbcCatalog()) {
            // A JDBC catalog scopes tables by catalog name: the writer must
            // share the reader's name or its tables are invisible there.
            catalog.initialize(MetricsIcebergConfig.CATALOG_NAME, Map.of(
                    CatalogProperties.URI, "jdbc:sqlite:" + lake.resolve("catalog.db"),
                    CatalogProperties.WAREHOUSE_LOCATION, lake.toString(),
                    CatalogProperties.FILE_IO_IMPL, LocalFileIO.class.getName()));
            catalog.createNamespace(Namespace.of(MetricsIcebergConfig.DEFAULT_NAMESPACE));
            Table table = IcebergSink.ensureTable(catalog,
                    TableIdentifier.of(MetricsIcebergConfig.DEFAULT_NAMESPACE,
                            RepoDocumentMapping.SUBJECT),
                    Document.getDescriptor());
            IcebergSink.append(table, Document.getDescriptor(), corpus);
        }
    }

    private static Map<String, String> lakeEnvironment(Path lake) {
        Map<String, String> environment = new HashMap<>();
        environment.put(DocumentPlatformConfig.ENV_SEARCH_READ_ONLY, "true");
        environment.put(MetricsIcebergConfig.ENV_CATALOG_URI,
                "jdbc:sqlite:" + lake.resolve("catalog.db"));
        environment.put(MetricsIcebergConfig.ENV_WAREHOUSE, lake.toString());
        return environment;
    }

    private DocumentPlatformConfig config(Path lake) {
        return new DocumentPlatformConfig(
                null, null, null, 0, 0, 0, 0,
                null, null, null,
                60L, 1, 0, work.resolve("index"), 0, 0,
                List.of("search", "metrics"), lakeEnvironment(lake));
    }

    private static void withMetricsStub(DocumentPlatform platform,
            Consumer<MetricServiceGrpc.MetricServiceBlockingStub> body) {
        ManagedChannel channel = NettyChannelBuilder
                .forAddress("127.0.0.1", platform.metricsPort())
                .usePlaintext().build();
        try {
            body.accept(MetricServiceGrpc.newBlockingStub(channel));
        } finally {
            channel.shutdownNow();
        }
    }

    @Test
    void theFamilyMountsTheLakeEngineBesideLuceneAndAnswersOverTheSinksTable()
            throws Exception {
        Path lake = work.resolve("lake");
        java.nio.file.Files.createDirectories(lake);
        seedLake(lake, List.of(
                document("doc-1", "PDF", "2026-07-10T10:00:00Z"),
                document("doc-2", "HTML", "2026-08-05T10:00:00Z"),
                document("doc-3", "PDF", "2026-08-20T10:00:00Z")));

        try (DocumentPlatform platform = DocumentPlatform.start(config(lake), null)) {
            withMetricsStub(platform, stub -> {
                assertThat(stub.describeMapping(DescribeMappingRequest.newBuilder()
                        .setMappingSubject(RepoDocumentMapping.SUBJECT)
                        .build()).getBackendsList())
                        .containsExactlyInAnyOrder(
                                MetricBackend.METRIC_BACKEND_LUCENE,
                                MetricBackend.METRIC_BACKEND_ICEBERG);

                // Two engines mounted: an unset backend refuses, never picks.
                assertThatThrownBy(() -> stub.queryMetrics(
                        QueryMetricsRequest.newBuilder()
                                .setMappingSubject(RepoDocumentMapping.SUBJECT)
                                .addMeasures("documents")
                                .setLimit(10)
                                .build()))
                        .isInstanceOf(StatusRuntimeException.class)
                        .hasMessageContaining("ambiguous-backend");

                QueryMetricsResponse counted = stub.queryMetrics(
                        QueryMetricsRequest.newBuilder()
                                .setMappingSubject(RepoDocumentMapping.SUBJECT)
                                .setBackend(MetricBackend.METRIC_BACKEND_ICEBERG)
                                .addMeasures("documents")
                                .setLimit(10)
                                .build());
                assertThat(counted.getRowsList()).hasSize(1);
                assertThat(counted.getRows(0).getMeasuresMap())
                        .containsEntry("documents", 3.0);
                assertThat(counted.getPhysicalPlan()).contains("SELECT");

                QueryMetricsResponse byType = stub.queryMetrics(
                        QueryMetricsRequest.newBuilder()
                                .setMappingSubject(RepoDocumentMapping.SUBJECT)
                                .setBackend(MetricBackend.METRIC_BACKEND_ICEBERG)
                                .addMeasures("documents")
                                .addDimensions(MemberRef.newBuilder()
                                        .setName("document_type"))
                                .setLimit(10)
                                .build());
                assertThat(byType.getRowsList()).hasSize(2);
                assertThat(byType.getRows(0).getDimensionsMap())
                        .containsEntry("document_type", "HTML");
                assertThat(byType.getRows(0).getMeasuresMap())
                        .containsEntry("documents", 1.0);
                assertThat(byType.getRows(1).getDimensionsMap())
                        .containsEntry("document_type", "PDF");
                assertThat(byType.getRows(1).getMeasuresMap())
                        .containsEntry("documents", 2.0);

                // The declared rollup lands in the same lake: the whole
                // group-by answer replaces protomolt.documents_by_type.
                ai.pipestream.proto.metric.RebuildRollupResponse written =
                        stub.rebuildRollup(ai.pipestream.proto.metric
                                .RebuildRollupRequest.newBuilder()
                                .setMappingSubject(RepoDocumentMapping.SUBJECT)
                                .setBackend(MetricBackend.METRIC_BACKEND_ICEBERG)
                                .addMeasures("documents")
                                .addDimensions(MemberRef.newBuilder()
                                        .setName("document_type"))
                                .setTable("documents_by_type")
                                .build());
                assertThat(written.getTable()).isEqualTo("protomolt.documents_by_type");
                assertThat(written.getRowsWritten()).isEqualTo(2);
                assertThat(written.getSnapshotId()).isNotZero();
                assertThat(written.getPhysicalPlan()).contains("SELECT");

                // The rebuilt rollup is instantly a queryable subject: it
                // resolves against the lake, serving its COUNT column as a
                // SUM (summing counts is counting), single-engine so the
                // backend stays unset.
                QueryMetricsResponse fromRollup = stub.queryMetrics(
                        QueryMetricsRequest.newBuilder()
                                .setMappingSubject("rollup:documents_by_type")
                                .addMeasures("documents")
                                .setLimit(10)
                                .build());
                assertThat(fromRollup.getBackend())
                        .isEqualTo(MetricBackend.METRIC_BACKEND_ICEBERG);
                assertThat(fromRollup.getRows(0).getMeasuresMap())
                        .containsEntry("documents", 3.0);
                assertThat(stub.describeMapping(DescribeMappingRequest.newBuilder()
                        .setMappingSubject("rollup:documents_by_type")
                        .build()).getMembersList())
                        .extracting(member -> member.getName())
                        .containsExactlyInAnyOrder("document_type", "documents");
            });
        }

        // The rollup reads back straight off its Parquet: two rows, the
        // whole answer.
        assertThat(rollupRows(lake, "documents_by_type"))
                .containsExactly(Map.entry("HTML", 1.0), Map.entry("PDF", 2.0));
    }

    /** Reads a rollup's (document_type, documents) rows through DuckDB. */
    private static Map<String, Double> rollupRows(Path lake, String table) throws Exception {
        try (JdbcCatalog catalog = new JdbcCatalog()) {
            catalog.initialize(MetricsIcebergConfig.CATALOG_NAME, Map.of(
                    CatalogProperties.URI, "jdbc:sqlite:" + lake.resolve("catalog.db"),
                    CatalogProperties.WAREHOUSE_LOCATION, lake.toString(),
                    CatalogProperties.FILE_IO_IMPL, LocalFileIO.class.getName()));
            Table committed = catalog.loadTable(TableIdentifier.of(
                    MetricsIcebergConfig.DEFAULT_NAMESPACE, table));
            java.util.List<String> files = new java.util.ArrayList<>();
            try (org.apache.iceberg.io.CloseableIterable<org.apache.iceberg.FileScanTask>
                    tasks = committed.newScan().planFiles()) {
                tasks.forEach(task -> files.add(task.file().location()));
            }
            Map<String, Double> rows = new java.util.LinkedHashMap<>();
            if (files.isEmpty()) {
                return rows;
            }
            String list = String.join(", ",
                    files.stream().map(file -> "'" + file + "'").toList());
            try (java.sql.Connection connection =
                    java.sql.DriverManager.getConnection("jdbc:duckdb:");
                    java.sql.Statement statement = connection.createStatement();
                    java.sql.ResultSet results = statement.executeQuery(
                            "SELECT document_type, documents FROM read_parquet(["
                                    + list + "]) ORDER BY document_type")) {
                while (results.next()) {
                    rows.put(results.getString(1), results.getDouble(2));
                }
            }
            return rows;
        }
    }

    @Test
    void withoutTheFamilyRollupsLandInTheLazyDefaultLocalLake() throws Exception {
        Path defaultLake = work.resolve("default-lake");

        Map<String, String> environment = new HashMap<>();
        environment.put(DocumentPlatformConfig.ENV_SEARCH_READ_ONLY, "true");
        environment.put(DocumentPlatformConfig.ENV_METRICS_LAKE_DIR,
                defaultLake.toString());
        DocumentPlatformConfig config = new DocumentPlatformConfig(
                null, null, null, 0, 0, 0, 0,
                null, null, null,
                60L, 1, 0, work.resolve("default-index"), 0, 0,
                List.of("search", "metrics"), environment);

        try (DocumentPlatform platform = DocumentPlatform.start(config, null)) {
            // The lake is lazy: booting alone must not create it.
            assertThat(java.nio.file.Files.exists(defaultLake)).isFalse();

            withMetricsStub(platform, stub -> {
                ai.pipestream.proto.metric.RebuildRollupResponse written =
                        stub.rebuildRollup(ai.pipestream.proto.metric
                                .RebuildRollupRequest.newBuilder()
                                .setMappingSubject(RepoDocumentMapping.SUBJECT)
                                .addMeasures("documents")
                                .setTable("documents_total")
                                .build());
                assertThat(written.getTable())
                        .isEqualTo("protomolt.documents_total");
                assertThat(written.getBackend())
                        .isEqualTo(MetricBackend.METRIC_BACKEND_LUCENE);

                // The default lake serves its rollups back as subjects too.
                QueryMetricsResponse fromRollup = stub.queryMetrics(
                        QueryMetricsRequest.newBuilder()
                                .setMappingSubject("rollup:documents_total")
                                .addMeasures("documents")
                                .setLimit(10)
                                .build());
                assertThat(fromRollup.getBackend())
                        .isEqualTo(MetricBackend.METRIC_BACKEND_ICEBERG);
            });

            assertThat(java.nio.file.Files.exists(defaultLake.resolve("catalog.db")))
                    .as("the first rebuild created the local lake")
                    .isTrue();
        }
        // The table is a real Iceberg table any engine can open.
        try (JdbcCatalog catalog = new JdbcCatalog()) {
            catalog.initialize(MetricsIcebergConfig.CATALOG_NAME, Map.of(
                    CatalogProperties.URI,
                    "jdbc:sqlite:" + defaultLake.resolve("catalog.db"),
                    CatalogProperties.WAREHOUSE_LOCATION, defaultLake.toString(),
                    CatalogProperties.FILE_IO_IMPL, LocalFileIO.class.getName()));
            assertThat(catalog.tableExists(TableIdentifier.of(
                    MetricsIcebergConfig.DEFAULT_NAMESPACE, "documents_total")))
                    .isTrue();
        }
    }

    @Test
    void aLakeWithoutTheSubjectsTableRefusesByName() throws Exception {
        Path lake = work.resolve("empty-lake");
        java.nio.file.Files.createDirectories(lake);

        try (DocumentPlatform platform = DocumentPlatform.start(config(lake), null)) {
            withMetricsStub(platform, stub ->
                    assertThatThrownBy(() -> stub.queryMetrics(
                            QueryMetricsRequest.newBuilder()
                                    .setMappingSubject(RepoDocumentMapping.SUBJECT)
                                    .setBackend(MetricBackend.METRIC_BACKEND_ICEBERG)
                                    .addMeasures("documents")
                                    .setLimit(10)
                                    .build()))
                            .isInstanceOf(StatusRuntimeException.class)
                            .hasMessageContaining("FAILED_PRECONDITION")
                            .hasMessageContaining("missing-table")
                            .hasMessageContaining(RepoDocumentMapping.SUBJECT));
        }
    }
}
