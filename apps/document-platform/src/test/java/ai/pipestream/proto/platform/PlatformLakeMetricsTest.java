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
import ai.pipestream.proto.search.door.RepoDocumentMapping;
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
 * and the metric door answers {@code METRIC_BACKEND_ICEBERG} queries
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
            });
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
