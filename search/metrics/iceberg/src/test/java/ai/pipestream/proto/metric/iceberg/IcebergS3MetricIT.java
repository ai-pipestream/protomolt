package ai.pipestream.proto.metric.iceberg;

import static org.assertj.core.api.Assertions.assertThat;

import ai.pipestream.proto.lake.iceberg.IcebergSink;
import ai.pipestream.proto.lake.iceberg.s3.S3Catalogs;
import ai.pipestream.proto.metric.Aggregate;
import ai.pipestream.proto.metric.FieldMetric;
import ai.pipestream.proto.metric.MemberRef;
import ai.pipestream.proto.metric.MemberRole;
import ai.pipestream.proto.metric.MetricBackend;
import ai.pipestream.proto.metric.QueryMetricsRequest;
import ai.pipestream.proto.metric.QueryMetricsResponse;
import ai.pipestream.proto.metric.spi.CatalogMetricHintSource;
import ai.pipestream.proto.metric.spi.MetricExecutor;
import ai.pipestream.proto.metric.spi.MetricMapping;
import ai.pipestream.proto.metric.spi.MetricMappings;
import ai.pipestream.proto.metric.spi.MetricQueries;
import ai.pipestream.proto.sources.CompiledProtos;
import ai.pipestream.proto.sources.ProtoSourceCompiler;
import ai.pipestream.proto.sources.ProtoSourceSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import java.net.URI;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * The metric reader over an object-store lake: the sink writes the table
 * to LocalStack S3 through {@code S3FileIO}, the executor materializes
 * the scan through the table's own FileIO (no DuckDB extension, no
 * second credential path), the numbers come back hand-checked, and the
 * physical plan says what moved.
 */
@Testcontainers(disabledWithoutDocker = true)
class IcebergS3MetricIT {

    @Container
    static final LocalStackContainer S3 = new LocalStackContainer(
            DockerImageName.parse("localstack/localstack:3.8")).withServices("s3");

    private static final String PROTO = """
            syntax = "proto3";
            package lakes3.v1;
            message Order {
              string id = 1;
              string segment = 2;
              int64 amount = 3;
            }
            """;

    @TempDir
    static Path work;

    static JdbcCatalog catalog;
    static MetricMapping mapping;
    static Map<MetricBackend, MetricExecutor> executors;

    @BeforeAll
    static void boot() throws Exception {
        try (software.amazon.awssdk.services.s3.S3Client s3 =
                software.amazon.awssdk.services.s3.S3Client.builder()
                        .region(software.amazon.awssdk.regions.Region.of(S3.getRegion()))
                        .endpointOverride(URI.create(S3.getEndpoint().toString()))
                        .forcePathStyle(true)
                        .credentialsProvider(software.amazon.awssdk.auth.credentials
                                .StaticCredentialsProvider.create(
                                        software.amazon.awssdk.auth.credentials
                                                .AwsBasicCredentials.create(
                                                        S3.getAccessKey(), S3.getSecretKey())))
                        .httpClientBuilder(software.amazon.awssdk.http.urlconnection
                                .UrlConnectionHttpClient.builder())
                        .build()) {
            s3.createBucket(b -> b.bucket("metric-lake"));
        }

        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("lakes3/v1/order.proto", PROTO, "test").build());
        Descriptor order = compiled.descriptorFor("lakes3/v1/order.proto").orElseThrow()
                .findMessageTypeByName("Order");

        // Local sqlite metadata, S3 file plane: the one-container catalog
        // over an object-store lake.
        Map<String, String> properties = new HashMap<>(S3Catalogs.pathStyle(
                S3.getEndpoint().toString(), S3.getRegion(),
                S3.getAccessKey(), S3.getSecretKey()));
        properties.put(CatalogProperties.URI, "jdbc:sqlite:" + work.resolve("catalog.db"));
        properties.put(CatalogProperties.WAREHOUSE_LOCATION, "s3://metric-lake/lake");
        catalog = new JdbcCatalog();
        catalog.initialize("s3-metrics", properties);
        catalog.createNamespace(Namespace.of("protomolt"));

        Table table = IcebergSink.ensureTable(
                catalog, TableIdentifier.of("protomolt", "orders"), order);
        IcebergSink.append(table, order, List.of(
                row(order, "o-1", "smb", 100),
                row(order, "o-2", "smb", 50),
                row(order, "o-3", "mid", 200)));

        mapping = MetricMappings.build("orders", order, new CatalogMetricHintSource()
                .put("lakes3.v1.Order", "segment", FieldMetric.newBuilder()
                        .setRole(MemberRole.MEMBER_ROLE_DIMENSION).build())
                .put("lakes3.v1.Order", "amount", FieldMetric.newBuilder()
                        .setRole(MemberRole.MEMBER_ROLE_MEASURE)
                        .setAggregate(Aggregate.AGGREGATE_SUM)
                        .setName("revenue").build()));
        Table served = catalog.loadTable(TableIdentifier.of("protomolt", "orders"));
        executors = Map.of(MetricBackend.METRIC_BACKEND_ICEBERG,
                new IcebergMetricExecutor(subject -> served));
    }

    @AfterAll
    static void shutdown() throws Exception {
        if (catalog != null) {
            catalog.close();
        }
    }

    static DynamicMessage row(Descriptor order, String id, String segment, long amount) {
        return DynamicMessage.newBuilder(order)
                .setField(order.findFieldByName("id"), id)
                .setField(order.findFieldByName("segment"), segment)
                .setField(order.findFieldByName("amount"), amount)
                .build();
    }

    @Test
    void theReaderAnswersOverTheObjectStoreAndThePlanSaysWhatMoved() {
        QueryMetricsResponse bySegment = MetricQueries.query(mapping, executors,
                QueryMetricsRequest.newBuilder()
                        .setMappingSubject("orders")
                        .addMeasures("revenue")
                        .addDimensions(MemberRef.newBuilder().setName("segment"))
                        .setLimit(10)
                        .build());
        // Hand-checked: smb = 100 + 50, mid = 200.
        assertThat(bySegment.getRowsList()).hasSize(2);
        assertThat(bySegment.getRows(0).getDimensionsMap()).containsEntry("segment", "mid");
        assertThat(bySegment.getRows(0).getMeasuresMap()).containsEntry("revenue", 200.0);
        assertThat(bySegment.getRows(1).getDimensionsMap()).containsEntry("segment", "smb");
        assertThat(bySegment.getRows(1).getMeasuresMap()).containsEntry("revenue", 150.0);
        assertThat(bySegment.getPhysicalPlan())
                .contains("materialized from the object store");
    }
}
