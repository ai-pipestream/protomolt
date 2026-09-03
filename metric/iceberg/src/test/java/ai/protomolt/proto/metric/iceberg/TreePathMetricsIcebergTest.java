package ai.protomolt.proto.metric.iceberg;

import static org.assertj.core.api.Assertions.assertThat;

import ai.protomolt.proto.iceberg.IcebergSink;
import ai.protomolt.proto.iceberg.LocalFileIO;
import ai.protomolt.proto.metric.Aggregate;
import ai.protomolt.proto.metric.FieldMetric;
import ai.protomolt.proto.metric.MemberRef;
import ai.protomolt.proto.metric.MemberRole;
import ai.protomolt.proto.metric.MetricBackend;
import ai.protomolt.proto.metric.MetricFilter;
import ai.protomolt.proto.metric.QueryMetricsRequest;
import ai.protomolt.proto.metric.QueryMetricsResponse;
import ai.protomolt.proto.metric.spi.CatalogMetricHintSource;
import ai.protomolt.proto.metric.spi.MetricExecutor;
import ai.protomolt.proto.metric.spi.MetricMapping;
import ai.protomolt.proto.metric.spi.MetricMappings;
import ai.protomolt.proto.metric.spi.MetricQueries;
import ai.protomolt.proto.sources.CompiledProtos;
import ai.protomolt.proto.sources.ProtoSourceCompiler;
import ai.protomolt.proto.sources.ProtoSourceSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import java.nio.file.Path;
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

/**
 * TreePath on the lake backend, end to end through the real Iceberg sink:
 * no chain column is materialized — the TreePath struct the lake already
 * writes is the column, and DuckDB derives the rendered path for both the
 * group-by labels and the descendant-or-self prefix, answering exactly the
 * Lucene backend's numbers over the mirrored corpus. The pathless product
 * (the non-uniform case) must fall out of both.
 */
class TreePathMetricsIcebergTest {

    static final String TREE_PATH_PROTO = """
            syntax = "proto3";
            package ai.pipestream.proto.types.v1;

            message TreePath {
              repeated string segments = 1;
            }
            """;

    static final String PRODUCT_PROTO = """
            syntax = "proto3";
            package catalogit.v1;
            import "ai/pipestream/proto/types/v1/tree_path.proto";

            message Product {
              string id = 1;
              ai.pipestream.proto.types.v1.TreePath category = 2;
              int64 amount = 3;
            }
            """;

    @TempDir
    static Path warehouse;

    static JdbcCatalog catalog;
    static Descriptor product;
    static MetricMapping mapping;
    static Map<MetricBackend, MetricExecutor> executors;

    @BeforeAll
    static void boot() throws Exception {
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("ai/pipestream/proto/types/v1/tree_path.proto", TREE_PATH_PROTO, "test")
                .add("catalogit/v1/product.proto", PRODUCT_PROTO, "test")
                .build());
        product = compiled.descriptorFor("catalogit/v1/product.proto").orElseThrow()
                .findMessageTypeByName("Product");

        catalog = new JdbcCatalog();
        catalog.initialize("test", Map.of(
                CatalogProperties.URI,
                "jdbc:sqlite:" + warehouse.resolve("catalog.db"),
                CatalogProperties.WAREHOUSE_LOCATION, warehouse.toString(),
                CatalogProperties.FILE_IO_IMPL, LocalFileIO.class.getName()));
        catalog.createNamespace(Namespace.of("protomolt"));
        Table table = IcebergSink.ensureTable(
                catalog, TableIdentifier.of("protomolt", "products"), product);
        IcebergSink.append(table, product, List.of(
                productRow("p-1", 100, "electronics", "computers", "laptops"),
                productRow("p-2", 50, "electronics", "computers"),
                productRow("p-3", 200, "electronics", "audio"),
                productRow("p-4", 30, "media", "books"),
                // The non-uniform case: no category at all.
                productRow("p-5", 999),
                // A single-segment path that merely STARTS with
                // "electronics": never in the "electronics" subtree.
                productRow("p-6", 7, "electronicsx")));

        CatalogMetricHintSource metrics = new CatalogMetricHintSource()
                .put("catalogit.v1.Product", "category", FieldMetric.newBuilder()
                        .setRole(MemberRole.MEMBER_ROLE_DIMENSION).build())
                .put("catalogit.v1.Product", "amount", FieldMetric.newBuilder()
                        .setRole(MemberRole.MEMBER_ROLE_MEASURE)
                        .setAggregate(Aggregate.AGGREGATE_SUM)
                        .setName("revenue").build());
        mapping = MetricMappings.build("products", product, metrics);

        Table served = catalog.loadTable(TableIdentifier.of("protomolt", "products"));
        IcebergMetricExecutor executor = new IcebergMetricExecutor(subject -> served);
        executors = Map.of(MetricBackend.METRIC_BACKEND_ICEBERG, executor);
    }

    @AfterAll
    static void shutdown() throws Exception {
        catalog.close();
    }

    static DynamicMessage productRow(String id, long amount, String... segments) {
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(product)
                .setField(product.findFieldByName("id"), id)
                .setField(product.findFieldByName("amount"), amount);
        if (segments.length > 0) {
            FieldDescriptor category = product.findFieldByName("category");
            DynamicMessage.Builder path = DynamicMessage.newBuilder(
                    category.getMessageType());
            FieldDescriptor segmentsField =
                    category.getMessageType().findFieldByName("segments");
            for (String segment : segments) {
                path.addRepeatedField(segmentsField, segment);
            }
            builder.setField(category, path.build());
        }
        return builder.build();
    }

    static QueryMetricsRequest.Builder request() {
        return QueryMetricsRequest.newBuilder()
                .setMappingSubject("products").setLimit(100).addMeasures("revenue");
    }

    static Map<String, Double> revenueByCategory(QueryMetricsResponse response) {
        return response.getRowsList().stream()
                .collect(java.util.stream.Collectors.toMap(
                        row -> row.getDimensionsOrThrow("category"),
                        row -> row.getMeasuresOrThrow("revenue")));
    }

    @Test
    void treePathDimensionsBucketByTheRenderedLeafPath() {
        QueryMetricsResponse response = MetricQueries.query(mapping, executors,
                request()
                        .addDimensions(MemberRef.newBuilder().setName("category"))
                        .build());
        // Exactly the Lucene backend's buckets and numbers; p-5 excluded.
        assertThat(revenueByCategory(response)).containsOnly(
                Map.entry("electronics/computers/laptops", 100.0),
                Map.entry("electronics/computers", 50.0),
                Map.entry("electronics/audio", 200.0),
                Map.entry("media/books", 30.0),
                Map.entry("electronicsx", 7.0));
        assertThat(response.getPhysicalPlan()).contains("array_to_string");
    }

    @Test
    void aPrefixStringSiblingStaysOutOfTheSubtree() {
        QueryMetricsResponse response = MetricQueries.query(mapping, executors,
                request()
                        .addFilters(MetricFilter.newBuilder()
                                .setMember("category")
                                .setPrefix(ai.protomolt.proto.types.TreePath.newBuilder()
                                        .addSegments("electronics")))
                        .build());
        // Hand-checked subtree: 100 + 50 + 200. The "/" in the prefix
        // predicate is what keeps "electronicsx" (7) out.
        assertThat(response.getRowsList()).singleElement().satisfies(row ->
                assertThat(row.getMeasuresOrThrow("revenue")).isEqualTo(350.0));
    }

    @Test
    void aPrefixFilterSelectsTheSubtreeInSql() {
        QueryMetricsResponse response = MetricQueries.query(mapping, executors,
                request()
                        .addFilters(MetricFilter.newBuilder()
                                .setMember("category")
                                .setPrefix(ai.protomolt.proto.types.TreePath.newBuilder()
                                        .addSegments("electronics")
                                        .addSegments("computers")))
                        .build());
        // Hand-checked: p-1 (100, descendant) + p-2 (50, the path itself);
        // "electronics/audio" and the pathless p-5 stay out.
        assertThat(response.getRowsList()).singleElement().satisfies(row ->
                assertThat(row.getMeasuresOrThrow("revenue")).isEqualTo(150.0));
        assertThat(response.getPhysicalPlan()).contains("starts_with");
    }
}
