package ai.pipestream.proto.metric.iceberg;

import static org.assertj.core.api.Assertions.assertThat;

import ai.pipestream.proto.iceberg.IcebergSink;
import ai.pipestream.proto.iceberg.LocalFileIO;
import ai.pipestream.proto.metric.Aggregate;
import ai.pipestream.proto.metric.FieldMetric;
import ai.pipestream.proto.metric.MemberRef;
import ai.pipestream.proto.metric.MemberRole;
import ai.pipestream.proto.metric.MetricBackend;
import ai.pipestream.proto.metric.MetricRow;
import ai.pipestream.proto.metric.QueryMetricsRequest;
import ai.pipestream.proto.metric.QueryMetricsResponse;
import ai.pipestream.proto.metric.TimeGrain;
import ai.pipestream.proto.metric.spi.CatalogMetricHintSource;
import ai.pipestream.proto.metric.spi.MetricExecutor;
import ai.pipestream.proto.metric.spi.MetricMapping;
import ai.pipestream.proto.metric.spi.MetricMappings;
import ai.pipestream.proto.metric.spi.MetricQueries;
import ai.pipestream.proto.sources.CompiledProtos;
import ai.pipestream.proto.sources.ProtoSourceCompiler;
import ai.pipestream.proto.sources.ProtoSourceSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Timestamp;
import java.nio.file.Path;
import java.time.Instant;
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

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The lake backend end to end: documents append through the real Iceberg
 * sink, and SUM / a filtered COUNT / month buckets / a nested group-by /
 * COUNT_DISTINCT answer exactly the hand-checked numbers through DuckDB,
 * with the rendered SQL as the physical plan. The corpus mirrors the
 * Lucene backend's, so the two engines demonstrably agree.
 */
class IcebergMetricExecutorTest {

    static final String PROTO = """
            syntax = "proto3";
            package ordersit.v1;
            import "google/protobuf/timestamp.proto";

            message Order {
              string id = 1;
              string segment = 2;
              bool paying = 3;
              int64 amount = 4;
              google.protobuf.Timestamp created_at = 5;
              Meta meta = 6;
              string customer = 7;
              message Meta {
                string region = 1;
              }
            }
            """;

    @TempDir
    static Path warehouse;

    static JdbcCatalog catalog;
    static Descriptor order;
    static MetricMapping mapping;
    static Map<MetricBackend, MetricExecutor> executors;

    @BeforeAll
    static void boot() throws Exception {
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("ordersit/v1/order.proto", PROTO, "test").build());
        order = compiled.descriptorFor("ordersit/v1/order.proto").orElseThrow()
                .findMessageTypeByName("Order");

        // InMemoryCatalog hardcodes its in-memory FileIO; the JDBC catalog
        // honors io-impl, so the sink's Parquet lands on real disk.
        catalog = new JdbcCatalog();
        catalog.initialize("test", Map.of(
                CatalogProperties.URI,
                "jdbc:sqlite:" + warehouse.resolve("catalog.db"),
                CatalogProperties.WAREHOUSE_LOCATION, warehouse.toString(),
                CatalogProperties.FILE_IO_IMPL, LocalFileIO.class.getName()));
        catalog.createNamespace(Namespace.of("protomolt"));
        Table table = IcebergSink.ensureTable(
                catalog, TableIdentifier.of("protomolt", "orders"), order);
        IcebergSink.append(table, order, List.of(
                orderRow("o-1", "smb", true, 100, "2026-07-10T10:00:00Z", "east", "c-1"),
                orderRow("o-2", "smb", false, 50, "2026-07-15T10:00:00Z", "west", "c-1"),
                orderRow("o-3", "mid", true, 200, "2026-08-01T10:00:00Z", "east", "c-2"),
                orderRow("o-4", "smb", true, 30, "2026-08-05T10:00:00Z", "east", "c-1"),
                // The non-uniform case: no region, so region group-bys must
                // exclude this row the way the doc-values backend would.
                orderRow("o-5", "smb", false, 20, "2026-08-10T10:00:00Z", "", "c-3")));

        CatalogMetricHintSource metrics = new CatalogMetricHintSource()
                .put("ordersit.v1.Order", "segment", FieldMetric.newBuilder()
                        .setRole(MemberRole.MEMBER_ROLE_DIMENSION).build())
                .put("ordersit.v1.Order", "created_at", FieldMetric.newBuilder()
                        .setRole(MemberRole.MEMBER_ROLE_DIMENSION)
                        .setDefaultGrain(TimeGrain.TIME_GRAIN_MONTH).build())
                .put("ordersit.v1.Order.Meta", "region", FieldMetric.newBuilder()
                        .setRole(MemberRole.MEMBER_ROLE_DIMENSION).build())
                .put("ordersit.v1.Order", "amount", FieldMetric.newBuilder()
                        .setRole(MemberRole.MEMBER_ROLE_MEASURE)
                        .setAggregate(Aggregate.AGGREGATE_SUM)
                        .setName("revenue").build())
                .put("ordersit.v1.Order", "id", FieldMetric.newBuilder()
                        .setRole(MemberRole.MEMBER_ROLE_MEASURE)
                        .setAggregate(Aggregate.AGGREGATE_COUNT)
                        .setName("orders")
                        .setFilterCel("this.paying == true").build())
                .put("ordersit.v1.Order", "customer", FieldMetric.newBuilder()
                        .setRole(MemberRole.MEMBER_ROLE_MEASURE)
                        .setAggregate(Aggregate.AGGREGATE_COUNT_DISTINCT)
                        .setName("customers").build())
                // A synthetic member: paying_count with no phantom field.
                .putMessage("ordersit.v1.Order", ai.pipestream.proto.metric.MessageMetric
                        .newBuilder()
                        .addMembers(FieldMetric.newBuilder()
                                .setRole(MemberRole.MEMBER_ROLE_MEASURE)
                                .setAggregate(Aggregate.AGGREGATE_COUNT)
                                .setName("paying_count")
                                .setFilterCel("this.paying == true"))
                        .build());
        mapping = MetricMappings.build("orders", order, metrics);

        Table served = catalog.loadTable(TableIdentifier.of("protomolt", "orders"));
        IcebergMetricExecutor executor = new IcebergMetricExecutor(subject -> served);
        executors = Map.of(MetricBackend.METRIC_BACKEND_ICEBERG, executor);
    }

    @AfterAll
    static void shutdown() throws Exception {
        catalog.close();
    }

    static DynamicMessage orderRow(String id, String segment, boolean paying, long amount,
            String createdAt, String region, String customer) {
        FieldDescriptor metaField = order.findFieldByName("meta");
        Instant instant = Instant.parse(createdAt);
        return DynamicMessage.newBuilder(order)
                .setField(order.findFieldByName("id"), id)
                .setField(order.findFieldByName("segment"), segment)
                .setField(order.findFieldByName("paying"), paying)
                .setField(order.findFieldByName("amount"), amount)
                .setField(order.findFieldByName("created_at"), Timestamp.newBuilder()
                        .setSeconds(instant.getEpochSecond()).build())
                .setField(metaField, DynamicMessage.newBuilder(metaField.getMessageType())
                        .setField(metaField.getMessageType().findFieldByName("region"), region)
                        .build())
                .setField(order.findFieldByName("customer"), customer)
                .build();
    }

    static QueryMetricsRequest.Builder query(String... measures) {
        QueryMetricsRequest.Builder request = QueryMetricsRequest.newBuilder()
                .setMappingSubject("orders").setLimit(10);
        for (String measure : measures) {
            request.addMeasures(measure);
        }
        return request;
    }

    static Map<String, Map<String, Double>> byFirstDimension(QueryMetricsResponse response,
            String dimension) {
        return response.getRowsList().stream().collect(
                java.util.stream.Collectors.toMap(
                        row -> row.getDimensionsMap().get(dimension),
                        MetricRow::getMeasuresMap,
                        (a, b) -> a,
                        java.util.LinkedHashMap::new));
    }

    @Test
    void aDateRangeFilterKeepsOnlyTheWindow() {
        // Hand-checked July orders: o-1 (100) + o-2 (50).
        QueryMetricsResponse july = MetricQueries.query(mapping, executors,
                query("revenue")
                        .addFilters(ai.pipestream.proto.metric.MetricFilter.newBuilder()
                                .setMember("created_at")
                                .setRange(ai.pipestream.proto.types.DateRange
                                        .newBuilder()
                                        .setBegin("2026-07-01").setEnd("2026-07-31")))
                        .build());
        assertThat(july.getRows(0).getMeasuresOrThrow("revenue")).isEqualTo(150.0);
        assertThat(july.getPhysicalPlan()).contains("epoch_ms");

        // An open lower side: everything from August on (200 + 30 + 20).
        QueryMetricsResponse fromAugust = MetricQueries.query(mapping, executors,
                query("revenue")
                        .addFilters(ai.pipestream.proto.metric.MetricFilter.newBuilder()
                                .setMember("created_at")
                                .setRange(ai.pipestream.proto.types.DateRange
                                        .newBuilder().setBegin("2026-08-01")))
                        .build());
        assertThat(fromAugust.getRows(0).getMeasuresOrThrow("revenue")).isEqualTo(250.0);
    }

    @Test
    void aSyntheticFilteredCountAnswersWithoutABackingField() {
        // Hand-checked paying orders: o-1, o-3, o-4.
        QueryMetricsResponse total = MetricQueries.query(mapping, executors,
                query("paying_count").build());
        assertThat(total.getRows(0).getMeasuresOrThrow("paying_count")).isEqualTo(3.0);

        QueryMetricsResponse bySegment = MetricQueries.query(mapping, executors,
                query("paying_count")
                        .addDimensions(MemberRef.newBuilder().setName("segment"))
                        .build());
        Map<String, Map<String, Double>> rows = byFirstDimension(bySegment, "segment");
        assertThat(rows.get("smb")).containsEntry("paying_count", 2.0);
        assertThat(rows.get("mid")).containsEntry("paying_count", 1.0);
    }

    @Test
    void sumGroupsBySegmentWithHandCheckedNumbers() {
        QueryMetricsResponse response = MetricQueries.query(mapping, executors,
                query("revenue")
                        .addDimensions(MemberRef.newBuilder().setName("segment"))
                        .build());
        Map<String, Map<String, Double>> rows = byFirstDimension(response, "segment");
        assertThat(rows.keySet()).containsExactly("mid", "smb");
        assertThat(rows.get("smb")).containsEntry("revenue", 200.0);
        assertThat(rows.get("mid")).containsEntry("revenue", 200.0);
        assertThat(response.getPhysicalPlan())
                .contains("SELECT").contains("read_parquet");
    }

    @Test
    void aFilteredCountOnlySeesItsRows() {
        QueryMetricsResponse response = MetricQueries.query(mapping, executors,
                query("orders")
                        .addDimensions(MemberRef.newBuilder().setName("segment"))
                        .build());
        Map<String, Map<String, Double>> rows = byFirstDimension(response, "segment");
        assertThat(rows.get("smb")).containsEntry("orders", 2.0);
        assertThat(rows.get("mid")).containsEntry("orders", 1.0);
    }

    @Test
    void monthBucketsMatchTheLuceneLabels() {
        QueryMetricsResponse response = MetricQueries.query(mapping, executors,
                query("revenue")
                        .addDimensions(MemberRef.newBuilder().setName("created_at"))
                        .build());
        Map<String, Map<String, Double>> rows = byFirstDimension(response, "created_at");
        assertThat(rows.keySet()).containsExactly("2026-07", "2026-08");
        assertThat(rows.get("2026-07")).containsEntry("revenue", 150.0);
        assertThat(rows.get("2026-08")).containsEntry("revenue", 250.0);
    }

    @Test
    void aNestedDimensionGroupsThroughItsFieldPath() {
        QueryMetricsResponse response = MetricQueries.query(mapping, executors,
                query("orders")
                        .addDimensions(MemberRef.newBuilder().setName("region"))
                        .build());
        Map<String, Map<String, Double>> rows = byFirstDimension(response, "region");
        assertThat(rows.keySet()).containsExactly("east", "west");
        assertThat(rows.get("east")).containsEntry("orders", 3.0);
        assertThat(rows.get("west")).containsEntry("orders", 0.0);
    }

    @Test
    void countDistinctIsThisBackendsExtraCapability() {
        QueryMetricsResponse ungrouped = MetricQueries.query(mapping, executors,
                query("customers").build());
        assertThat(ungrouped.getRowsList()).hasSize(1);
        assertThat(ungrouped.getRows(0).getMeasuresMap()).containsEntry("customers", 3.0);
    }

    @Test
    void anUnknownMemberStillRefusesBeforeTheEngine() {
        assertThatThrownBy(() -> MetricQueries.query(mapping, executors,
                query("nonsense").build()))
                .hasMessageContaining("unknown member 'nonsense'");
    }
}
