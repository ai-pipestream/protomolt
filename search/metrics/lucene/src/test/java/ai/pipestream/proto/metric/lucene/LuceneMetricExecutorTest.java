package ai.pipestream.proto.metric.lucene;

import ai.pipestream.proto.index.spi.CatalogIndexingHintSource;
import ai.pipestream.proto.index.spi.IndexFieldKind;
import ai.pipestream.proto.index.spi.IndexMapping;
import ai.pipestream.proto.index.spi.IndexMappingFactory;
import ai.pipestream.proto.index.spi.ResolvedFieldHint;
import ai.pipestream.proto.metric.Aggregate;
import ai.pipestream.proto.metric.FieldMetric;
import ai.pipestream.proto.metric.MemberRole;
import ai.pipestream.proto.metric.MetricBackend;
import ai.pipestream.proto.metric.MetricFilter;
import ai.pipestream.proto.metric.MemberRef;
import ai.pipestream.proto.metric.MetricRow;
import ai.pipestream.proto.metric.QueryMetricsRequest;
import ai.pipestream.proto.metric.QueryMetricsResponse;
import ai.pipestream.proto.metric.TimeGrain;
import ai.pipestream.proto.metric.spi.CatalogMetricHintSource;
import ai.pipestream.proto.metric.spi.MetricExecutor;
import ai.pipestream.proto.metric.spi.MetricMapping;
import ai.pipestream.proto.metric.spi.MetricMappings;
import ai.pipestream.proto.metric.spi.MetricQueries;
import ai.pipestream.proto.metric.spi.MetricRefusal;
import ai.pipestream.proto.search.door.LuceneSearchStore;
import ai.pipestream.proto.search.door.ServedMapping;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Timestamp;
import com.google.protobuf.TimestampProto;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The Lucene backend end to end over the real door store: documents index
 * through the door's mapper, and SUM / COUNT / group-by / a filtered
 * measure / date grains answer exactly the hand-checked numbers. A field
 * without the doc values a query needs fails loudly naming the indexing
 * hint, never answering zero.
 */
class LuceneMetricExecutorTest {

    @TempDir
    static Path work;

    static Descriptor order;
    static LuceneSearchStore store;
    static MetricMapping mapping;
    static IndexMapping storeMapping;
    static Map<MetricBackend, MetricExecutor> executors;

    @BeforeAll
    static void boot() throws Exception {
        order = orderDescriptor();

        CatalogIndexingHintSource hints = new CatalogIndexingHintSource();
        hints.put("test.Order", "id",
                ResolvedFieldHint.builder(IndexFieldKind.KEYWORD).stored(true).build());
        hints.put("test.Order", "segment",
                ResolvedFieldHint.builder(IndexFieldKind.KEYWORD).facetable(true).build());
        hints.put("test.Order", "paying",
                ResolvedFieldHint.builder(IndexFieldKind.BOOLEAN).facetable(true).build());
        hints.put("test.Order", "amount",
                ResolvedFieldHint.builder(IndexFieldKind.INT64).sortable(true).build());
        hints.put("test.Order", "created_at",
                ResolvedFieldHint.builder(IndexFieldKind.DATE).facetable(true).build());
        hints.put("test.Order", "ratio",
                ResolvedFieldHint.builder(IndexFieldKind.DOUBLE).facetable(true).build());
        hints.put("test.Order", "customer",
                ResolvedFieldHint.builder(IndexFieldKind.KEYWORD).facetable(true).build());
        hints.put("test.Order", "zone",
                ResolvedFieldHint.builder(IndexFieldKind.INT64).facetable(true).build());
        // title stays a plain text field: indexed, no doc values.
        IndexMapping indexMapping = IndexMappingFactory.defaults(hints).create(order);
        storeMapping = indexMapping;
        ServedMapping served = new ServedMapping(indexMapping, "id",
                message -> (String) message.getField(order.findFieldByName("id")), null);
        store = new LuceneSearchStore(work, Map.of("orders", served));

        index("o-1", "smb", true, 100, 0.5, "2026-07-10T10:00:00Z", "alpha invoice",
                "cust-a", 10);
        index("o-2", "smb", false, 50, 0.25, "2026-07-15T10:00:00Z", "beta invoice",
                "cust-b", 20);
        index("o-3", "mid", true, 200, 0.75, "2026-08-01T10:00:00Z", "gamma invoice",
                "cust-a", 10);
        index("o-4", "smb", true, 30, 0.5, "2026-08-05T10:00:00Z", "delta invoice",
                "cust-c", 10);

        CatalogMetricHintSource metrics = new CatalogMetricHintSource()
                .put("test.Order", "segment", FieldMetric.newBuilder()
                        .setRole(MemberRole.MEMBER_ROLE_DIMENSION).build())
                .put("test.Order", "title", FieldMetric.newBuilder()
                        .setRole(MemberRole.MEMBER_ROLE_DIMENSION).build())
                .put("test.Order", "created_at", FieldMetric.newBuilder()
                        .setRole(MemberRole.MEMBER_ROLE_DIMENSION)
                        .setDefaultGrain(TimeGrain.TIME_GRAIN_MONTH).build())
                .put("test.Order", "amount", FieldMetric.newBuilder()
                        .setRole(MemberRole.MEMBER_ROLE_MEASURE)
                        .setAggregate(Aggregate.AGGREGATE_SUM)
                        .setName("revenue").build())
                .put("test.Order", "id", FieldMetric.newBuilder()
                        .setRole(MemberRole.MEMBER_ROLE_MEASURE)
                        .setAggregate(Aggregate.AGGREGATE_COUNT)
                        .setName("orders")
                        .setFilterCel("this.paying == true").build())
                .put("test.Order", "ratio", FieldMetric.newBuilder()
                        .setRole(MemberRole.MEMBER_ROLE_MEASURE)
                        .setAggregate(Aggregate.AGGREGATE_AVG)
                        .setName("avg_ratio").build())
                .put("test.Order", "customer", FieldMetric.newBuilder()
                        .setRole(MemberRole.MEMBER_ROLE_MEASURE)
                        .setAggregate(Aggregate.AGGREGATE_COUNT_DISTINCT)
                        .setName("customers").build())
                .put("test.Order", "zone", FieldMetric.newBuilder()
                        .setRole(MemberRole.MEMBER_ROLE_MEASURE)
                        .setAggregate(Aggregate.AGGREGATE_COUNT_DISTINCT)
                        .setName("zones").build())
                // A synthetic member: paying_count with no phantom field.
                .putMessage("test.Order", ai.pipestream.proto.metric.MessageMetric
                        .newBuilder()
                        .addMembers(FieldMetric.newBuilder()
                                .setRole(MemberRole.MEMBER_ROLE_MEASURE)
                                .setAggregate(Aggregate.AGGREGATE_COUNT)
                                .setName("paying_count")
                                .setFilterCel("this.paying == true"))
                        .build());
        mapping = MetricMappings.build("orders", order, metrics);

        LuceneMetricExecutor executor = new LuceneMetricExecutor(
                new LuceneMetricExecutor.SubjectReader() {
                    @Override
                    public IndexMapping mapping(String subject) {
                        return indexMapping;
                    }

                    @Override
                    public MetricExecutor.Result read(
                            String subject, LuceneMetricExecutor.Aggregation aggregation) {
                        return store.withSearcher(subject, aggregation::run);
                    }
                });
        executors = Map.of(MetricBackend.METRIC_BACKEND_LUCENE, executor);
    }

    @AfterAll
    static void shutdown() {
        store.close();
    }

    static Descriptor orderDescriptor() throws Exception {
        DescriptorProto.Builder message = DescriptorProto.newBuilder().setName("Order");
        message.addField(scalar("id", 1, FieldDescriptorProto.Type.TYPE_STRING));
        message.addField(scalar("segment", 2, FieldDescriptorProto.Type.TYPE_STRING));
        message.addField(scalar("paying", 3, FieldDescriptorProto.Type.TYPE_BOOL));
        message.addField(scalar("amount", 4, FieldDescriptorProto.Type.TYPE_INT64));
        message.addField(FieldDescriptorProto.newBuilder()
                .setName("created_at").setNumber(5)
                .setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
                .setTypeName(".google.protobuf.Timestamp")
                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL));
        message.addField(scalar("title", 6, FieldDescriptorProto.Type.TYPE_STRING));
        message.addField(scalar("ratio", 7, FieldDescriptorProto.Type.TYPE_DOUBLE));
        message.addField(scalar("customer", 8, FieldDescriptorProto.Type.TYPE_STRING));
        message.addField(scalar("zone", 9, FieldDescriptorProto.Type.TYPE_INT64));
        FileDescriptorProto file = FileDescriptorProto.newBuilder()
                .setName("test/order.proto").setPackage("test").setSyntax("proto3")
                .addDependency("google/protobuf/timestamp.proto")
                .addMessageType(message)
                .build();
        return FileDescriptor.buildFrom(
                        file, new FileDescriptor[] {TimestampProto.getDescriptor()})
                .findMessageTypeByName("Order");
    }

    static FieldDescriptorProto.Builder scalar(
            String name, int number, FieldDescriptorProto.Type type) {
        return FieldDescriptorProto.newBuilder()
                .setName(name).setNumber(number).setType(type)
                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL);
    }

    static void index(String id, String segment, boolean paying, long amount,
            double ratio, String createdAt, String title, String customer, long zone) {
        Instant instant = Instant.parse(createdAt);
        store.index("orders", DynamicMessage.newBuilder(order)
                .setField(order.findFieldByName("id"), id)
                .setField(order.findFieldByName("segment"), segment)
                .setField(order.findFieldByName("paying"), paying)
                .setField(order.findFieldByName("amount"), amount)
                .setField(order.findFieldByName("ratio"), ratio)
                .setField(order.findFieldByName("created_at"), Timestamp.newBuilder()
                        .setSeconds(instant.getEpochSecond()).build())
                .setField(order.findFieldByName("title"), title)
                .setField(order.findFieldByName("customer"), customer)
                .setField(order.findFieldByName("zone"), zone)
                .build());
    }

    static QueryMetricsRequest.Builder request(String... measures) {
        QueryMetricsRequest.Builder builder = QueryMetricsRequest.newBuilder()
                .setMappingSubject("orders").setLimit(100);
        for (String measure : measures) {
            builder.addMeasures(measure);
        }
        return builder;
    }

    static Map<String, MetricRow> bySegment(QueryMetricsResponse response) {
        return response.getRowsList().stream()
                .collect(java.util.stream.Collectors.toMap(
                        row -> row.getDimensionsOrThrow("segment"), row -> row));
    }

    // ------------------------------------------------------------- the numbers

    @Test
    void sumCountAndAFilteredMeasureGroupBySegment() {
        QueryMetricsResponse response = MetricQueries.query(mapping, executors,
                request("revenue", "orders")
                        .addDimensions(MemberRef.newBuilder().setName("segment"))
                        .build());

        // Hand-checked: smb = 100 + 50 + 30, mid = 200; paying orders
        // smb = o-1 and o-4, mid = o-3.
        Map<String, MetricRow> rows = bySegment(response);
        assertThat(rows).containsOnlyKeys("smb", "mid");
        assertThat(rows.get("smb").getMeasuresOrThrow("revenue")).isEqualTo(180.0);
        assertThat(rows.get("smb").getMeasuresOrThrow("orders")).isEqualTo(2.0);
        assertThat(rows.get("mid").getMeasuresOrThrow("revenue")).isEqualTo(200.0);
        assertThat(rows.get("mid").getMeasuresOrThrow("orders")).isEqualTo(1.0);
        assertThat(response.getPhysicalPlan())
                .contains("group-by=[segment]")
                .contains("paying in [true]");
    }

    @Test
    void dateDimensionsBucketUnderTheResolvedGrain() {
        QueryMetricsResponse byMonth = MetricQueries.query(mapping, executors,
                request("revenue")
                        .addDimensions(MemberRef.newBuilder().setName("created_at"))
                        .build());
        // The declared default grain is MONTH: July = 100 + 50, August = 200 + 30.
        assertThat(byMonth.getRowsList()).hasSize(2);
        assertThat(byMonth.getRows(0).getDimensionsOrThrow("created_at")).isEqualTo("2026-07");
        assertThat(byMonth.getRows(0).getMeasuresOrThrow("revenue")).isEqualTo(150.0);
        assertThat(byMonth.getRows(1).getDimensionsOrThrow("created_at")).isEqualTo("2026-08");
        assertThat(byMonth.getRows(1).getMeasuresOrThrow("revenue")).isEqualTo(230.0);

        QueryMetricsResponse byYear = MetricQueries.query(mapping, executors,
                request("revenue")
                        .addDimensions(MemberRef.newBuilder().setName("created_at")
                                .setGrain(TimeGrain.TIME_GRAIN_YEAR))
                        .build());
        assertThat(byYear.getRowsList()).hasSize(1);
        assertThat(byYear.getRows(0).getDimensionsOrThrow("created_at")).isEqualTo("2026");
        assertThat(byYear.getRows(0).getMeasuresOrThrow("revenue")).isEqualTo(380.0);
    }

    @Test
    void queryWideFiltersGateEveryMeasure() {
        QueryMetricsResponse response = MetricQueries.query(mapping, executors,
                request("revenue")
                        .addFilters(MetricFilter.newBuilder()
                                .setMember("segment").addEquals("smb"))
                        .build());
        assertThat(response.getRowsList()).hasSize(1);
        assertThat(response.getRows(0).getMeasuresOrThrow("revenue")).isEqualTo(180.0);
    }

    @Test
    void anUngroupedQueryAnswersOneRowOverEverything() {
        QueryMetricsResponse response = MetricQueries.query(mapping, executors,
                request("revenue", "orders").build());
        assertThat(response.getRowsList()).hasSize(1);
        assertThat(response.getRows(0).getMeasuresOrThrow("revenue")).isEqualTo(380.0);
        assertThat(response.getRows(0).getMeasuresOrThrow("orders")).isEqualTo(3.0);
    }

    @Test
    void sortableEncodedDoublesDecodeBeforeAggregating() {
        // The mapper writes facetable DOUBLE doc values sortable-encoded;
        // an executor that forgets to decode answers astronomically wrong
        // sums, so the exact averages pin the decode.
        QueryMetricsResponse response = MetricQueries.query(mapping, executors,
                request("avg_ratio").build());
        assertThat(response.getRows(0).getMeasuresOrThrow("avg_ratio")).isEqualTo(0.5);

        QueryMetricsResponse bySegment = MetricQueries.query(mapping, executors,
                request("avg_ratio")
                        .addDimensions(MemberRef.newBuilder().setName("segment"))
                        .build());
        assertThat(bySegment(bySegment).get("mid").getMeasuresOrThrow("avg_ratio"))
                .isEqualTo(0.75);
    }

    @Test
    void countDistinctOverKeywordTermsCountsExactly() {
        // Hand-checked customers: cust-a (o-1, o-3), cust-b, cust-c = 3.
        QueryMetricsResponse response = MetricQueries.query(mapping, executors,
                request("customers").build());
        assertThat(response.getRows(0).getMeasuresOrThrow("customers")).isEqualTo(3.0);
        assertThat(response.getPhysicalPlan()).contains("count_distinct exact, bound=");

        // Grouped: smb sees cust-a, cust-b, cust-c; mid sees cust-a alone.
        QueryMetricsResponse bySegment = MetricQueries.query(mapping, executors,
                request("customers")
                        .addDimensions(MemberRef.newBuilder().setName("segment"))
                        .build());
        Map<String, MetricRow> rows = bySegment(bySegment);
        assertThat(rows.get("smb").getMeasuresOrThrow("customers")).isEqualTo(3.0);
        assertThat(rows.get("mid").getMeasuresOrThrow("customers")).isEqualTo(1.0);
    }

    @Test
    void countDistinctOverNumericValuesCountsExactly() {
        // Hand-checked zones: 10 (o-1, o-3, o-4) and 20 = 2.
        QueryMetricsResponse response = MetricQueries.query(mapping, executors,
                request("zones").build());
        assertThat(response.getRows(0).getMeasuresOrThrow("zones")).isEqualTo(2.0);
    }

    @Test
    void passingTheDistinctBoundRefusesByNameInsteadOfEstimating() {
        LuceneMetricExecutor bounded = new LuceneMetricExecutor(
                new LuceneMetricExecutor.SubjectReader() {
                    @Override
                    public IndexMapping mapping(String subject) {
                        return storeMapping;
                    }

                    @Override
                    public MetricExecutor.Result read(
                            String subject, LuceneMetricExecutor.Aggregation aggregation) {
                        return store.withSearcher(subject, aggregation::run);
                    }
                }, 2);
        assertThatThrownBy(() -> MetricQueries.query(mapping,
                Map.of(MetricBackend.METRIC_BACKEND_LUCENE, bounded),
                request("customers").build()))
                .isInstanceOfSatisfying(MetricRefusal.class, refusal -> {
                    assertThat(refusal.code()).isEqualTo(MetricRefusal.DISTINCT_BOUND);
                    assertThat(refusal.getMessage())
                            .contains("customers")
                            .contains("bound of 2")
                            .contains("Iceberg");
                });
    }

    @Test
    void aSyntheticFilteredCountAnswersWithoutABackingField() {
        // Hand-checked paying orders: o-1, o-3, o-4.
        QueryMetricsResponse total = MetricQueries.query(mapping, executors,
                request("paying_count").build());
        assertThat(total.getRows(0).getMeasuresOrThrow("paying_count")).isEqualTo(3.0);

        QueryMetricsResponse bySegment = MetricQueries.query(mapping, executors,
                request("paying_count")
                        .addDimensions(MemberRef.newBuilder().setName("segment"))
                        .build());
        Map<String, MetricRow> rows = bySegment(bySegment);
        assertThat(rows.get("smb").getMeasuresOrThrow("paying_count")).isEqualTo(2.0);
        assertThat(rows.get("mid").getMeasuresOrThrow("paying_count")).isEqualTo(1.0);
    }

    @Test
    void aFieldWithoutTheNeededDocValuesFailsLoudlyNamingTheHint() {
        assertThatThrownBy(() -> MetricQueries.query(mapping, executors,
                request("revenue")
                        .addDimensions(MemberRef.newBuilder().setName("title"))
                        .build()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("title")
                .hasMessageContaining("declare facetable or sortable");
    }
}
