package ai.protomolt.proto.metric.lucene;

import ai.protomolt.proto.search.index.spi.CatalogIndexingHintSource;
import ai.protomolt.proto.search.index.spi.IndexFieldKind;
import ai.protomolt.proto.search.index.spi.IndexMapping;
import ai.protomolt.proto.search.index.spi.IndexMappingFactory;
import ai.protomolt.proto.search.index.spi.ResolvedFieldHint;
import ai.protomolt.proto.metric.Aggregate;
import ai.protomolt.proto.metric.FieldMetric;
import ai.protomolt.proto.metric.MemberRef;
import ai.protomolt.proto.metric.MemberRole;
import ai.protomolt.proto.metric.MetricBackend;
import ai.protomolt.proto.metric.MetricFilter;
import ai.protomolt.proto.metric.MetricRow;
import ai.protomolt.proto.metric.QueryMetricsRequest;
import ai.protomolt.proto.metric.QueryMetricsResponse;
import ai.protomolt.proto.metric.spi.CatalogMetricHintSource;
import ai.protomolt.proto.metric.spi.MetricExecutor;
import ai.protomolt.proto.metric.spi.MetricMapping;
import ai.protomolt.proto.metric.spi.MetricMappings;
import ai.protomolt.proto.metric.spi.MetricQueries;
import ai.protomolt.proto.search.service.LuceneSearchStore;
import ai.protomolt.proto.search.service.ServedMapping;
import ai.protomolt.proto.types.TreePath;
import ai.protomolt.proto.types.TreePathProto;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * TreePath on the Lucene metric backend, end to end through the service
 * store: a TREE_PATH dimension buckets by the whole leaf path even though
 * the doc values hold the full ancestor chain, and a prefix filter is one
 * exact term match selecting the subtree. The corpus carries a pathless
 * product (the non-uniform case): it must fall out of both the group-by
 * and every prefix.
 */
class TreePathMetricsLuceneTest {

    @TempDir
    static Path work;

    static Descriptor product;
    static LuceneSearchStore store;
    static MetricMapping mapping;
    static Map<MetricBackend, MetricExecutor> executors;

    @BeforeAll
    static void boot() throws Exception {
        product = productDescriptor();

        CatalogIndexingHintSource hints = new CatalogIndexingHintSource();
        hints.put("test.Product", "id",
                ResolvedFieldHint.builder(IndexFieldKind.KEYWORD).stored(true).build());
        hints.put("test.Product", "category",
                ResolvedFieldHint.builder(IndexFieldKind.TREE_PATH).facetable(true).build());
        hints.put("test.Product", "amount",
                ResolvedFieldHint.builder(IndexFieldKind.INT64).sortable(true).build());
        IndexMapping indexMapping = IndexMappingFactory.defaults(hints).create(product);
        ServedMapping served = new ServedMapping(indexMapping, "id",
                message -> (String) message.getField(product.findFieldByName("id")), null);
        store = new LuceneSearchStore(work, Map.of("products", served));

        index("p-1", 100, "electronics", "computers", "laptops");
        index("p-2", 50, "electronics", "computers");
        index("p-3", 200, "electronics", "audio");
        index("p-4", 30, "media", "books");
        index("p-5", 999);
        // A single-segment path that merely STARTS with "electronics": it
        // must never land in the "electronics" subtree.
        index("p-6", 7, "electronicsx");

        CatalogMetricHintSource metrics = new CatalogMetricHintSource()
                .put("test.Product", "category", FieldMetric.newBuilder()
                        .setRole(MemberRole.MEMBER_ROLE_DIMENSION).build())
                .put("test.Product", "amount", FieldMetric.newBuilder()
                        .setRole(MemberRole.MEMBER_ROLE_MEASURE)
                        .setAggregate(Aggregate.AGGREGATE_SUM)
                        .setName("revenue").build());
        mapping = MetricMappings.build("products", product, metrics);

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

    static Descriptor productDescriptor() throws Exception {
        FileDescriptorProto file = FileDescriptorProto.newBuilder()
                .setName("test/product.proto").setPackage("test").setSyntax("proto3")
                .addDependency("ai/pipestream/proto/types/v1/tree_path.proto")
                .addMessageType(DescriptorProto.newBuilder().setName("Product")
                        .addField(FieldDescriptorProto.newBuilder()
                                .setName("id").setNumber(1)
                                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL))
                        .addField(FieldDescriptorProto.newBuilder()
                                .setName("category").setNumber(2)
                                .setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
                                .setTypeName(".ai.pipestream.proto.types.v1.TreePath")
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL))
                        .addField(FieldDescriptorProto.newBuilder()
                                .setName("amount").setNumber(3)
                                .setType(FieldDescriptorProto.Type.TYPE_INT64)
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)))
                .build();
        return FileDescriptor.buildFrom(
                        file, new FileDescriptor[] {TreePathProto.getDescriptor()})
                .findMessageTypeByName("Product");
    }

    static void index(String id, long amount, String... segments) {
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(product)
                .setField(product.findFieldByName("id"), id)
                .setField(product.findFieldByName("amount"), amount);
        if (segments.length > 0) {
            TreePath.Builder path = TreePath.newBuilder();
            for (String segment : segments) {
                path.addSegments(segment);
            }
            builder.setField(product.findFieldByName("category"), path.build());
        }
        store.index("products", builder.build());
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
    void treePathDimensionsBucketByTheLeafPathNotPerAncestor() {
        QueryMetricsResponse response = MetricQueries.query(mapping, executors,
                request()
                        .addDimensions(MemberRef.newBuilder().setName("category"))
                        .build());
        // One bucket per product path, labeled by the whole leaf path; a
        // per-ancestor bucketing would have produced an "electronics" row
        // counting three products, and the pathless p-5 must be excluded.
        assertThat(revenueByCategory(response)).containsOnly(
                Map.entry("electronics/computers/laptops", 100.0),
                Map.entry("electronics/computers", 50.0),
                Map.entry("electronics/audio", 200.0),
                Map.entry("media/books", 30.0),
                Map.entry("electronicsx", 7.0));
    }

    @Test
    void aPrefixFilterSelectsTheSubtreeAsOneTermMatch() {
        QueryMetricsResponse response = MetricQueries.query(mapping, executors,
                request()
                        .addFilters(MetricFilter.newBuilder()
                                .setMember("category")
                                .setPrefix(TreePath.newBuilder()
                                        .addSegments("electronics")
                                        .addSegments("computers")))
                        .build());
        // Hand-checked: p-1 (100, descendant) + p-2 (50, the path itself).
        assertThat(response.getRowsList()).singleElement().satisfies(row ->
                assertThat(row.getMeasuresOrThrow("revenue")).isEqualTo(150.0));
        assertThat(response.getPhysicalPlan()).contains("category:electronics/computers");
    }

    @Test
    void aPrefixComposesWithTheTreePathGroupBy() {
        QueryMetricsResponse response = MetricQueries.query(mapping, executors,
                request()
                        .addDimensions(MemberRef.newBuilder().setName("category"))
                        .addFilters(MetricFilter.newBuilder()
                                .setMember("category")
                                .setPrefix(TreePath.newBuilder().addSegments("electronics")))
                        .build());
        // "electronicsx" merely starts with the prefix string; it is not in
        // the subtree, and neither is the pathless p-5.
        assertThat(revenueByCategory(response)).containsOnly(
                Map.entry("electronics/computers/laptops", 100.0),
                Map.entry("electronics/computers", 50.0),
                Map.entry("electronics/audio", 200.0));
    }
}
