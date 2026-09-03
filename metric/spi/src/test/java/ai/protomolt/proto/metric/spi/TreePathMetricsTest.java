package ai.protomolt.proto.metric.spi;

import ai.protomolt.proto.metric.Aggregate;
import ai.protomolt.proto.metric.FieldMetric;
import ai.protomolt.proto.metric.MemberRef;
import ai.protomolt.proto.metric.MetricFilter;
import ai.protomolt.proto.metric.MetricProto;
import ai.protomolt.proto.metric.QueryMetricsRequest;
import ai.protomolt.proto.metric.spi.CompiledMetricQuery.DimensionKind;
import ai.protomolt.proto.metric.spi.MetricMapping.FieldKind;
import ai.protomolt.proto.types.TreePath;
import ai.protomolt.proto.types.TreePathProto;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldOptions;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The metric surface speaks TreePath: a canonical TreePath field is a
 * TREE_PATH dimension (a leaf of the walk, like Timestamp), the prefix
 * filter form compiles to the rendered root-first path, and every wrong
 * combination refuses by name — equality on a tree path most of all,
 * because a term match against the ancestor chain would silently mean
 * descendant-or-self.
 */
class TreePathMetricsTest {

    static FieldDescriptorProto.Builder treePath(String name, int number, FieldMetric metric) {
        FieldDescriptorProto.Builder field = FieldDescriptorProto.newBuilder()
                .setName(name).setNumber(number)
                .setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
                .setTypeName(".ai.pipestream.proto.types.v1.TreePath")
                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL);
        if (metric != null) {
            field.setOptions(FieldOptions.newBuilder()
                    .setExtension(MetricProto.metric, metric));
        }
        return field;
    }

    static Descriptor build(DescriptorProto message) throws Exception {
        FileDescriptorProto file = FileDescriptorProto.newBuilder()
                .setName("test/products.proto").setPackage("test").setSyntax("proto3")
                .addDependency("ai/pipestream/proto/types/v1/tree_path.proto")
                .addMessageType(message)
                .build();
        return FileDescriptor.buildFrom(
                        file, new FileDescriptor[] {TreePathProto.getDescriptor()})
                .findMessageTypeByName(message.getName());
    }

    static Descriptor products() throws Exception {
        return build(DescriptorProto.newBuilder().setName("Product")
                .addField(MetricMappingsTest.scalar(
                        "id", 1, FieldDescriptorProto.Type.TYPE_STRING))
                .addField(treePath("category", 2, MetricMappingsTest.dimension()))
                .addField(MetricMappingsTest.declared(
                        "segment", 3, FieldDescriptorProto.Type.TYPE_STRING,
                        MetricMappingsTest.dimension()))
                .addField(MetricMappingsTest.declared(
                        "amount", 4, FieldDescriptorProto.Type.TYPE_INT64,
                        MetricMappingsTest.measure(Aggregate.AGGREGATE_SUM)))
                .build());
    }

    static MetricMapping mapping() throws Exception {
        return MetricMappings.build("products", products(), MetricMappingsTest.OPTIONS);
    }

    static QueryMetricsRequest.Builder request() {
        return QueryMetricsRequest.newBuilder()
                .setMappingSubject("products").setLimit(10).addMeasures("amount");
    }

    static TreePath path(String... segments) {
        TreePath.Builder builder = TreePath.newBuilder();
        for (String segment : segments) {
            builder.addSegments(segment);
        }
        return builder.build();
    }

    static CompiledMetricQuery compiled(QueryMetricsRequest built) throws Exception {
        MetricQueriesTest.FakeExecutor executor = MetricQueriesTest.FakeExecutor.lucene(
                List.of());
        MetricQueries.query(mapping(), Map.of(executor.backend, executor), built);
        return executor.executed;
    }

    static MetricRefusal refusal(QueryMetricsRequest built) throws Exception {
        MetricQueriesTest.FakeExecutor executor = MetricQueriesTest.FakeExecutor.lucene(
                List.of());
        MetricMapping mapping = mapping();
        return MetricQueriesTest.refusalOf(() ->
                MetricQueries.query(mapping, Map.of(executor.backend, executor), built));
    }

    // ------------------------------------------------------------- the mapping

    @Test
    void aTreePathFieldBuildsAsATreePathDimension() throws Exception {
        MetricMapping mapping = mapping();
        MetricMapping.MetricMember category = mapping.member("category").orElseThrow();
        assertThat(category.kind()).isEqualTo(FieldKind.TREE_PATH);
        assertThat(category.fieldName()).isEqualTo("category");
    }

    @Test
    void aTreePathMeasureIsRefused() throws Exception {
        Descriptor sums = build(DescriptorProto.newBuilder().setName("Sums")
                .addField(treePath("category", 1,
                        MetricMappingsTest.measure(Aggregate.AGGREGATE_SUM)))
                .build());
        assertThatThrownBy(() ->
                MetricMappings.build("sums", sums, MetricMappingsTest.OPTIONS))
                .isInstanceOf(MetricSchemaException.class)
                .hasMessageContaining("needs a numeric field");
    }

    @Test
    void anUndeclaredTreePathFieldStaysALeafOfTheWalk() throws Exception {
        Descriptor quiet = build(DescriptorProto.newBuilder().setName("Quiet")
                .addField(treePath("category", 1, null))
                .addField(MetricMappingsTest.declared(
                        "amount", 2, FieldDescriptorProto.Type.TYPE_INT64,
                        MetricMappingsTest.measure(Aggregate.AGGREGATE_SUM)))
                .build());
        MetricMapping mapping = MetricMappings.build(
                "quiet", quiet, MetricMappingsTest.OPTIONS);
        assertThat(mapping.memberNames()).containsExactly("amount");
    }

    // ------------------------------------------------------------- the compile

    @Test
    void aTreePathDimensionCompilesToTreePathKind() throws Exception {
        CompiledMetricQuery query = compiled(request()
                .addDimensions(MemberRef.newBuilder().setName("category"))
                .build());
        assertThat(query.dimensions()).singleElement().satisfies(dimension ->
                assertThat(dimension.kind()).isEqualTo(DimensionKind.TREE_PATH));
    }

    @Test
    void aPrefixFilterCompilesToTheRenderedPath() throws Exception {
        CompiledMetricQuery query = compiled(request()
                .addFilters(MetricFilter.newBuilder()
                        .setMember("category")
                        .setPrefix(path("electronics", "computers")))
                .build());
        assertThat(query.pathPrefixes()).containsExactly(
                new CompiledMetricQuery.PathPrefixFilter(
                        "category", "category", "category", "electronics/computers"));
        assertThat(query.filters()).isEmpty();
    }

    // ------------------------------------------------------------- refusals

    @Test
    void aPrefixOnAKeywordDimensionIsRefused() throws Exception {
        assertThat(refusal(request()
                .addFilters(MetricFilter.newBuilder()
                        .setMember("segment")
                        .setPrefix(path("electronics")))
                .build()).getMessage())
                .contains("needs a TREE_PATH dimension");
    }

    @Test
    void equalityOnATreePathDimensionIsRefusedTowardPrefix() throws Exception {
        assertThat(refusal(request()
                .addFilters(MetricFilter.newBuilder()
                        .setMember("category")
                        .addEquals("electronics/computers"))
                .build()).getMessage())
                .contains("filters by prefix");
    }

    @Test
    void combinedFormsAreRefused() throws Exception {
        assertThat(refusal(request()
                .addFilters(MetricFilter.newBuilder()
                        .setMember("category")
                        .addEquals("electronics")
                        .setPrefix(path("electronics")))
                .build()).getMessage())
                .contains("more than one form");
    }

    @Test
    void anEmptyPrefixIsRefused() throws Exception {
        assertThat(refusal(request()
                .addFilters(MetricFilter.newBuilder()
                        .setMember("category")
                        .setPrefix(TreePath.getDefaultInstance()))
                .build()).getMessage())
                .contains("no segments");
    }

    @Test
    void aDelimiterInAPrefixSegmentIsRefused() throws Exception {
        assertThat(refusal(request()
                .addFilters(MetricFilter.newBuilder()
                        .setMember("category")
                        .setPrefix(path("electronics/computers")))
                .build()).getMessage())
                .contains("free of the \"/\" delimiter");
    }
}
