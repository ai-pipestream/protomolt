package ai.protomolt.proto.metric.spi;

import ai.protomolt.proto.metric.Aggregate;
import ai.protomolt.proto.metric.FieldMetric;
import ai.protomolt.proto.metric.MemberRole;
import ai.protomolt.proto.metric.MessageMetric;
import ai.protomolt.proto.metric.MetricProto;
import ai.protomolt.proto.metric.TimeGrain;
import ai.protomolt.proto.metric.spi.CompiledMetricQuery.DimensionKind;
import ai.protomolt.proto.metric.spi.CompiledMetricQuery.EqualsFilter;
import ai.protomolt.proto.metric.spi.MetricMapping.FieldKind;
import ai.protomolt.proto.metric.spi.MetricMapping.MetricMember;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldOptions;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.TimestampProto;
import java.util.List;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The mapping build from metric.v1 declarations: the worked example builds
 * with translated filters and recorded calculated inputs, every schema
 * error fails the build naming the field path, and options that survived a
 * registry-less link as unknown fields still count.
 */
class MetricMappingsTest {

    static final MetricHintSource OPTIONS = new ProtoOptionsMetricHintSource();

    // ------------------------------------------------------------- fixtures

    /** A field with no metric declaration. */
    static FieldDescriptorProto.Builder scalar(
            String name, int number, FieldDescriptorProto.Type type) {
        return FieldDescriptorProto.newBuilder()
                .setName(name).setNumber(number).setType(type)
                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL);
    }

    static FieldDescriptorProto.Builder declared(
            String name, int number, FieldDescriptorProto.Type type, FieldMetric metric) {
        return scalar(name, number, type)
                .setOptions(FieldOptions.newBuilder()
                        .setExtension(MetricProto.metric, metric));
    }

    static FieldDescriptorProto.Builder timestamp(String name, int number, FieldMetric metric) {
        FieldDescriptorProto.Builder field = FieldDescriptorProto.newBuilder()
                .setName(name).setNumber(number)
                .setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
                .setTypeName(".google.protobuf.Timestamp")
                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL);
        if (metric != null) {
            field.setOptions(FieldOptions.newBuilder()
                    .setExtension(MetricProto.metric, metric));
        }
        return field;
    }

    static Descriptor build(DescriptorProto message) throws Exception {
        FileDescriptorProto file = FileDescriptorProto.newBuilder()
                .setName("test/orders.proto").setPackage("test").setSyntax("proto3")
                .addDependency("google/protobuf/timestamp.proto")
                .addMessageType(message)
                .build();
        return FileDescriptor.buildFrom(
                        file, new FileDescriptor[] {TimestampProto.getDescriptor()})
                .findMessageTypeByName(message.getName());
    }

    static FieldMetric dimension() {
        return FieldMetric.newBuilder().setRole(MemberRole.MEMBER_ROLE_DIMENSION).build();
    }

    static FieldMetric measure(Aggregate aggregate) {
        return FieldMetric.newBuilder()
                .setRole(MemberRole.MEMBER_ROLE_MEASURE).setAggregate(aggregate).build();
    }

    /** The design doc's worked example, plus a filtered and a calculated measure. */
    static Descriptor orders() throws Exception {
        return build(DescriptorProto.newBuilder().setName("Order")
                .setOptions(com.google.protobuf.DescriptorProtos.MessageOptions.newBuilder()
                        .setExtension(MetricProto.metricMessage, MessageMetric.newBuilder()
                                .setSubject("orders").setIdentityField("id").build()))
                .addField(scalar("id", 1, FieldDescriptorProto.Type.TYPE_STRING))
                .addField(declared("segment", 2, FieldDescriptorProto.Type.TYPE_STRING,
                        dimension()))
                .addField(timestamp("created_at", 3, FieldMetric.newBuilder()
                        .setRole(MemberRole.MEMBER_ROLE_DIMENSION)
                        .setDefaultGrain(TimeGrain.TIME_GRAIN_MONTH).build()))
                .addField(declared("amount_cents", 4, FieldDescriptorProto.Type.TYPE_INT64,
                        FieldMetric.newBuilder()
                                .setRole(MemberRole.MEMBER_ROLE_MEASURE)
                                .setAggregate(Aggregate.AGGREGATE_SUM)
                                .setName("revenue").build()))
                .addField(scalar("paying", 5, FieldDescriptorProto.Type.TYPE_BOOL))
                .addField(declared("order_id", 6, FieldDescriptorProto.Type.TYPE_STRING,
                        FieldMetric.newBuilder()
                                .setRole(MemberRole.MEMBER_ROLE_MEASURE)
                                .setAggregate(Aggregate.AGGREGATE_COUNT)
                                .setName("orders")
                                .setFilterCel("this.paying == true && this.segment == 'smb'")
                                .build()))
                .addField(declared("ratio_seed", 7, FieldDescriptorProto.Type.TYPE_INT64,
                        FieldMetric.newBuilder()
                                .setRole(MemberRole.MEMBER_ROLE_MEASURE)
                                .setName("average_order")
                                .setCel("revenue / orders").build()))
                .build());
    }

    // ------------------------------------------------------------- the build

    @Test
    void theWorkedExampleBuildsWithTranslatedFiltersAndCalculatedInputs() throws Exception {
        MetricMapping mapping = MetricMappings.build("", orders(), OPTIONS);

        assertThat(mapping.subject()).isEqualTo("orders");
        assertThat(mapping.messageType()).isEqualTo("test.Order");
        assertThat(mapping.memberNames()).containsExactly(
                "segment", "created_at", "revenue", "orders", "average_order");

        MetricMember segment = mapping.member("segment").orElseThrow();
        assertThat(segment.role()).isEqualTo(MemberRole.MEMBER_ROLE_DIMENSION);
        assertThat(segment.kind()).isEqualTo(FieldKind.KEYWORD);

        MetricMember createdAt = mapping.member("created_at").orElseThrow();
        assertThat(createdAt.kind()).isEqualTo(FieldKind.DATE);
        assertThat(createdAt.defaultGrain()).isEqualTo(TimeGrain.TIME_GRAIN_MONTH);

        MetricMember revenue = mapping.member("revenue").orElseThrow();
        assertThat(revenue.aggregate()).isEqualTo(Aggregate.AGGREGATE_SUM);
        assertThat(revenue.fieldName()).isEqualTo("amount_cents");

        // The filter_cel translated to the engine-neutral equality form.
        MetricMember orders = mapping.member("orders").orElseThrow();
        assertThat(orders.rowFilters()).containsExactly(
                new EqualsFilter("orders", "paying", "paying",
                        DimensionKind.BOOLEAN, List.of("true")),
                new EqualsFilter("orders", "segment", "segment",
                        DimensionKind.TERM, List.of("smb")));

        // The calculated measure recorded what it reads.
        MetricMember average = mapping.member("average_order").orElseThrow();
        assertThat(average.calculated()).isTrue();
        assertThat(average.kind()).isEqualTo(FieldKind.SYNTHETIC);
        assertThat(average.celRequires()).containsExactly("revenue", "orders");
    }

    @Test
    void aHostSubjectOverridesTheDeclaredDefault() throws Exception {
        assertThat(MetricMappings.build("orders-eu", orders(), OPTIONS).subject())
                .isEqualTo("orders-eu");
    }

    @Test
    void optionsSurvivingARegistrylessLinkAsUnknownFieldsStillCount() throws Exception {
        // Reparse the file proto without the extension registry: the metric
        // options become unknown fields, exactly what a descriptor set
        // produced by a foreign toolchain looks like.
        FileDescriptorProto original = orders().getFile().toProto();
        FileDescriptorProto stripped = FileDescriptorProto.parseFrom(original.toByteArray());
        Descriptor relinked = FileDescriptor.buildFrom(
                        stripped, new FileDescriptor[] {TimestampProto.getDescriptor()})
                .findMessageTypeByName("Order");
        assertThat(relinked.getFields().get(1).getOptions()
                .hasExtension(MetricProto.metric)).isFalse();

        MetricMapping mapping = MetricMappings.build("", relinked, OPTIONS);
        assertThat(mapping.memberNames()).containsExactly(
                "segment", "created_at", "revenue", "orders", "average_order");
    }

    @Test
    void theCatalogSourceDeclaresWhatTheSchemaCannot() throws Exception {
        Descriptor bare = build(DescriptorProto.newBuilder().setName("Bare")
                .addField(scalar("region", 1, FieldDescriptorProto.Type.TYPE_STRING))
                .addField(scalar("total", 2, FieldDescriptorProto.Type.TYPE_INT64))
                .build());
        CatalogMetricHintSource catalog = new CatalogMetricHintSource()
                .put("test.Bare", "region", dimension())
                .put("test.Bare", "total", measure(Aggregate.AGGREGATE_SUM))
                .putMessage("test.Bare", MessageMetric.newBuilder().setSubject("bare").build());

        MetricMapping mapping = MetricMappings.build("", bare, catalog);
        assertThat(mapping.subject()).isEqualTo("bare");
        assertThat(mapping.memberNames()).containsExactly("region", "total");
    }

    // ------------------------------------------------------------- schema errors

    @Test
    void everyViolationReportsAtOnceNamingItsFieldPath() throws Exception {
        Descriptor broken = build(DescriptorProto.newBuilder().setName("Broken")
                // role unset on a present option
                .addField(declared("a", 1, FieldDescriptorProto.Type.TYPE_STRING,
                        FieldMetric.getDefaultInstance()))
                // dimension with an aggregate
                .addField(declared("b", 2, FieldDescriptorProto.Type.TYPE_STRING,
                        FieldMetric.newBuilder()
                                .setRole(MemberRole.MEMBER_ROLE_DIMENSION)
                                .setAggregate(Aggregate.AGGREGATE_SUM).build()))
                // measure with neither aggregate nor cel
                .addField(declared("c", 3, FieldDescriptorProto.Type.TYPE_INT64,
                        FieldMetric.newBuilder()
                                .setRole(MemberRole.MEMBER_ROLE_MEASURE).build()))
                // default_grain on a non-DATE field
                .addField(declared("d", 4, FieldDescriptorProto.Type.TYPE_STRING,
                        FieldMetric.newBuilder()
                                .setRole(MemberRole.MEMBER_ROLE_DIMENSION)
                                .setDefaultGrain(TimeGrain.TIME_GRAIN_DAY).build()))
                // numeric dimension
                .addField(declared("e", 5, FieldDescriptorProto.Type.TYPE_INT64,
                        dimension()))
                // SUM on a string
                .addField(declared("f", 6, FieldDescriptorProto.Type.TYPE_STRING,
                        measure(Aggregate.AGGREGATE_SUM)))
                .build());

        assertThatThrownBy(() -> MetricMappings.build("broken", broken, OPTIONS))
                .isInstanceOfSatisfying(MetricSchemaException.class, e ->
                        assertThat(e.violations())
                                .hasSize(6)
                                .anySatisfy(v -> assertThat(v)
                                        .contains("test.Broken.a").contains("no role"))
                                .anySatisfy(v -> assertThat(v)
                                        .contains("test.Broken.b").contains("aggregate"))
                                .anySatisfy(v -> assertThat(v)
                                        .contains("test.Broken.c")
                                        .contains("neither an aggregate nor a cel"))
                                .anySatisfy(v -> assertThat(v)
                                        .contains("test.Broken.d").contains("default_grain"))
                                .anySatisfy(v -> assertThat(v)
                                        .contains("test.Broken.e").contains("numeric"))
                                .anySatisfy(v -> assertThat(v)
                                        .contains("test.Broken.f").contains("numeric field")));
    }

    @Test
    void calculatedMeasuresRejectAggregatesSiblingsAndNonNumericResults() throws Exception {
        Descriptor broken = build(DescriptorProto.newBuilder().setName("Calc")
                .addField(declared("total", 1, FieldDescriptorProto.Type.TYPE_INT64,
                        measure(Aggregate.AGGREGATE_SUM)))
                // cel plus aggregate
                .addField(declared("g", 2, FieldDescriptorProto.Type.TYPE_INT64,
                        FieldMetric.newBuilder()
                                .setRole(MemberRole.MEMBER_ROLE_MEASURE)
                                .setAggregate(Aggregate.AGGREGATE_SUM)
                                .setCel("total * 2.0").build()))
                // cel over an unknown sibling
                .addField(declared("h", 3, FieldDescriptorProto.Type.TYPE_INT64,
                        FieldMetric.newBuilder()
                                .setRole(MemberRole.MEMBER_ROLE_MEASURE)
                                .setCel("nope + 1.0").build()))
                // cel that is not numeric
                .addField(declared("i", 4, FieldDescriptorProto.Type.TYPE_INT64,
                        FieldMetric.newBuilder()
                                .setRole(MemberRole.MEMBER_ROLE_MEASURE)
                                .setCel("'text'").build()))
                .build());

        assertThatThrownBy(() -> MetricMappings.build("calc", broken, OPTIONS))
                .isInstanceOfSatisfying(MetricSchemaException.class, e ->
                        assertThat(e.violations())
                                .anySatisfy(v -> assertThat(v)
                                        .contains("test.Calc.g")
                                        .contains("aggregate or filter_cel"))
                                .anySatisfy(v -> assertThat(v)
                                        .contains("'h'").contains("type-check"))
                                .anySatisfy(v -> assertThat(v)
                                        .contains("'i'").contains("numeric")));
    }

    @Test
    void filterCelMustBeBoolAndInsideTheTranslatableSubset() throws Exception {
        Descriptor broken = build(DescriptorProto.newBuilder().setName("Filters")
                .addField(scalar("amount", 1, FieldDescriptorProto.Type.TYPE_INT64))
                // not bool
                .addField(declared("j", 2, FieldDescriptorProto.Type.TYPE_INT64,
                        FieldMetric.newBuilder()
                                .setRole(MemberRole.MEMBER_ROLE_MEASURE)
                                .setAggregate(Aggregate.AGGREGATE_COUNT)
                                .setFilterCel("this.amount").build()))
                // bool but beyond the equality subset
                .addField(declared("k", 3, FieldDescriptorProto.Type.TYPE_INT64,
                        FieldMetric.newBuilder()
                                .setRole(MemberRole.MEMBER_ROLE_MEASURE)
                                .setAggregate(Aggregate.AGGREGATE_COUNT)
                                .setFilterCel("this.amount > 100").build()))
                .build());

        assertThatThrownBy(() -> MetricMappings.build("filters", broken, OPTIONS))
                .isInstanceOfSatisfying(MetricSchemaException.class, e ->
                        assertThat(e.violations())
                                .anySatisfy(v -> assertThat(v)
                                        .contains("test.Filters.j").contains("must be bool"))
                                .anySatisfy(v -> assertThat(v)
                                        .contains("test.Filters.k")
                                        .contains("translatable subset")));
    }

    @Test
    void collisionsRepeatedFieldsAndUnknownIdentityAreRefused() throws Exception {
        Descriptor broken = build(DescriptorProto.newBuilder().setName("Odds")
                .setOptions(com.google.protobuf.DescriptorProtos.MessageOptions.newBuilder()
                        .setExtension(MetricProto.metricMessage, MessageMetric.newBuilder()
                                .setSubject("odds").setIdentityField("missing").build()))
                .addField(declared("m", 1, FieldDescriptorProto.Type.TYPE_STRING,
                        FieldMetric.newBuilder()
                                .setRole(MemberRole.MEMBER_ROLE_DIMENSION)
                                .setName("shared").build()))
                .addField(declared("n", 2, FieldDescriptorProto.Type.TYPE_STRING,
                        FieldMetric.newBuilder()
                                .setRole(MemberRole.MEMBER_ROLE_DIMENSION)
                                .setName("shared").build()))
                .addField(FieldDescriptorProto.newBuilder()
                        .setName("tags").setNumber(3)
                        .setType(FieldDescriptorProto.Type.TYPE_STRING)
                        .setLabel(FieldDescriptorProto.Label.LABEL_REPEATED)
                        .setOptions(FieldOptions.newBuilder()
                                .setExtension(MetricProto.metric, dimension())))
                .build());

        assertThatThrownBy(() -> MetricMappings.build("", broken, OPTIONS))
                .isInstanceOfSatisfying(MetricSchemaException.class, e ->
                        assertThat(e.violations())
                                .anySatisfy(v -> assertThat(v)
                                        .contains("'shared'").contains("collides"))
                                .anySatisfy(v -> assertThat(v)
                                        .contains("test.Odds.tags").contains("repeated"))
                                .anySatisfy(v -> assertThat(v)
                                        .contains("identity_field 'missing'")));
    }

    @Test
    void aMappingWithoutASubjectAnywhereIsRefused() throws Exception {
        Descriptor bare = build(DescriptorProto.newBuilder().setName("NoSubject")
                .addField(declared("p", 1, FieldDescriptorProto.Type.TYPE_STRING, dimension()))
                .build());
        assertThatThrownBy(() -> MetricMappings.build("", bare, OPTIONS))
                .isInstanceOfSatisfying(MetricSchemaException.class, e ->
                        assertThat(e.violations())
                                .anySatisfy(v -> assertThat(v).contains("no subject")));
    }
}
