package ai.protomolt.proto.metric.spi;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.protomolt.proto.metric.Aggregate;
import ai.protomolt.proto.metric.FieldMetric;
import ai.protomolt.proto.metric.MemberRole;
import ai.protomolt.proto.metric.MessageMetric;
import ai.protomolt.proto.metric.TimeGrain;
import ai.protomolt.proto.metric.spi.MetricMapping.FieldKind;
import ai.protomolt.proto.metric.spi.MetricMapping.MetricMember;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import org.junit.jupiter.api.Test;

/**
 * Synthetic members: measures with no backing field, declared on the
 * message. A filtered COUNT ({@code paying_count} without a phantom
 * field) and a calculated cel both build; everything needing storage, a
 * name, or an aggregate a fieldless member cannot run refuses at build
 * time naming the member — never at the first query.
 */
class SyntheticMembersTest {

    static Descriptor order() throws Exception {
        DescriptorProto.Builder message = DescriptorProto.newBuilder().setName("Order");
        message.addField(field("id", 1, FieldDescriptorProto.Type.TYPE_STRING));
        message.addField(field("segment", 2, FieldDescriptorProto.Type.TYPE_STRING));
        message.addField(field("paying", 3, FieldDescriptorProto.Type.TYPE_BOOL));
        message.addField(field("amount", 4, FieldDescriptorProto.Type.TYPE_INT64));
        FileDescriptorProto file = FileDescriptorProto.newBuilder()
                .setName("synthit/order.proto").setPackage("synthit").setSyntax("proto3")
                .addMessageType(message)
                .build();
        return FileDescriptor.buildFrom(file, new FileDescriptor[0])
                .findMessageTypeByName("Order");
    }

    static FieldDescriptorProto.Builder field(
            String name, int number, FieldDescriptorProto.Type type) {
        return FieldDescriptorProto.newBuilder()
                .setName(name).setNumber(number).setType(type)
                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL);
    }

    static CatalogMetricHintSource base() {
        return new CatalogMetricHintSource()
                .put("synthit.Order", "segment", FieldMetric.newBuilder()
                        .setRole(MemberRole.MEMBER_ROLE_DIMENSION).build())
                .put("synthit.Order", "amount", FieldMetric.newBuilder()
                        .setRole(MemberRole.MEMBER_ROLE_MEASURE)
                        .setAggregate(Aggregate.AGGREGATE_SUM)
                        .setName("revenue").build());
    }

    static FieldMetric.Builder synthetic(String name) {
        return FieldMetric.newBuilder()
                .setRole(MemberRole.MEMBER_ROLE_MEASURE)
                .setName(name);
    }

    @Test
    void aFilteredCountAndACalculatedCelBuildWithoutAField() throws Exception {
        MetricMapping mapping = MetricMappings.build("orders", order(),
                base().putMessage("synthit.Order", MessageMetric.newBuilder()
                        .addMembers(synthetic("paying_count")
                                .setAggregate(Aggregate.AGGREGATE_COUNT)
                                .setFilterCel("this.paying == true"))
                        .addMembers(synthetic("double_revenue")
                                .setCel("revenue * 2.0"))
                        .build()));

        MetricMember payingCount = mapping.members().get("paying_count");
        assertThat(payingCount.kind()).isEqualTo(FieldKind.SYNTHETIC);
        assertThat(payingCount.fieldName()).isEmpty();
        assertThat(payingCount.fieldPath()).isEmpty();
        assertThat(payingCount.aggregate()).isEqualTo(Aggregate.AGGREGATE_COUNT);
        // The filter translated against the root message with no prefix.
        assertThat(payingCount.rowFilters()).containsExactly(
                new CompiledMetricQuery.EqualsFilter("paying_count", "paying", "paying",
                        CompiledMetricQuery.DimensionKind.BOOLEAN,
                        java.util.List.of("true")));

        MetricMember doubled = mapping.members().get("double_revenue");
        assertThat(doubled.calculated()).isTrue();
        assertThat(doubled.celRequires()).containsExactly("revenue");
    }

    @Test
    void everythingAFieldlessMemberCannotBeRefusesAtBuildTime() throws Exception {
        Descriptor order = order();
        assertThatThrownBy(() -> MetricMappings.build("orders", order,
                base().putMessage("synthit.Order", MessageMetric.newBuilder()
                        .addMembers(FieldMetric.newBuilder()
                                .setRole(MemberRole.MEMBER_ROLE_MEASURE)
                                .setAggregate(Aggregate.AGGREGATE_COUNT))
                        .build())))
                .isInstanceOf(MetricSchemaException.class)
                .hasMessageContaining("declares no name");
        assertThatThrownBy(() -> MetricMappings.build("orders", order,
                base().putMessage("synthit.Order", MessageMetric.newBuilder()
                        .addMembers(synthetic("region")
                                .setRole(MemberRole.MEMBER_ROLE_DIMENSION))
                        .build())))
                .isInstanceOf(MetricSchemaException.class)
                .hasMessageContaining("a dimension needs storage");
        assertThatThrownBy(() -> MetricMappings.build("orders", order,
                base().putMessage("synthit.Order", MessageMetric.newBuilder()
                        .addMembers(synthetic("total")
                                .setAggregate(Aggregate.AGGREGATE_SUM))
                        .build())))
                .isInstanceOf(MetricSchemaException.class)
                .hasMessageContaining("only COUNT")
                .hasMessageContaining("AGGREGATE_SUM");
        assertThatThrownBy(() -> MetricMappings.build("orders", order,
                base().putMessage("synthit.Order", MessageMetric.newBuilder()
                        .addMembers(synthetic("revenue")
                                .setAggregate(Aggregate.AGGREGATE_COUNT))
                        .build())))
                .isInstanceOf(MetricSchemaException.class)
                .hasMessageContaining("collides");
        assertThatThrownBy(() -> MetricMappings.build("orders", order,
                base().putMessage("synthit.Order", MessageMetric.newBuilder()
                        .addMembers(synthetic("mixed")
                                .setAggregate(Aggregate.AGGREGATE_COUNT)
                                .setCel("revenue * 2.0"))
                        .build())))
                .isInstanceOf(MetricSchemaException.class)
                .hasMessageContaining("calculated and also declares");
        assertThatThrownBy(() -> MetricMappings.build("orders", order,
                base().putMessage("synthit.Order", MessageMetric.newBuilder()
                        .addMembers(synthetic("grained")
                                .setAggregate(Aggregate.AGGREGATE_COUNT)
                                .setDefaultGrain(TimeGrain.TIME_GRAIN_DAY))
                        .build())))
                .isInstanceOf(MetricSchemaException.class)
                .hasMessageContaining("default_grain");
    }
}
