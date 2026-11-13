package ai.protomolt.proto.metric.spi;

import ai.protomolt.proto.metric.Aggregate;
import ai.protomolt.proto.metric.MemberRole;
import ai.protomolt.proto.metric.TimeGrain;
import ai.protomolt.proto.metric.spi.CompiledMetricQuery.RowPredicate;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MetricPredicatePrecisionTest {
    private static final long EXACT = 9007199254740993L;

    private static MetricMapping mapping() {
        return new MetricMapping("numbers", "test.Numbers", Map.of(
                "count", numeric("count"), "score", numeric("score")));
    }

    private static MetricMapping.MetricMember numeric(String name) {
        return new MetricMapping.MetricMember(name, MemberRole.MEMBER_ROLE_DIMENSION,
                Aggregate.AGGREGATE_UNSPECIFIED, name, name, MetricMapping.FieldKind.NUMERIC,
                List.of(), "", List.of(), TimeGrain.TIME_GRAIN_UNSPECIFIED, "", "");
    }

    @Test void unrelatedDoubleDoesNotRoundInt64Equality() {
        var result = (RowPredicate.And) MetricRowPredicates.compile(mapping(),
                "count == 9007199254740993 && score > 1.5");
        var count = (RowPredicate.NumericRange) result.children().get(0);
        assertThat(count.filter().integral()).isTrue();
        assertThat(count.filter().gteLong()).isEqualTo(EXACT);
        assertThat(count.filter().lteLong()).isEqualTo(EXACT);
    }

    @Test void unrelatedDoubleDoesNotRoundInt64Membership() {
        var result = (RowPredicate.And) MetricRowPredicates.compile(mapping(),
                "this.count in [9007199254740993] && this.score > 1.5");
        var count = (RowPredicate.NumericRange) result.children().get(0);
        assertThat(count.filter().integral()).isTrue();
        assertThat(count.filter().gteLong()).isEqualTo(EXACT);
    }

    @Test void oneMemberCanHaveExactIntegerAndFractionalComparisons() {
        var result = (RowPredicate.And) MetricRowPredicates.compile(mapping(),
                "this.count == 9007199254740993 && this.count > 1.5");
        var count = (RowPredicate.NumericRange) result.children().get(0);
        assertThat(count.filter().gteLong()).isEqualTo(EXACT);
        assertThat(count.filter().integral()).isTrue();
        assertThat(MetricRowPredicates.referencedMembers(mapping(),
                "this.count == 9007199254740993 && this.score > 1.5"))
                .containsExactlyInAnyOrder("count", "score");
    }

    @Test void unknownNumericRepresentationDoesNotPermitNonnumericOperands() {
        for (String expression : List.of("count == true", "this.count == 'text'", "!this.count",
                "count in ['text']", "this.unknown == 1", "score + 1.5 > 2.0")) {
            assertThatThrownBy(() -> MetricRowPredicates.compile(mapping(), expression))
                    .as(expression).isInstanceOf(MetricRefusal.class);
        }
    }

    @Test void descriptorBackedIntegersAndFloatingFieldsKeepTheirDeclaredTypes() throws Exception {
        var descriptor = com.google.protobuf.Descriptors.FileDescriptor.buildFrom(
                com.google.protobuf.DescriptorProtos.FileDescriptorProto.newBuilder()
                        .setName("numbers.proto").setPackage("test").setSyntax("proto3")
                        .addMessageType(com.google.protobuf.DescriptorProtos.DescriptorProto.newBuilder().setName("Numbers")
                                .addField(com.google.protobuf.DescriptorProtos.FieldDescriptorProto.newBuilder()
                                        .setName("count").setNumber(1)
                                        .setType(com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64))
                                .addField(com.google.protobuf.DescriptorProtos.FieldDescriptorProto.newBuilder()
                                        .setName("score").setNumber(2)
                                        .setType(com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type.TYPE_DOUBLE)))
                        .build(), new com.google.protobuf.Descriptors.FileDescriptor[0]).findMessageTypeByName("Numbers");
        var typed = new MetricMapping("numbers", "test.Numbers", mapping().members(), descriptor);
        var result = (RowPredicate.And) MetricRowPredicates.compile(typed,
                "this.count == 9007199254740993 && this.score == 1.5");
        assertThat(((RowPredicate.NumericRange) result.children().get(0)).filter().gteLong()).isEqualTo(EXACT);
        assertThat(((RowPredicate.NumericRange) result.children().get(1)).filter().integral()).isFalse();
        assertThatThrownBy(() -> MetricRowPredicates.compile(typed, "this.count == 1.5"))
                .isInstanceOf(MetricRefusal.class);
    }

    @Test void fractionalPredicatesRemainAcceptedWithoutDescriptor() {
        assertThat(MetricRowPredicates.compile(mapping(), "this.score == 1.5"))
                .isInstanceOf(RowPredicate.NumericRange.class);
        assertThat(MetricRowPredicates.compile(mapping(), "score in [1.5, 2.5]"))
                .isInstanceOf(RowPredicate.Or.class);
    }
}
