package ai.pipestream.proto.metric.spi;

import static ai.pipestream.proto.metric.spi.MetricMappingsTest.OPTIONS;
import static ai.pipestream.proto.metric.spi.MetricMappingsTest.declared;
import static ai.pipestream.proto.metric.spi.MetricMappingsTest.dimension;
import static ai.pipestream.proto.metric.spi.MetricMappingsTest.measure;
import static ai.pipestream.proto.metric.spi.MetricMappingsTest.scalar;
import static ai.pipestream.proto.metric.spi.MetricMappingsTest.timestamp;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.pipestream.proto.metric.Aggregate;
import ai.pipestream.proto.metric.FieldMetric;
import ai.pipestream.proto.metric.MemberRole;
import ai.pipestream.proto.metric.TimeGrain;
import ai.pipestream.proto.metric.spi.CompiledMetricQuery.DimensionKind;
import ai.pipestream.proto.metric.spi.CompiledMetricQuery.EqualsFilter;
import ai.pipestream.proto.metric.spi.MetricMapping.FieldKind;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.TimestampProto;
import org.junit.jupiter.api.Test;

/**
 * Nested declarations become members: the walk descends singular message
 * fields, field names flatten to the index mapping's own naming, a nested
 * filter_cel speaks about its declaring message's siblings, repeated paths
 * refuse loudly, and recursive types stop the descent instead of hanging
 * the build.
 */
class NestedMetricMembersTest {

    static FieldDescriptorProto.Builder message(String name, int number, String typeName) {
        return FieldDescriptorProto.newBuilder()
                .setName(name).setNumber(number)
                .setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
                .setTypeName(".test." + typeName)
                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL);
    }

    static Descriptor buildFile(String root, DescriptorProto... messages) throws Exception {
        FileDescriptorProto.Builder file = FileDescriptorProto.newBuilder()
                .setName("test/nested.proto").setPackage("test").setSyntax("proto3")
                .addDependency("google/protobuf/timestamp.proto");
        for (DescriptorProto type : messages) {
            file.addMessageType(type);
        }
        return FileDescriptor.buildFrom(
                        file.build(), new FileDescriptor[] {TimestampProto.getDescriptor()})
                .findMessageTypeByName(root);
    }

    @Test
    void nestedDeclarationsBecomeMembersOnTheFlattenedFieldNames() throws Exception {
        Descriptor document = buildFile("Document",
                DescriptorProto.newBuilder().setName("Document")
                        .addField(declared("doc_id", 1, FieldDescriptorProto.Type.TYPE_STRING,
                                FieldMetric.newBuilder()
                                        .setRole(MemberRole.MEMBER_ROLE_MEASURE)
                                        .setAggregate(Aggregate.AGGREGATE_COUNT)
                                        .setName("documents").build()))
                        .addField(message("meta", 2, "Meta"))
                        .build(),
                DescriptorProto.newBuilder().setName("Meta")
                        .addField(declared("language", 1,
                                FieldDescriptorProto.Type.TYPE_STRING, dimension()))
                        .addField(declared("bytes_seed", 2,
                                FieldDescriptorProto.Type.TYPE_INT64,
                                measure(Aggregate.AGGREGATE_SUM)))
                        .addField(timestamp("processed_at", 3, FieldMetric.newBuilder()
                                .setRole(MemberRole.MEMBER_ROLE_DIMENSION)
                                .setDefaultGrain(TimeGrain.TIME_GRAIN_DAY).build()))
                        .build());

        MetricMapping mapping = MetricMappings.build("corpus", document, OPTIONS);
        assertThat(mapping.member("documents").orElseThrow().fieldName())
                .isEqualTo("doc_id");
        assertThat(mapping.member("language")).hasValueSatisfying(member -> {
            assertThat(member.fieldName()).isEqualTo("meta_language");
            assertThat(member.fieldPath()).isEqualTo("meta.language");
            assertThat(member.kind()).isEqualTo(FieldKind.KEYWORD);
        });
        assertThat(mapping.member("bytes_seed").orElseThrow().fieldName())
                .isEqualTo("meta_bytes_seed");
        assertThat(mapping.member("processed_at")).hasValueSatisfying(member -> {
            assertThat(member.fieldName()).isEqualTo("meta_processed_at");
            assertThat(member.kind()).isEqualTo(FieldKind.DATE);
            assertThat(member.defaultGrain()).isEqualTo(TimeGrain.TIME_GRAIN_DAY);
        });
    }

    @Test
    void deepNestingFlattensEveryLevel() throws Exception {
        Descriptor root = buildFile("A",
                DescriptorProto.newBuilder().setName("A")
                        .addField(message("b", 1, "B")).build(),
                DescriptorProto.newBuilder().setName("B")
                        .addField(message("c", 1, "C")).build(),
                DescriptorProto.newBuilder().setName("C")
                        .addField(declared("kind", 1,
                                FieldDescriptorProto.Type.TYPE_STRING, dimension()))
                        .build());

        MetricMapping mapping = MetricMappings.build("deep", root, OPTIONS);
        assertThat(mapping.member("kind").orElseThrow().fieldName())
                .isEqualTo("b_c_kind");
        assertThat(mapping.member("kind").orElseThrow().fieldPath())
                .isEqualTo("b.c.kind");
    }

    @Test
    void aNestedFilterCelSpeaksAboutItsDeclaringMessage() throws Exception {
        // `tier` and `paying` exist only on Profile: the filter compiles
        // against the declaring message, and the translated filters carry
        // the same flattened prefix as the member itself.
        Descriptor account = buildFile("Account",
                DescriptorProto.newBuilder().setName("Account")
                        .addField(message("profile", 1, "Profile")).build(),
                DescriptorProto.newBuilder().setName("Profile")
                        .addField(scalar("tier", 1, FieldDescriptorProto.Type.TYPE_STRING))
                        .addField(scalar("paying", 2, FieldDescriptorProto.Type.TYPE_BOOL))
                        .addField(declared("id", 3, FieldDescriptorProto.Type.TYPE_STRING,
                                FieldMetric.newBuilder()
                                        .setRole(MemberRole.MEMBER_ROLE_MEASURE)
                                        .setAggregate(Aggregate.AGGREGATE_COUNT)
                                        .setName("gold_accounts")
                                        .setFilterCel(
                                                "this.paying == true && this.tier == 'gold'")
                                        .build()))
                        .build());

        MetricMapping mapping = MetricMappings.build("accounts", account, OPTIONS);
        assertThat(mapping.member("gold_accounts").orElseThrow().rowFilters())
                .containsExactly(
                        new EqualsFilter("gold_accounts", "profile_paying",
                                "profile.paying", DimensionKind.BOOLEAN,
                                java.util.List.of("true")),
                        new EqualsFilter("gold_accounts", "profile_tier",
                                "profile.tier", DimensionKind.TERM,
                                java.util.List.of("gold")));
    }

    @Test
    void aDeclarationBelowARepeatedFieldIsRefused() throws Exception {
        Descriptor order = buildFile("Order",
                DescriptorProto.newBuilder().setName("Order")
                        .addField(message("items", 1, "Item")
                                .setLabel(FieldDescriptorProto.Label.LABEL_REPEATED))
                        .build(),
                DescriptorProto.newBuilder().setName("Item")
                        .addField(declared("sku", 1,
                                FieldDescriptorProto.Type.TYPE_STRING, dimension()))
                        .build());

        assertThatThrownBy(() -> MetricMappings.build("orders", order, OPTIONS))
                .isInstanceOf(MetricSchemaException.class)
                .hasMessageContaining("test.Item.sku")
                .hasMessageContaining("repeated field 'test.Order.items'");
    }

    @Test
    void aRecursiveTypeStopsTheDescentInsteadOfHanging() throws Exception {
        Descriptor node = buildFile("Node",
                DescriptorProto.newBuilder().setName("Node")
                        .addField(declared("label", 1,
                                FieldDescriptorProto.Type.TYPE_STRING, dimension()))
                        .addField(message("next", 2, "Node"))
                        .build());

        MetricMapping mapping = MetricMappings.build("nodes", node, OPTIONS);
        assertThat(mapping.memberNames()).containsExactly("label");
        assertThat(mapping.member("label").orElseThrow().fieldName()).isEqualTo("label");
    }

    @Test
    void memberNamesCollidingAcrossDepthsAreRefused() throws Exception {
        Descriptor document = buildFile("Document",
                DescriptorProto.newBuilder().setName("Document")
                        .addField(declared("status", 1,
                                FieldDescriptorProto.Type.TYPE_STRING, dimension()))
                        .addField(message("meta", 2, "Meta"))
                        .build(),
                DescriptorProto.newBuilder().setName("Meta")
                        .addField(declared("status", 1,
                                FieldDescriptorProto.Type.TYPE_STRING, dimension()))
                        .build());

        assertThatThrownBy(() -> MetricMappings.build("corpus", document, OPTIONS))
                .isInstanceOf(MetricSchemaException.class)
                .hasMessageContaining("member name 'status'")
                .hasMessageContaining("collides");
    }
}
