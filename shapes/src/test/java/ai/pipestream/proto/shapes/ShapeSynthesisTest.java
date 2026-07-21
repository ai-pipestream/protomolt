package ai.pipestream.proto.shapes;

import ai.pipestream.proto.cel.CelMappingRule;
import ai.pipestream.proto.mapper.MappingException;
import ai.pipestream.proto.sources.CompiledProtos;
import ai.pipestream.proto.sources.ProtoSourceCompiler;
import ai.pipestream.proto.sources.ProtoSourceSet;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Shapes end to end: synthesized envelopes, projections, and tagged unions link, emit
 * registrable source, and join real messages through scoped rules and CEL.
 */
class ShapeSynthesisTest {

    private static final String ORDER_PROTO = """
            syntax = "proto3";
            package shop.v1;
            message Order {
              string id = 1;
              int64 qty = 2;
              repeated string tags = 3;
              Address ship_to = 4;
            }
            message Address {
              string city = 1;
            }
            """;

    private static final String CUSTOMER_PROTO = """
            syntax = "proto3";
            package crm.v1;
            message Customer {
              string id = 1;
              string name = 2;
              string email = 3;
            }
            """;

    private static Descriptor order;
    private static Descriptor customer;

    private final ShapeSynthesizer synthesizer = new ShapeSynthesizer();
    private final MessageJoiner joiner = new MessageJoiner();

    @BeforeAll
    static void compile() throws Exception {
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("shop/v1/order.proto", ORDER_PROTO, "test")
                .add("crm/v1/customer.proto", CUSTOMER_PROTO, "test")
                .build());
        order = compiled.descriptorFor("shop/v1/order.proto").orElseThrow()
                .findMessageTypeByName("Order");
        customer = compiled.descriptorFor("crm/v1/customer.proto").orElseThrow()
                .findMessageTypeByName("Customer");
    }

    private static DynamicMessage order(String id, long qty, String city) {
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(order);
        builder.setField(order.findFieldByName("id"), id);
        builder.setField(order.findFieldByName("qty"), qty);
        if (!city.isEmpty()) {
            Descriptor address = order.findFieldByName("ship_to").getMessageType();
            builder.setField(order.findFieldByName("ship_to"),
                    DynamicMessage.newBuilder(address)
                            .setField(address.findFieldByName("city"), city)
                            .build());
        }
        return builder.build();
    }

    private static DynamicMessage customer(String id, String name) {
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(customer);
        builder.setField(customer.findFieldByName("id"), id);
        builder.setField(customer.findFieldByName("name"), name);
        return builder.build();
    }

    private static MessageScope scope() {
        return MessageScope.builder()
                .add("order", order("o-1", 3, "Springfield"))
                .add("customer", customer("c-9", "Pat"))
                .build();
    }

    @Test
    void envelopeHoldsEachSourceIntact() throws Exception {
        var shape = synthesizer.envelope("derived.v1.OrderWithCustomer", List.of(
                new ShapeSynthesizer.NamedType("order", order),
                new ShapeSynthesizer.NamedType("customer", customer)));
        assertThat(shape.type().getFullName()).isEqualTo("derived.v1.OrderWithCustomer");
        assertThat(shape.protoSource())
                .contains("import \"shop/v1/order.proto\";")
                .contains("import \"crm/v1/customer.proto\";")
                .contains("shop.v1.Order order = 1;")
                .contains("crm.v1.Customer customer = 2;");

        DynamicMessage joined = joiner.join(shape, scope(), List.of(), List.of());
        DynamicMessage inner = (DynamicMessage) joined.getField(
                shape.type().findFieldByName("customer"));
        assertThat(inner.getField(customer.findFieldByName("name"))).isEqualTo("Pat");
    }

    @Test
    void projectionInfersFieldTypesFromSourcePaths() throws Exception {
        var shape = synthesizer.projection("derived.v1.OrderSummary",
                List.of(new ShapeSynthesizer.NamedType("order", order),
                        new ShapeSynthesizer.NamedType("customer", customer)),
                List.of(new ShapeSynthesizer.ProjectedField("order_id", "order.id"),
                        new ShapeSynthesizer.ProjectedField("qty", "order.qty"),
                        new ShapeSynthesizer.ProjectedField("tags", "order.tags"),
                        new ShapeSynthesizer.ProjectedField("city", "order.ship_to.city"),
                        new ShapeSynthesizer.ProjectedField("customer_name", "customer.name")));
        Descriptor type = shape.type();
        assertThat(type.findFieldByName("qty").getType())
                .isEqualTo(FieldDescriptor.Type.INT64);
        assertThat(type.findFieldByName("tags").isRepeated()).isTrue();
        assertThat(type.findFieldByName("city").getType())
                .isEqualTo(FieldDescriptor.Type.STRING);

        DynamicMessage joined = joiner.join(shape, scope(), List.of(), List.of());
        assertThat(joined.getField(type.findFieldByName("order_id"))).isEqualTo("o-1");
        assertThat(joined.getField(type.findFieldByName("qty"))).isEqualTo(3L);
        assertThat(joined.getField(type.findFieldByName("city"))).isEqualTo("Springfield");
        assertThat(joined.getField(type.findFieldByName("customer_name"))).isEqualTo("Pat");
    }

    @Test
    void celRulesJoinAcrossSources() throws Exception {
        var shape = synthesizer.projection("derived.v1.Line",
                List.of(new ShapeSynthesizer.NamedType("order", order),
                        new ShapeSynthesizer.NamedType("customer", customer)),
                List.of(new ShapeSynthesizer.ProjectedField("label", "customer.name")));
        DynamicMessage joined = joiner.join(shape.type(), scope(), List.of(),
                List.of(new CelMappingRule(null,
                        "customer.name + ' x' + string(order.qty)", "label")));
        assertThat(joined.getField(shape.type().findFieldByName("label")))
                .isEqualTo("Pat x3");
    }

    @Test
    void taggedUnionWrapsEitherCase() throws Exception {
        var shape = synthesizer.taggedUnion("derived.v1.Either", List.of(
                new ShapeSynthesizer.NamedType("order", order),
                new ShapeSynthesizer.NamedType("customer", customer)));
        assertThat(shape.protoSource()).contains("oneof value {");

        DynamicMessage wrapped = joiner.wrap(shape, "customer", customer("c-1", "Sam"));
        var oneof = shape.type().getOneofs().get(0);
        assertThat(wrapped.getOneofFieldDescriptor(oneof).getName()).isEqualTo("customer");
        assertThatThrownBy(() -> joiner.wrap(shape, "invoice", customer("c", "x")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("invoice");
    }

    @Test
    void descriptorSetIsSelfContainedAndDependencyOrdered() throws Exception {
        var shape = synthesizer.envelope("derived.v1.Pair", List.of(
                new ShapeSynthesizer.NamedType("order", order),
                new ShapeSynthesizer.NamedType("customer", customer)));
        FileDescriptorSet set = shape.descriptorSet();
        // Every file's dependencies appear earlier: a single forward pass links.
        Map<String, FileDescriptor> built = new HashMap<>();
        for (FileDescriptorProto proto : set.getFileList()) {
            FileDescriptor[] deps = proto.getDependencyList().stream()
                    .map(built::get).toArray(FileDescriptor[]::new);
            assertThat(deps).doesNotContainNull();
            built.put(proto.getName(), FileDescriptor.buildFrom(proto, deps));
        }
        assertThat(built).containsKey("derived/v1/pair.proto");
    }

    @Test
    void scopedRulesSupportAppendClearAndWholeMessage() throws Exception {
        var shape = synthesizer.envelope("derived.v1.Wrapped", List.of(
                new ShapeSynthesizer.NamedType("order", order)));
        DynamicMessage joined = joiner.join(shape.type(), scope(),
                List.of("order = order",
                        "order.tags += customer.name",
                        "-order.ship_to"),
                List.of());
        DynamicMessage inner = (DynamicMessage) joined.getField(
                shape.type().findFieldByName("order"));
        assertThat(inner.getField(order.findFieldByName("tags")))
                .isEqualTo(List.of("Pat"));
        assertThat(inner.hasField(order.findFieldByName("ship_to"))).isFalse();
    }

    /**
     * Literal sources are part of the shared rule syntax, so the scoped runtime must not
     * read them as scope names — the static checker already waves them through.
     */
    @Test
    void scopedRulesAcceptLiteralSources() throws Exception {
        DynamicMessage joined = joiner.join(order, scope(),
                List.of("ship_to = order.ship_to",
                        "id = \"fixed\"",
                        "qty = 7",
                        "tags += \"extra\"",
                        "ship_to = null"),
                List.of());
        assertThat(joined.getField(order.findFieldByName("id"))).isEqualTo("fixed");
        assertThat(joined.getField(order.findFieldByName("qty"))).isEqualTo(7L);
        assertThat(joined.getField(order.findFieldByName("tags"))).isEqualTo(List.of("extra"));
        assertThat(joined.hasField(order.findFieldByName("ship_to"))).isFalse();
    }

    @Test
    void badPathsAndNamesFailClearly() {
        List<ShapeSynthesizer.NamedType> sources =
                List.of(new ShapeSynthesizer.NamedType("order", order));
        assertThatThrownBy(() -> synthesizer.projection("derived.v1.Bad", sources,
                List.of(new ShapeSynthesizer.ProjectedField("x", "invoice.id"))))
                .hasMessageContaining("Unknown source 'invoice'");
        assertThatThrownBy(() -> synthesizer.projection("derived.v1.Bad", sources,
                List.of(new ShapeSynthesizer.ProjectedField("x", "order.nope"))))
                .hasMessageContaining("No field 'nope'");
        assertThatThrownBy(() -> synthesizer.projection("derived.v1.Bad", sources,
                List.of(new ShapeSynthesizer.ProjectedField("x", "order.tags.city"))))
                .hasMessageContaining("not a singular message");
        assertThatThrownBy(() -> synthesizer.envelope("derived.v1.Bad", List.of(
                new ShapeSynthesizer.NamedType("order", order),
                new ShapeSynthesizer.NamedType("order", customer))))
                .hasMessageContaining("Duplicate source name");
        assertThatThrownBy(() -> joiner.join(order, scope(),
                List.of("id = warehouse.id"), List.of()))
                .isInstanceOf(MappingException.class)
                .hasMessageContaining("Unknown source 'warehouse'");
    }
}
