package ai.protomolt.proto.shapes;

import ai.protomolt.proto.descriptors.DescriptorRegistry;
import ai.protomolt.proto.mapper.MappingException;
import ai.protomolt.proto.sources.CompiledProtos;
import ai.protomolt.proto.sources.ProtoSourceCompiler;
import ai.protomolt.proto.sources.ProtoSourceSet;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The scoped text-rule runtime, exercised directly rather than through the joiner: rule
 * syntax errors, unknown scope names, absent-optionSource skips, list semantics of
 * {@code =} versus {@code +=}, and the {@code null} literal clearing its target.
 */
class ScopedProtoMapperTest {

    private static final String PROTO = """
            syntax = "proto3";
            package scope.test;
            import "google/protobuf/any.proto";
            message Order {
              string id = 1;
              repeated string tags = 2;
              Address ship_to = 3;
              google.protobuf.Any payload = 4;
            }
            message Address {
              string city = 1;
            }
            message Customer {
              string id = 1;
              string name = 2;
            }
            """;

    private static Descriptor order;
    private static Descriptor customer;

    private static ScopedProtoMapper mapper;

    @BeforeAll
    static void compile() throws Exception {
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("scope/test/scope.proto", PROTO, "test").build());
        var file = compiled.descriptorFor("scope/test/scope.proto").orElseThrow();
        order = file.findMessageTypeByName("Order");
        customer = file.findMessageTypeByName("Customer");
        DescriptorRegistry registry = DescriptorRegistry.create();
        registry.registerFile(file);
        mapper = new ScopedProtoMapper(registry);
    }

    private static DynamicMessage order(String id, String city, String... tags) {
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(order)
                .setField(order.findFieldByName("id"), id)
                .setField(order.findFieldByName("tags"), List.of(tags));
        if (city != null) {
            Descriptor address = order.findFieldByName("ship_to").getMessageType();
            builder.setField(order.findFieldByName("ship_to"),
                    DynamicMessage.newBuilder(address)
                            .setField(address.findFieldByName("city"), city)
                            .build());
        }
        return builder.build();
    }

    private static MessageScope scope() {
        return MessageScope.builder()
                .add("order", order("o-1", null, "a", "b"))
                .add("customer", DynamicMessage.newBuilder(customer)
                        .setField(customer.findFieldByName("id"), "c-9")
                        .setField(customer.findFieldByName("name"), "Pat")
                        .build())
                .build();
    }

    private static DynamicMessage.Builder target() {
        return DynamicMessage.newBuilder(order);
    }

    @Test
    void blankRulesAreSkipped() throws Exception {
        DynamicMessage.Builder out = target();
        mapper.map(scope(), out, List.of("", "   "));
        assertThat(out.build().getField(order.findFieldByName("id"))).isEqualTo("");
    }

    @Test
    void malformedRulesFailWithTheRuleAttached() {
        DynamicMessage.Builder out = target();
        assertThatThrownBy(() -> mapper.map(scope(), out, List.of("id order.id")))
                .isInstanceOf(MappingException.class)
                .hasMessageContaining("not 'target = source.path'");
        assertThatThrownBy(() -> mapper.map(scope(), out, List.of(" = order.id")))
                .isInstanceOf(MappingException.class);
        assertThatThrownBy(() -> mapper.map(scope(), out, List.of("id = ")))
                .isInstanceOf(MappingException.class);
    }

    @Test
    void unknownScopeNamesAreAlwaysAnError() {
        DynamicMessage.Builder out = target();
        assertThatThrownBy(() -> mapper.map(scope(), out, List.of("id = warehouse.id")))
                .isInstanceOf(MappingException.class)
                .hasMessageContaining("Unknown source 'warehouse'")
                .hasMessageContaining("order");
    }

    @Test
    void aBareNameResolvesTheWholeEntry() throws Exception {
        MessageScope scope = scope();
        assertThat(mapper.resolve(scope, "order", "test")).isEqualTo(scope.get("order"));
        assertThat(mapper.resolve(scope, "order.id", "test")).isEqualTo("o-1");
    }

    /**
     * Regression test: an unset <em>intermediate</em> hop (order.ship_to.city with no
     * ship_to) used to fail the whole mapping with a MappingException from the field
     * accessor, contradicting the documented "an unset optional hop skips its rule"
     * join semantics; resolve() now treats it as absent, like MessageProjection does.
     */
    @Test
    void absentOptionalSourcesSkipTheirRule() throws Exception {
        MessageScope withAddress = MessageScope.builder()
                .add("order", order("o-1", "Springfield"))
                .build();
        DynamicMessage.Builder out = target();
        // The scope built by scope() has no ship_to: both rules resolve to nothing and skip.
        mapper.map(scope(), out,
                List.of("ship_to = order.ship_to", "id = order.ship_to.city"));
        DynamicMessage built = out.build();
        assertThat(built.hasField(order.findFieldByName("ship_to"))).isFalse();
        assertThat(built.getField(order.findFieldByName("id"))).isEqualTo("");

        // Same rules against a present ship_to resolve normally.
        DynamicMessage.Builder filled = target();
        mapper.map(withAddress, filled, List.of("id = order.ship_to.city"));
        assertThat(filled.build().getField(order.findFieldByName("id")))
                .isEqualTo("Springfield");
    }

    @Test
    void onlyAnUnsetIntermediateMessageIsAbsent() {
        assertThatThrownBy(() -> mapper.resolve(scope(), "order.missing", "test"))
                .isInstanceOf(MappingException.class)
                .hasMessageContaining("Field 'missing' not found in message 'Order'");
        assertThatThrownBy(() -> mapper.resolve(scope(), "order.id.deeper", "test"))
                .isInstanceOf(MappingException.class)
                .hasMessageContaining("non-message or repeated field 'id'");
        assertThatThrownBy(() -> mapper.resolve(scope(), "order.tags.deeper", "test"))
                .isInstanceOf(MappingException.class)
                .hasMessageContaining("non-message or repeated field 'tags'");
    }

    @Test
    void anyUnpackFailuresAreNotTreatedAsAbsent() {
        Any unknown = Any.newBuilder()
                .setTypeUrl("type.googleapis.com/scope.test.NotRegistered")
                .setValue(ByteString.copyFromUtf8("not-a-known-message"))
                .build();
        DynamicMessage source = DynamicMessage.newBuilder(order)
                .setField(order.findFieldByName("payload"), unknown)
                .build();
        MessageScope scope = MessageScope.builder().add("order", source).build();

        assertThatThrownBy(() -> mapper.resolve(scope, "order.payload.value", "test"))
                .isInstanceOf(MappingException.class)
                .hasMessageContaining("Failed to unpack Any field 'payload'");
    }

    @Test
    void assigningAListClearsBeforeAppending() throws Exception {
        DynamicMessage.Builder out = target();
        mapper.map(scope(), out, List.of("tags = order.tags", "tags = order.tags"));
        // Plain assignment replaces; only += accumulates.
        assertThat(out.build().getField(order.findFieldByName("tags")))
                .isEqualTo(List.of("a", "b"));
    }

    @Test
    void appendAccumulatesAcrossRules() throws Exception {
        DynamicMessage.Builder out = target();
        mapper.map(scope(), out, List.of("tags = order.tags", "tags += order.tags"));
        assertThat(out.build().getField(order.findFieldByName("tags")))
                .isEqualTo(List.of("a", "b", "a", "b"));
    }

    @Test
    void theNullLiteralClearsItsTarget() throws Exception {
        DynamicMessage.Builder out = target();
        mapper.map(scope(), out, List.of("id = order.id", "id = null"));
        assertThat(out.build().getField(order.findFieldByName("id"))).isEqualTo("");
    }

    @Test
    void dashRulesClearFields() throws Exception {
        DynamicMessage.Builder out = target();
        mapper.map(scope(), out, List.of("id = order.id", "-id", "tags = order.tags", "-tags"));
        DynamicMessage built = out.build();
        assertThat(built.getField(order.findFieldByName("id"))).isEqualTo("");
        assertThat(built.getField(order.findFieldByName("tags"))).isEqualTo(List.of());
    }

    @Test
    void appendingToASingularFieldFailsAtRuntime() {
        DynamicMessage.Builder out = target();
        assertThatThrownBy(() -> mapper.map(scope(), out, List.of("id += order.id")))
                .isInstanceOf(MappingException.class)
                .hasMessageContaining("not repeated");
    }

    @Test
    void aRegistryIsRequired() {
        assertThatThrownBy(() -> new ScopedProtoMapper(null))
                .isInstanceOf(NullPointerException.class);
    }
}
