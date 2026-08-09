package ai.pipestream.proto.shapes;

import ai.pipestream.proto.cel.CelMappingRule;
import ai.pipestream.proto.sources.CompiledProtos;
import ai.pipestream.proto.sources.ProtoSourceCompiler;
import ai.pipestream.proto.sources.ProtoSourceSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The joiner itself, beyond what the shape tests exercise: implied rules plus extra rules
 * in order, CEL filters gating selectors, CEL text fallbacks applied in place on the
 * progressive target, and the null-target guard.
 */
class MessageJoinerTest {

    private static final String PROTO = """
            syntax = "proto3";
            package join.test;
            message Order {
              string id = 1;
              int64 qty = 2;
            }
            message Customer {
              string id = 1;
              string name = 2;
            }
            """;

    private static Descriptor order;
    private static Descriptor customer;

    private final ShapeSynthesizer synthesizer = new ShapeSynthesizer();
    private final MessageJoiner joiner = new MessageJoiner();

    @BeforeAll
    static void compile() throws Exception {
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("join/test/join.proto", PROTO, "test").build());
        var file = compiled.descriptorFor("join/test/join.proto").orElseThrow();
        order = file.findMessageTypeByName("Order");
        customer = file.findMessageTypeByName("Customer");
    }

    private static ShapeSynthesizer.SynthesizedShape labelShape(ShapeSynthesizer s) {
        return s.projection("derived.v1.Label",
                List.of(new ShapeSynthesizer.NamedType("order", order),
                        new ShapeSynthesizer.NamedType("customer", customer)),
                List.of(new ShapeSynthesizer.ProjectedField("text", "order.id"),
                        new ShapeSynthesizer.ProjectedField("qty", "order.qty")));
    }

    private static MessageScope scope(long qty) {
        return MessageScope.builder()
                .add("order", DynamicMessage.newBuilder(order)
                        .setField(order.findFieldByName("id"), "o-1")
                        .setField(order.findFieldByName("qty"), qty)
                        .build())
                .add("customer", DynamicMessage.newBuilder(customer)
                        .setField(customer.findFieldByName("id"), "c-9")
                        .setField(customer.findFieldByName("name"), "Pat")
                        .build())
                .build();
    }

    @Test
    void extraRulesRunAfterTheImpliedOnes() throws Exception {
        var shape = labelShape(synthesizer);
        // The implied rule maps text from order.id; the extra rule runs later and wins.
        DynamicMessage joined = joiner.join(shape, scope(3),
                List.of("text = customer.name"), List.of());
        Descriptor type = shape.type();
        assertThat(joined.getField(type.findFieldByName("text"))).isEqualTo("Pat");
        assertThat(joined.getField(type.findFieldByName("qty"))).isEqualTo(3L);
    }

    @Test
    void celFiltersGateTheirSelectors() throws Exception {
        var shape = labelShape(synthesizer);
        CelMappingRule rule = new CelMappingRule("order.qty > 100", "'big'", "text");

        DynamicMessage small = joiner.join(shape.type(), scope(3), List.of(), List.of(rule));
        assertThat(small.getField(shape.type().findFieldByName("text"))).isEqualTo("");

        DynamicMessage big = joiner.join(shape.type(), scope(300), List.of(), List.of(rule));
        assertThat(big.getField(shape.type().findFieldByName("text"))).isEqualTo("big");
    }

    @Test
    void celFallbackRulesApplyInPlaceOnTheProgressiveTarget() throws Exception {
        var shape = labelShape(synthesizer);
        // No selector: the fallback is the in-place dialect, applied to the target itself.
        DynamicMessage joined = joiner.join(shape, scope(3), List.of(),
                List.of(new CelMappingRule(null, null, "qty", List.of("qty = 41"))));
        assertThat(joined.getField(shape.type().findFieldByName("qty"))).isEqualTo(41L);
        // A scoped-looking fallback is not resolved against the scope: it is in-place.
        assertThatThrownBy(() -> joiner.join(shape.type(), scope(3), List.of(),
                List.of(new CelMappingRule(null, null, "text",
                        List.of("text = customer.name")))))
                .isInstanceOf(ai.pipestream.proto.mapper.MappingException.class);
    }

    @Test
    void theTargetIsRequired() {
        MessageScope scope = scope(1);
        assertThatThrownBy(() -> joiner.join((Descriptor) null, scope, List.of(), List.of()))
                .isInstanceOf(NullPointerException.class);
    }
}
