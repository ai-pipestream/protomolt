package ai.pipestream.proto.shapes;

import ai.pipestream.proto.sources.CompiledProtos;
import ai.pipestream.proto.sources.ProtoSourceCompiler;
import ai.pipestream.proto.sources.ProtoSourceSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The schema-level merge: validate (clash report from descriptors alone), resolve (rename,
 * prefer, coalesce), emit (shape plus defined-join and defined-union rules in one move).
 */
class SchemaMergerTest {

    private static final String ORDER_PROTO = """
            syntax = "proto3";
            package shop.v1;
            message Order {
              string id = 1;
              int64 qty = 2;
              string status = 3;
              repeated string tags = 4;
            }
            """;

    private static final String TICKET_PROTO = """
            syntax = "proto3";
            package support.v1;
            message Ticket {
              string id = 1;
              Status status = 2;
              string assignee = 3;
              repeated string tags = 4;
            }
            enum Status {
              STATUS_UNSPECIFIED = 0;
              OPEN = 1;
            }
            """;

    private static Descriptor order;
    private static Descriptor ticket;

    private final SchemaMerger merger = new SchemaMerger();
    private final MessageJoiner joiner = new MessageJoiner();

    @BeforeAll
    static void compile() throws Exception {
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add("shop/v1/order.proto", ORDER_PROTO, "test")
                .add("support/v1/ticket.proto", TICKET_PROTO, "test")
                .build());
        order = compiled.descriptorFor("shop/v1/order.proto").orElseThrow()
                .findMessageTypeByName("Order");
        ticket = compiled.descriptorFor("support/v1/ticket.proto").orElseThrow()
                .findMessageTypeByName("Ticket");
    }

    private static List<ShapeSynthesizer.NamedType> sources() {
        return List.of(new ShapeSynthesizer.NamedType("order", order),
                new ShapeSynthesizer.NamedType("ticket", ticket));
    }

    @Test
    void validationReportsClashesWithoutEmitting() {
        SchemaMerger.MergeResult result = merger.merge("derived.v1.Case", sources(), Map.of());
        assertThat(result.resolved()).isFalse();
        assertThat(result.shape()).isNull();
        assertThat(result.clashes()).hasSize(3);

        var byField = result.clashes().stream()
                .collect(java.util.stream.Collectors.toMap(
                        SchemaMerger.Clash::field, c -> c));
        // Same name, same type: the natural join key, reported as info.
        assertThat(byField.get("id").kind()).isEqualTo(SchemaMerger.ClashKind.COALESCED);
        assertThat(byField.get("tags").kind()).isEqualTo(SchemaMerger.ClashKind.COALESCED);
        // string vs enum: a hard clash, rename suggested with source prefixes.
        var status = byField.get("status");
        assertThat(status.kind()).isEqualTo(SchemaMerger.ClashKind.TYPE_CLASH);
        assertThat(status.suggested().action()).isEqualTo("rename");
        assertThat(status.suggested().names())
                .containsEntry("order", "order_status")
                .containsEntry("ticket", "ticket_status");
        assertThat(status.origins()).extracting(SchemaMerger.Origin::display)
                .containsExactly("string", "support.v1.Status");
    }

    @Test
    void resolvedMergeEmitsShapeAndBothRulesets() {
        SchemaMerger.MergeResult result = merger.merge("derived.v1.Case", sources(),
                Map.of("status", new SchemaMerger.Resolution("rename", null, Map.of())));
        assertThat(result.resolved()).isTrue();
        Descriptor type = result.shape().type();
        // Coalesced once, renamed per source, singles carried; declaration order stable.
        assertThat(type.getFields()).extracting(FieldDescriptor::getName).containsExactly(
                "id", "qty", "order_status", "tags", "ticket_status", "assignee");
        assertThat(type.findFieldByName("ticket_status").getEnumType().getFullName())
                .isEqualTo("support.v1.Status");
        assertThat(result.shape().protoSource())
                .contains("import \"support/v1/ticket.proto\";")
                .contains("support.v1.Status ticket_status");

        // The defined join: one ruleset over both sources; coalesced repeated appends.
        assertThat(result.shape().impliedRules()).contains(
                "id = order.id", "id = ticket.id",
                "tags = order.tags", "tags += ticket.tags",
                "order_status = order.status", "ticket_status = ticket.status");
        // The defined union: each source maps alone onto the merged shape, in shape order.
        assertThat(result.unionRules().get("ticket")).containsExactly(
                "id = ticket.id", "tags = ticket.tags",
                "ticket_status = ticket.status", "assignee = ticket.assignee");
    }

    @Test
    void mergedShapeJoinsAndUnionsRealMessages() throws Exception {
        SchemaMerger.MergeResult result = merger.merge("derived.v1.Case", sources(),
                Map.of("status", new SchemaMerger.Resolution("rename", null, Map.of())));

        DynamicMessage orderMsg = DynamicMessage.newBuilder(order)
                .setField(order.findFieldByName("id"), "o-1")
                .setField(order.findFieldByName("qty"), 3L)
                .setField(order.findFieldByName("status"), "shipped")
                .build();
        DynamicMessage ticketMsg = DynamicMessage.newBuilder(ticket)
                .setField(ticket.findFieldByName("id"), "t-7")
                .setField(ticket.findFieldByName("assignee"), "Pat")
                .build();

        // Join: both sources at once; coalesced 'id' takes the later source's value.
        MessageScope both = MessageScope.builder()
                .add("order", orderMsg).add("ticket", ticketMsg).build();
        DynamicMessage joined = joiner.join(result.shape(), both, List.of(), List.of());
        Descriptor type = result.shape().type();
        assertThat(joined.getField(type.findFieldByName("id"))).isEqualTo("t-7");
        assertThat(joined.getField(type.findFieldByName("order_status"))).isEqualTo("shipped");
        assertThat(joined.getField(type.findFieldByName("assignee"))).isEqualTo("Pat");

        // Union: one source alone onto the same shape.
        DynamicMessage unioned = joiner.join(type,
                MessageScope.builder().add("ticket", ticketMsg).build(),
                result.unionRules().get("ticket"), List.of());
        assertThat(unioned.getField(type.findFieldByName("id"))).isEqualTo("t-7");
        assertThat(unioned.getField(type.findFieldByName("order_status"))).isEqualTo("");
    }

    @Test
    void preferKeepsOneSourcesField() {
        SchemaMerger.MergeResult result = merger.merge("derived.v1.Case", sources(),
                Map.of("status", new SchemaMerger.Resolution("prefer", "ticket", Map.of())));
        Descriptor type = result.shape().type();
        assertThat(type.findFieldByName("status").getEnumType().getFullName())
                .isEqualTo("support.v1.Status");
        assertThat(result.shape().impliedRules())
                .contains("status = ticket.status")
                .doesNotContain("status = order.status");
        assertThat(result.unionRules().get("order"))
                .noneMatch(rule -> rule.startsWith("status"));
    }

    @Test
    void badResolutionsFailClearly() {
        assertThatThrownBy(() -> merger.merge("derived.v1.Case", sources(),
                Map.of("status", new SchemaMerger.Resolution("coalesce", null, Map.of()))))
                .hasMessageContaining("cannot coalesce");
        assertThatThrownBy(() -> merger.merge("derived.v1.Case", sources(),
                Map.of("status", new SchemaMerger.Resolution("prefer", "invoice", Map.of()))))
                .hasMessageContaining("'source' among the field's contributors");
        assertThatThrownBy(() -> merger.merge("derived.v1.Case", sources(),
                Map.of("qty", new SchemaMerger.Resolution("rename", null, Map.of()))))
                .hasMessageContaining("does not clash");
        assertThatThrownBy(() -> merger.merge("derived.v1.Case", sources(),
                Map.of("status", new SchemaMerger.Resolution("rename", null,
                        Map.of("order", "id")))))
                .hasMessageContaining("collides");
    }
}
