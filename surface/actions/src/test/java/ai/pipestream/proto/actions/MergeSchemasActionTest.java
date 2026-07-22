package ai.pipestream.proto.actions;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import static ai.pipestream.proto.actions.TestFixtures.obj;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The merge-schemas verb: validate (clash report, no emission), resolve, emit — and the
 * emitted rulesets drive join-messages directly (the free mappings).
 */
class MergeSchemasActionTest {

    private static final String ORDER_PROTO = """
            syntax = "proto3";
            package shop.v1;
            message Order {
              string id = 1;
              string status = 2;
              int64 qty = 3;
            }
            """;

    private static final String TICKET_PROTO = """
            syntax = "proto3";
            package support.v1;
            message Ticket {
              string id = 1;
              int32 status = 2;
              string assignee = 3;
            }
            """;

    private final ActionCatalog catalog = ActionCatalog.defaults(TestFixtures.personContext());

    private static ObjectNode mergeInput(String extraJson) {
        ObjectNode input = obj("""
                {"name": "derived.v1.Case",
                 "sources": [
                   {"name": "order", "schema": {"sources": {}}, "type": "shop.v1.Order"},
                   {"name": "ticket", "schema": {"sources": {}}, "type": "support.v1.Ticket"}
                 ]%s}
                """.formatted(extraJson.isEmpty() ? "" : ", " + extraJson));
        ((ObjectNode) input.get("sources").get(0).get("schema").get("sources"))
                .put("shop/v1/order.proto", ORDER_PROTO);
        ((ObjectNode) input.get("sources").get(1).get("schema").get("sources"))
                .put("support/v1/ticket.proto", TICKET_PROTO);
        return input;
    }

    @Test
    void unresolvedClashesReportWithoutEmitting() throws Exception {
        ObjectNode result = catalog.execute("merge-schemas", mergeInput(""));
        assertThat(result.get("resolved").asBoolean()).isFalse();
        assertThat(result.has("protoSource")).isFalse();
        JsonNode clashes = result.get("clashes");
        assertThat(clashes.findValuesAsText("field")).containsExactly("id", "status");
        JsonNode status = clashes.get(1);
        assertThat(status.get("kind").asText()).isEqualTo("type-clash");
        assertThat(status.get("suggested").get("action").asText()).isEqualTo("rename");
        assertThat(status.get("suggested").get("names").get("order").asText())
                .isEqualTo("order_status");
        assertThat(status.get("origins").findValuesAsText("type"))
                .containsExactly("string", "int32");
    }

    @Test
    void reportOnlySkipsEmissionEvenWhenResolved() throws Exception {
        ObjectNode input = mergeInput("""
                "resolutions": {"status": {"action": "rename"}}, "reportOnly": true
                """);
        ObjectNode result = catalog.execute("merge-schemas", input);
        assertThat(result.get("resolved").asBoolean()).isFalse();
        assertThat(result.has("protoSource")).isFalse();
        assertThat(result.get("clashes").size()).isEqualTo(2);
    }

    @Test
    void resolvedMergeEmitsShapeAndRulesInOneMove() throws Exception {
        ObjectNode input = mergeInput("""
                "resolutions": {"status": {"action": "rename",
                                           "names": {"ticket": "ticket_code"}}}
                """);
        ObjectNode result = catalog.execute("merge-schemas", input);
        assertThat(result.get("resolved").asBoolean()).isTrue();
        assertThat(result.get("type").asText()).isEqualTo("derived.v1.Case");
        assertThat(result.get("protoSource").asText())
                .contains("string order_status")
                .contains("int32 ticket_code")
                .contains("string assignee");
        assertThat(result.get("joinRules")).extracting(JsonNode::asText)
                .contains("id = order.id", "id = ticket.id",
                        "order_status = order.status", "ticket_code = ticket.status");
        assertThat(result.get("unionRules").get("ticket").get("rules"))
                .extracting(JsonNode::asText)
                .contains("id = ticket.id", "ticket_code = ticket.status");
        assertThat(result.get("descriptorSetBase64").asText()).isNotEmpty();
    }

    @Test
    void mergedShapeFeedsJoinMessagesDirectly() throws Exception {
        ObjectNode merged = catalog.execute("merge-schemas", mergeInput("""
                "resolutions": {"status": {"action": "prefer", "source": "order"}}
                """));
        // The free mappings: joined via the merged descriptor set and join rules verbatim.
        ObjectNode join = mergeInput("");
        ((ObjectNode) join.get("sources").get(0))
                .set("message", obj("{\"id\": \"o-1\", \"status\": \"open\", \"qty\": \"2\"}"));
        ((ObjectNode) join.get("sources").get(1))
                .set("message", obj("{\"id\": \"t-9\", \"assignee\": \"Sam\"}"));
        join.remove("name");
        join.putObject("target")
                .put("type", "derived.v1.Case")
                .putObject("schema")
                .put("descriptorSetBase64", merged.get("descriptorSetBase64").asText());
        join.set("rules", merged.get("joinRules").deepCopy());
        ObjectNode result = catalog.execute("join-messages", join);
        assertThat(result.get("message").get("id").asText()).isEqualTo("t-9");
        assertThat(result.get("message").get("status").asText()).isEqualTo("open");
        assertThat(result.get("message").get("assignee").asText()).isEqualTo("Sam");
    }

    @Test
    void badResolutionsAreInvalidInput() {
        assertThatThrownBy(() -> catalog.execute("merge-schemas", mergeInput("""
                "resolutions": {"status": {"action": "prefer"}}
                """)))
                .isInstanceOf(ActionException.class)
                .hasMessageContaining("'source'");
        assertThatThrownBy(() -> catalog.execute("merge-schemas", mergeInput("""
                "resolutions": {"id": {"action": "sideways"}}
                """)))
                .isInstanceOf(ActionException.class)
                .hasMessageContaining("sideways");
    }
}
