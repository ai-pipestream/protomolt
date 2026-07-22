package ai.pipestream.proto.actions;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import static ai.pipestream.proto.actions.TestFixtures.obj;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The shape verbs end to end: synthesized projections, envelopes, and unions; joins into
 * synthesized and authored targets with scoped text and CEL rules.
 */
class ShapeActionsTest {

    private static final String ORDER_PROTO = """
            syntax = "proto3";
            package shop.v1;
            message Order {
              string id = 1;
              int64 qty = 2;
            }
            """;

    private static final String CUSTOMER_PROTO = """
            syntax = "proto3";
            package crm.v1;
            message Customer {
              string id = 1;
              string name = 2;
            }
            """;

    private final ActionCatalog catalog = ActionCatalog.defaults(TestFixtures.personContext());

    private static ObjectNode sourcesInput(String extraJson) {
        ObjectNode input = obj("""
                {"sources": [
                   {"name": "order", "schema": {"sources": {}}, "type": "shop.v1.Order",
                    "message": {"id": "o-1", "qty": "3"}},
                   {"name": "customer", "schema": {"sources": {}}, "type": "crm.v1.Customer",
                    "message": {"id": "c-9", "name": "Pat"}}
                 ]%s}
                """.formatted(extraJson.isEmpty() ? "" : ", " + extraJson));
        ((ObjectNode) input.get("sources").get(0).get("schema").get("sources"))
                .put("shop/v1/order.proto", ORDER_PROTO);
        ((ObjectNode) input.get("sources").get(1).get("schema").get("sources"))
                .put("crm/v1/customer.proto", CUSTOMER_PROTO);
        return input;
    }

    @Test
    void synthesizesAProjectionWithImpliedRules() throws Exception {
        ObjectNode input = sourcesInput("""
                "mode": "projection", "name": "derived.v1.Summary",
                "fields": [{"name": "order_id", "from": "order.id"},
                           {"name": "customer_name", "from": "customer.name"}]
                """);
        ObjectNode result = catalog.execute("synthesize-shape", input);
        assertThat(result.get("type").asText()).isEqualTo("derived.v1.Summary");
        assertThat(result.get("file").asText()).isEqualTo("derived/v1/summary.proto");
        // A scalar-only projection depends on nothing: its source has no imports.
        assertThat(result.get("protoSource").asText())
                .doesNotContain("import ")
                .contains("string order_id = 1;")
                .contains("string customer_name = 2;");
        assertThat(result.get("descriptorSetBase64").asText()).isNotEmpty();
        assertThat(result.get("impliedRules"))
                .extracting(n -> n.asText())
                .containsExactly("order_id = order.id", "customer_name = customer.name");
    }

    @Test
    void synthesizesEnvelopeAndUnion() throws Exception {
        ObjectNode envelope = catalog.execute("synthesize-shape",
                sourcesInput("\"mode\": \"envelope\", \"name\": \"derived.v1.Pair\""));
        assertThat(envelope.get("protoSource").asText())
                .contains("shop.v1.Order order = 1;")
                .contains("crm.v1.Customer customer = 2;");

        ObjectNode union = catalog.execute("synthesize-shape",
                sourcesInput("\"mode\": \"union\", \"name\": \"derived.v1.Either\""));
        assertThat(union.get("protoSource").asText()).contains("oneof value {");
    }

    @Test
    void joinsIntoASynthesizedShapeWithNoRules() throws Exception {
        ObjectNode input = sourcesInput("""
                "shape": {"mode": "projection", "name": "derived.v1.Summary",
                          "fields": [{"name": "order_id", "from": "order.id"},
                                     {"name": "customer_name", "from": "customer.name"}]}
                """);
        ObjectNode result = catalog.execute("join-messages", input);
        assertThat(result.get("type").asText()).isEqualTo("derived.v1.Summary");
        assertThat(result.get("message").get("orderId").asText()).isEqualTo("o-1");
        assertThat(result.get("message").get("customerName").asText()).isEqualTo("Pat");
        assertThat(result.get("descriptorSetBase64").asText()).isNotEmpty();
    }

    @Test
    void joinsIntoAnAuthoredTargetWithScopedAndCelRules() throws Exception {
        ObjectNode input = sourcesInput("""
                "target": {"schema": {"sources": {}}, "type": "shop.v1.Order"},
                "rules": ["id = customer.id"],
                "celRules": [{"selector": "order.qty * 2", "target": "qty"}]
                """);
        ((ObjectNode) input.get("target").get("schema").get("sources"))
                .put("shop/v1/order.proto", ORDER_PROTO);
        ObjectNode result = catalog.execute("join-messages", input);
        assertThat(result.get("type").asText()).isEqualTo("shop.v1.Order");
        assertThat(result.get("message").get("id").asText()).isEqualTo("c-9");
        assertThat(result.get("message").get("qty").asText()).isEqualTo("6");
    }

    @Test
    void badInputsSurfaceAsTypedErrors() {
        assertThatThrownBy(() -> catalog.execute("join-messages", sourcesInput("")))
                .isInstanceOf(ActionException.class)
                .hasMessageContaining("'target' or 'shape'");
        assertThatThrownBy(() -> catalog.execute("synthesize-shape", sourcesInput("""
                "mode": "projection", "name": "derived.v1.Bad",
                "fields": [{"name": "x", "from": "order.nope"}]
                """)))
                .isInstanceOf(ActionException.class)
                .hasMessageContaining("No field 'nope'");
        assertThatThrownBy(() -> catalog.execute("synthesize-shape",
                sourcesInput("\"mode\": \"sideways\", \"name\": \"derived.v1.Bad\"")))
                .isInstanceOf(ActionException.class)
                .hasMessageContaining("sideways");
    }
}
