package ai.pipestream.proto.actions;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import static ai.pipestream.proto.actions.TestFixtures.obj;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The check-rules verb in both dialects: static findings, and the dry run over sample
 * messages with mapped output and filter verdicts.
 */
class CheckRulesActionTest {

    private static final String ORDER_PROTO = """
            syntax = "proto3";
            package shop.v1;
            import "google/protobuf/struct.proto";
            message Order {
              string id = 1;
              int64 qty = 2;
              string note = 3;
              google.protobuf.Struct extras = 4;
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

    private static ObjectNode singleSource(String extraJson) {
        ObjectNode input = obj("""
                {"sources": [{"name": "input", "schema": {"sources": {}},
                              "type": "shop.v1.Order",
                              "message": {"id": "o-1", "qty": "3"}}]%s}
                """.formatted(extraJson.isEmpty() ? "" : ", " + extraJson));
        ((ObjectNode) input.get("sources").get(0).get("schema").get("sources"))
                .put("shop/v1/order.proto", ORDER_PROTO);
        return input;
    }

    @Test
    void inPlaceModeChecksAndDryRuns() throws Exception {
        ObjectNode result = catalog.execute("check-rules", singleSource("""
                "rules": ["note = id"],
                "celRules": [{"selector": "input.qty * 2", "target": "qty"}],
                "filters": ["input.qty > 1"]
                """));
        assertThat(result.get("ok").asBoolean()).isTrue();
        assertThat(result.get("findings")).isEmpty();
        assertThat(result.get("message").get("note").asText()).isEqualTo("o-1");
        assertThat(result.get("message").get("qty").asText()).isEqualTo("6");
        assertThat(result.get("filterResults").get(0).asBoolean()).isTrue();
    }

    @Test
    void staticFindingsComeBackTyped() throws Exception {
        ObjectNode result = catalog.execute("check-rules", singleSource("""
                "rules": ["nope = id"],
                "filters": ["input.qty + 1"]
                """));
        assertThat(result.get("ok").asBoolean()).isFalse();
        JsonNode findings = result.get("findings");
        assertThat(findings).hasSize(2);
        assertThat(findings.get(0).get("kind").asText()).isEqualTo("rule");
        assertThat(findings.get(0).get("error").asText()).contains("no field 'nope'");
        assertThat(findings.get(1).get("kind").asText()).isEqualTo("filter");
        assertThat(findings.get(1).get("error").asText()).contains("must be a boolean");
        // No dry run on a broken ruleset.
        assertThat(result.has("message")).isFalse();
    }

    @Test
    void scopedModeChecksAgainstATarget() throws Exception {
        ObjectNode input = obj("""
                {"sources": [
                   {"name": "order", "schema": {"sources": {}}, "type": "shop.v1.Order",
                    "message": {"id": "o-1", "qty": "3"}},
                   {"name": "customer", "schema": {"sources": {}}, "type": "crm.v1.Customer",
                    "message": {"id": "c-9", "name": "Pat"}}
                 ],
                 "target": {"schema": {"sources": {}}, "type": "shop.v1.Order"},
                 "rules": ["id = customer.id", "note = customer.name"],
                 "filters": ["order.qty > 0 && customer.name != ''"]}
                """);
        ((ObjectNode) input.get("sources").get(0).get("schema").get("sources"))
                .put("shop/v1/order.proto", ORDER_PROTO);
        ((ObjectNode) input.get("sources").get(1).get("schema").get("sources"))
                .put("crm/v1/customer.proto", CUSTOMER_PROTO);
        ((ObjectNode) input.get("target").get("schema").get("sources"))
                .put("shop/v1/order.proto", ORDER_PROTO);
        ObjectNode result = catalog.execute("check-rules", input);
        assertThat(result.get("ok").asBoolean()).isTrue();
        assertThat(result.get("message").get("id").asText()).isEqualTo("c-9");
        assertThat(result.get("message").get("note").asText()).isEqualTo("Pat");
        assertThat(result.get("filterResults").get(0).asBoolean()).isTrue();
    }

    @Test
    void structPathsPassStaticallyAndMapForRealInTheDryRun() throws Exception {
        // Struct keys are undeclared, so the static pass reports nothing — and the
        // runtime mapper genuinely writes and reads through them, proven by the dry run.
        ObjectNode result = catalog.execute("check-rules", singleSource("""
                "rules": ["extras.warehouse = id", "note = extras.warehouse"]
                """));
        assertThat(result.get("ok").asBoolean()).isTrue();
        assertThat(result.get("findings")).isEmpty();
        assertThat(result.get("message").get("extras").get("warehouse").asText())
                .isEqualTo("o-1");
        assertThat(result.get("message").get("note").asText()).isEqualTo("o-1");
    }

    @Test
    void staticOnlyWhenNoSampleMessages() throws Exception {
        ObjectNode input = singleSource("\"rules\": [\"note = id\"]");
        ((ObjectNode) input.get("sources").get(0)).remove("message");
        ObjectNode result = catalog.execute("check-rules", input);
        assertThat(result.get("ok").asBoolean()).isTrue();
        assertThat(result.has("message")).isFalse();
        assertThat(result.has("filterResults")).isFalse();
    }
}
