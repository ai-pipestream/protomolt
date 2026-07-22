package ai.pipestream.proto.actions;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import static ai.pipestream.proto.actions.TestFixtures.obj;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DiffSchemasActionTest {

    private final ActionCatalog catalog = ActionCatalog.defaults(ActionContext.create());

    private static ObjectNode diffInput(String oldProto, String newProto) {
        ObjectNode input = obj("""
                {"old": {"sources": {}}, "new": {"sources": {}}}
                """);
        ((ObjectNode) input.get("old").get("sources")).put("doc.proto", oldProto);
        ((ObjectNode) input.get("new").get("sources")).put("doc.proto", newProto);
        return input;
    }

    @Test
    void addedFieldIsAnInformationalChange() throws Exception {
        ObjectNode result = catalog.execute("diff-schemas", diffInput(
                "syntax = \"proto3\"; package ex; message Doc { string title = 1; }",
                "syntax = \"proto3\"; package ex; message Doc { string title = 1; string email = 2; }"));
        assertThat(result.get("changes")).hasSize(1);
        JsonNode change = result.get("changes").get(0);
        assertThat(change.get("ruleId").asText()).isEqualTo("FIELD_ADDED");
        assertThat(change.get("path").asText()).isEqualTo("ex.Doc.email");
        assertThat(change.get("before").asText()).isEmpty();
        assertThat(change.get("after").asText()).isNotBlank();
        assertThat(change.get("message").asText()).isNotBlank();
        assertThat(change.get("impacts")).isEmpty();
    }

    @Test
    void typeChangeCarriesWireImpacts() throws Exception {
        ObjectNode result = catalog.execute("diff-schemas", diffInput(
                "syntax = \"proto3\"; package ex; message Doc { int32 count = 1; }",
                "syntax = \"proto3\"; package ex; message Doc { string count = 1; }"));
        JsonNode change = result.get("changes").get(0);
        assertThat(change.get("ruleId").asText()).isEqualTo("FIELD_TYPE_CHANGED");
        assertThat(change.get("path").asText()).isEqualTo("ex.Doc.count");
        assertThat(change.get("impacts")).extracting(JsonNode::asText)
                .contains("WIRE_BACKWARD", "WIRE_FORWARD");
    }

    @Test
    void identicalSchemasProduceNoChanges() throws Exception {
        String proto = "syntax = \"proto3\"; package ex; message Doc { string title = 1; }";
        ObjectNode result = catalog.execute("diff-schemas", diffInput(proto, proto));
        assertThat(result).isEqualTo(obj("{\"changes\": []}"));
    }

    @Test
    void missingOldSchemaIsInvalidInputWithPointer() {
        ObjectNode input = obj("{\"new\": {\"sources\": {\"a.proto\": \"syntax = \\\"proto3\\\";\"}}}");
        assertThatThrownBy(() -> catalog.execute("diff-schemas", input))
                .isInstanceOfSatisfying(ActionException.class, e -> {
                    assertThat(e.code()).isEqualTo("invalid-input");
                    assertThat(e.details().orElseThrow().get("pointer").asText()).isEqualTo("/old");
                });
    }
}
