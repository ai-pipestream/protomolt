package ai.pipestream.proto.actions;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import static ai.pipestream.proto.actions.TestFixtures.obj;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MapMessageActionTest {

    private final ActionCatalog catalog = ActionCatalog.defaults(TestFixtures.personContext());

    private static ObjectNode inlineDocInput(String message) {
        ObjectNode input = obj("""
                {"schema": {"sources": {}}, "message": %s}
                """.formatted(message));
        ((ObjectNode) input.get("schema").get("sources")).put("doc.proto", TestFixtures.DOC_PROTO);
        return input;
    }

    @Test
    void textRulesCopyAndClearFields() throws Exception {
        ObjectNode input = inlineDocInput("{\"title\": \"old\", \"alt\": \"hello\"}");
        input.putArray("rules").add("title = alt").add("- alt");
        ObjectNode result = catalog.execute("map-message", input);
        assertThat(result).isEqualTo(obj(
                "{\"message\": {\"title\": \"hello\", \"alt\": \"\"}}"));
    }

    @Test
    void celRuleWithFilterAndSelectorApplies() throws Exception {
        ObjectNode input = inlineDocInput("{\"title\": \"hi\", \"alt\": \"x\"}");
        input.set("celRules", obj("""
                {"celRules": [{"filter": "input.title != ''",
                               "selector": "input.title + '!'",
                               "target": "title"}]}
                """).get("celRules"));
        ObjectNode result = catalog.execute("map-message", input);
        assertThat(result.get("message").get("title").asText()).isEqualTo("hi!");
    }

    @Test
    void celRuleWithFalseFilterIsSkipped() throws Exception {
        ObjectNode input = inlineDocInput("{\"title\": \"hi\"}");
        input.set("celRules", obj("""
                {"celRules": [{"filter": "input.title == 'other'",
                               "selector": "'never'",
                               "target": "title"}]}
                """).get("celRules"));
        ObjectNode result = catalog.execute("map-message", input);
        assertThat(result.get("message").get("title").asText()).isEqualTo("hi");
    }

    @Test
    void celRuleFallbackTextRulesApplyWhenNoSelector() throws Exception {
        ObjectNode input = inlineDocInput("{\"alt\": \"fallback\"}");
        input.set("celRules", obj("""
                {"celRules": [{"target": "title", "fallback": ["title = alt"]}]}
                """).get("celRules"));
        ObjectNode result = catalog.execute("map-message", input);
        assertThat(result.get("message").get("title").asText()).isEqualTo("fallback");
    }

    @Test
    void textAndCelRulesComposeInOrder() throws Exception {
        ObjectNode input = inlineDocInput("{\"alt\": \"hello\"}");
        input.putArray("rules").add("title = alt");
        input.set("celRules", obj("""
                {"celRules": [{"selector": "input.title + '!'", "target": "title"}]}
                """).get("celRules"));
        ObjectNode result = catalog.execute("map-message", input);
        assertThat(result.get("message").get("title").asText()).isEqualTo("hello!");
    }

    @Test
    void mapsAStructTypedMessageWithACelSelector() throws Exception {
        ObjectNode input = obj("""
                {"schema": {"type": "google.protobuf.Struct"},
                 "message": {"a": 1.5, "b": "x"},
                 "celRules": [{"selector": "input['a'] + 1.0", "target": "c"}]}
                """);
        ObjectNode result = catalog.execute("map-message", input);
        assertThat(result).isEqualTo(obj(
                "{\"message\": {\"a\": 1.5, \"b\": \"x\", \"c\": 2.5}}"));
    }

    @Test
    void invalidTextRuleSyntaxIsMappingFailed() {
        ObjectNode input = inlineDocInput("{\"title\": \"hi\"}");
        input.putArray("rules").add("this is not a rule ???");
        assertThatThrownBy(() -> catalog.execute("map-message", input))
                .isInstanceOfSatisfying(ActionException.class, e -> {
                    assertThat(e.code()).isEqualTo("mapping-failed");
                    assertThat(e.details().orElseThrow().get("detail").asText()).isNotBlank();
                });
    }

    @Test
    void unresolvablePathIsMappingFailed() {
        ObjectNode input = inlineDocInput("{\"title\": \"hi\"}");
        input.putArray("rules").add("title = no_such_field");
        assertThatThrownBy(() -> catalog.execute("map-message", input))
                .isInstanceOfSatisfying(ActionException.class,
                        e -> assertThat(e.code()).isEqualTo("mapping-failed"));
    }

    @Test
    void nonCompilingCelSelectorIsInvalidExpression() {
        ObjectNode input = inlineDocInput("{\"title\": \"hi\"}");
        input.set("celRules", obj("""
                {"celRules": [{"selector": "input.no_such_field", "target": "title"}]}
                """).get("celRules"));
        assertThatThrownBy(() -> catalog.execute("map-message", input))
                .isInstanceOfSatisfying(ActionException.class,
                        e -> assertThat(e.code()).isEqualTo("invalid-expression"));
    }

    @Test
    void missingRulesAndCelRulesIsInvalidInput() {
        ObjectNode input = inlineDocInput("{\"title\": \"hi\"}");
        assertThatThrownBy(() -> catalog.execute("map-message", input))
                .isInstanceOfSatisfying(ActionException.class, e -> {
                    assertThat(e.code()).isEqualTo("invalid-input");
                    assertThat(e.getMessage()).contains("rules");
                });
    }

    @Test
    void celRuleWithoutTargetIsInvalidInputWithPointer() {
        ObjectNode input = inlineDocInput("{\"title\": \"hi\"}");
        input.set("celRules", obj("{\"celRules\": [{\"selector\": \"'x'\"}]}").get("celRules"));
        assertThatThrownBy(() -> catalog.execute("map-message", input))
                .isInstanceOfSatisfying(ActionException.class, e -> {
                    assertThat(e.code()).isEqualTo("invalid-input");
                    assertThat(e.details().orElseThrow().get("pointer").asText())
                            .isEqualTo("/celRules/0/target");
                });
    }
}
