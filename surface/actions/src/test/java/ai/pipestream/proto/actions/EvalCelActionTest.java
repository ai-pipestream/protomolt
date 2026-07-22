package ai.pipestream.proto.actions;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import static ai.pipestream.proto.actions.TestFixtures.obj;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EvalCelActionTest {

    private final ActionCatalog catalog = ActionCatalog.defaults(TestFixtures.personContext());

    private static ObjectNode inlineDocInput(String message, String expression) {
        ObjectNode input = obj("""
                {"schema": {"sources": {}}, "message": %s, "expression": ""}
                """.formatted(message));
        ((ObjectNode) input.get("schema").get("sources")).put("doc.proto", TestFixtures.DOC_PROTO);
        input.put("expression", expression);
        return input;
    }

    @Test
    void evaluatesOverAStructTypedMessage() throws Exception {
        ObjectNode result = catalog.execute("eval-cel", obj("""
                {"schema": {"type": "google.protobuf.Struct"},
                 "message": {"a": 1.5, "b": "x"},
                 "expression": "input['a']"}
                """));
        assertThat(result).isEqualTo(obj("{\"result\": 1.5, \"resultType\": \"double\"}"));
    }

    @Test
    void evaluatesStringExpressionOverACompiledInlineMessage() throws Exception {
        ObjectNode result = catalog.execute("eval-cel",
                inlineDocInput("{\"title\": \"hi\"}", "input.title + '!'"));
        assertThat(result).isEqualTo(obj("{\"result\": \"hi!\", \"resultType\": \"string\"}"));
    }

    @Test
    void intBoolListAndMessageResultTypes() throws Exception {
        ObjectNode size = catalog.execute("eval-cel",
                inlineDocInput("{\"title\": \"hi\"}", "size(input.title)"));
        assertThat(size.get("resultType").asText()).isEqualTo("int");
        assertThat(size.get("result").asLong()).isEqualTo(2);
        assertThat(catalog.execute("eval-cel",
                inlineDocInput("{\"title\": \"hi\"}", "input.title == 'hi'")))
                .isEqualTo(obj("{\"result\": true, \"resultType\": \"bool\"}"));
        ObjectNode list = catalog.execute("eval-cel", inlineDocInput("{}", "[1, 2, 3]"));
        assertThat(list.get("resultType").asText()).isEqualTo("list");
        assertThat(list.get("result")).extracting(com.fasterxml.jackson.databind.JsonNode::asLong)
                .containsExactly(1L, 2L, 3L);
        ObjectNode message = catalog.execute("eval-cel",
                inlineDocInput("{\"title\": \"hi\"}", "input"));
        assertThat(message.get("resultType").asText()).isEqualTo("message:t.Doc");
        assertThat(message.get("result").get("title").asText()).isEqualTo("hi");
    }

    @Test
    void nonCompilingExpressionIsInvalidExpression() {
        ObjectNode input = inlineDocInput("{\"title\": \"hi\"}", "input.no_such_field");
        assertThatThrownBy(() -> catalog.execute("eval-cel", input))
                .isInstanceOfSatisfying(ActionException.class, e -> {
                    assertThat(e.code()).isEqualTo("invalid-expression");
                    assertThat(e.details().orElseThrow().get("detail").asText()).isNotBlank();
                });
    }

    @Test
    void runtimeFailureIsEvaluationFailed() {
        ObjectNode input = inlineDocInput("{}", "1 / 0");
        assertThatThrownBy(() -> catalog.execute("eval-cel", input))
                .isInstanceOfSatisfying(ActionException.class, e -> {
                    assertThat(e.code()).isEqualTo("evaluation-failed");
                    assertThat(e.details().orElseThrow().get("expression").asText()).isEqualTo("1 / 0");
                });
    }

    @Test
    void malformedMessageIsInvalidMessage() {
        ObjectNode input = inlineDocInput("{\"title\": {\"not\": \"a string\"}}", "input.title");
        assertThatThrownBy(() -> catalog.execute("eval-cel", input))
                .isInstanceOfSatisfying(ActionException.class,
                        e -> assertThat(e.code()).isEqualTo("invalid-message"));
    }

    @Test
    void missingExpressionIsInvalidInputWithPointer() {
        ObjectNode input = obj("""
                {"schema": {"type": "google.protobuf.Struct"}, "message": {}}
                """);
        assertThatThrownBy(() -> catalog.execute("eval-cel", input))
                .isInstanceOfSatisfying(ActionException.class, e -> {
                    assertThat(e.code()).isEqualTo("invalid-input");
                    assertThat(e.details().orElseThrow().get("pointer").asText())
                            .isEqualTo("/expression");
                });
    }
}
