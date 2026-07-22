package ai.pipestream.proto.actions;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import static ai.pipestream.proto.actions.TestFixtures.obj;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ValidateMessageActionTest {

    private final ActionCatalog catalog = ActionCatalog.defaults(TestFixtures.personContext());

    @Test
    void validMessageProducesExactCleanResult() throws Exception {
        ObjectNode result = catalog.execute("validate-message", obj("""
                {"schema": {"type": "actions.test.Person"},
                 "message": {"name": "Joseph", "age": 42, "nickname": "Joe"}}
                """));
        assertThat(result).isEqualTo(obj("{\"valid\": true, \"violations\": []}"));
    }

    @Test
    void violationsCarryFieldRuleIdAndMessage() throws Exception {
        ObjectNode result = catalog.execute("validate-message", obj("""
                {"schema": {"type": "actions.test.Person"},
                 "message": {"name": "Jo", "age": -1}}
                """));
        assertThat(result.get("valid").asBoolean()).isFalse();
        assertThat(result.get("violations")).hasSize(2);
        ObjectNode first = (ObjectNode) result.get("violations").get(0);
        assertThat(first.get("field").asText()).isEqualTo("name");
        assertThat(first.get("ruleId").asText()).isEqualTo("string.min_len");
        assertThat(first.get("message").asText()).isEqualTo("length must be at least 3");
        assertThat(result.get("violations").findValuesAsText("ruleId"))
                .containsExactlyInAnyOrder("string.min_len", "int32.gte");
        assertThat(result.get("violations").findValuesAsText("field"))
                .containsExactlyInAnyOrder("name", "age");
    }

    @Test
    void malformedMessageJsonIsInvalidMessage() {
        ObjectNode input = obj("""
                {"schema": {"type": "actions.test.Person"},
                 "message": {"name": "Joseph", "age": "not-a-number"}}
                """);
        assertThatThrownBy(() -> catalog.execute("validate-message", input))
                .isInstanceOfSatisfying(ActionException.class, e -> {
                    assertThat(e.code()).isEqualTo("invalid-message");
                    assertThat(e.details().orElseThrow().get("detail").asText()).isNotBlank();
                    assertThat(e.details().orElseThrow().get("pointer").asText())
                            .isEqualTo("/message");
                });
    }

    @Test
    void missingMessageFieldIsInvalidInputWithPointer() {
        assertThatThrownBy(() -> catalog.execute("validate-message",
                obj("{\"schema\": {\"type\": \"actions.test.Person\"}}")))
                .isInstanceOfSatisfying(ActionException.class, e -> {
                    assertThat(e.code()).isEqualTo("invalid-input");
                    assertThat(e.details().orElseThrow().get("pointer").asText())
                            .isEqualTo("/message");
                });
    }

    @Test
    void missingSchemaFieldIsInvalidInputWithPointer() {
        assertThatThrownBy(() -> catalog.execute("validate-message", obj("{\"message\": {}}")))
                .isInstanceOfSatisfying(ActionException.class, e -> {
                    assertThat(e.code()).isEqualTo("invalid-input");
                    assertThat(e.details().orElseThrow().get("pointer").asText())
                            .isEqualTo("/schema");
                });
    }

    @Test
    void unknownTypeInSchemaIsUnknownType() {
        assertThatThrownBy(() -> catalog.execute("validate-message",
                obj("{\"schema\": {\"type\": \"actions.test.Nobody\"}, \"message\": {}}")))
                .isInstanceOfSatisfying(ActionException.class,
                        e -> assertThat(e.code()).isEqualTo("unknown-type"));
    }

    @Test
    void inlineSourcesWithValidationOptionsAreEnforced() throws Exception {
        ObjectNode input = obj("""
                {"schema": {"sources": {}, "root": "person.proto"},
                 "message": {"name": "Jo"}}
                """);
        ObjectNode sources = (ObjectNode) input.get("schema").get("sources");
        sources.put("person.proto", """
                syntax = "proto3";
                package inline.test;
                import "ai/pipestream/proto/validate/v1/validate.proto";
                message Person {
                  string name = 1 [(ai.pipestream.proto.validate.v1.field).string.min_len = 3];
                }
                """);
        sources.put("ai/pipestream/proto/validate/v1/validate.proto",
                TestFixtures.validateProtoSource());
        ObjectNode result = catalog.execute("validate-message", input);
        assertThat(result.get("valid").asBoolean()).isFalse();
        assertThat(result.get("violations").get(0).get("ruleId").asText())
                .isEqualTo("string.min_len");
    }
}
