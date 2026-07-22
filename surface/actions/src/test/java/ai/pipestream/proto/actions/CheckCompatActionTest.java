package ai.pipestream.proto.actions;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import static ai.pipestream.proto.actions.TestFixtures.obj;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CheckCompatActionTest {

    private static final String OLD = """
            syntax = "proto3";
            package ex;
            message Doc {
              int32 count = 1;
              string note = 2;
            }
            """;

    private final ActionCatalog catalog = ActionCatalog.defaults(ActionContext.create());

    private static ObjectNode compatInput(String oldProto, String newProto) {
        ObjectNode input = obj("""
                {"old": {"sources": {}}, "new": {"sources": {}}}
                """);
        ((ObjectNode) input.get("old").get("sources")).put("doc.proto", oldProto);
        ((ObjectNode) input.get("new").get("sources")).put("doc.proto", newProto);
        return input;
    }

    @Test
    void int32ToStringViolatesBackward() throws Exception {
        ObjectNode input = compatInput(OLD, """
                syntax = "proto3";
                package ex;
                message Doc {
                  string count = 1;
                  string note = 2;
                }
                """);
        ObjectNode result = catalog.execute("check-compat", input);
        assertThat(result.get("compatible").asBoolean()).isFalse();
        assertThat(result.get("mode").asText()).isEqualTo("BACKWARD");
        assertThat(result.get("violations").findValuesAsText("ruleId"))
                .containsExactly("FIELD_TYPE_CHANGED");
        assertThat(result.get("violations").get(0).get("path").asText()).isEqualTo("ex.Doc.count");
    }

    @Test
    void fieldRemovalIsCleanUnderWireOnlyBackward() throws Exception {
        ObjectNode input = compatInput(OLD, """
                syntax = "proto3";
                package ex;
                message Doc {
                  int32 count = 1;
                  reserved 2;
                  reserved "note";
                }
                """);
        ObjectNode result = catalog.execute("check-compat", input);
        assertThat(result.get("compatible").asBoolean()).isTrue();
        assertThat(result.get("violations")).isEmpty();
        // the removal is still reported as a change
        assertThat(result.get("changes").findValuesAsText("ruleId")).contains("FIELD_REMOVED");
    }

    @Test
    void fieldRemovalViolatesBackwardWithJsonRules() throws Exception {
        ObjectNode input = compatInput(OLD, """
                syntax = "proto3";
                package ex;
                message Doc {
                  int32 count = 1;
                  reserved 2;
                  reserved "note";
                }
                """);
        input.put("includeJsonRules", true);
        ObjectNode result = catalog.execute("check-compat", input);
        assertThat(result.get("compatible").asBoolean()).isFalse();
        assertThat(result.get("violations").findValuesAsText("ruleId")).contains("FIELD_REMOVED");
        assertThat(result.get("violations").get(0).get("impacts"))
                .extracting(com.fasterxml.jackson.databind.JsonNode::asText)
                .contains("JSON_BACKWARD");
    }

    @Test
    void explicitModeIsHonored() throws Exception {
        // FIELD_ADDED is informational; under NONE nothing ever violates
        ObjectNode input = compatInput(OLD, """
                syntax = "proto3";
                package ex;
                message Doc {
                  string count = 1;
                  string note = 2;
                }
                """);
        input.put("mode", "NONE");
        ObjectNode result = catalog.execute("check-compat", input);
        assertThat(result.get("compatible").asBoolean()).isTrue();
        assertThat(result.get("mode").asText()).isEqualTo("NONE");
        assertThat(result.get("changes")).isNotEmpty();
    }

    @Test
    void sourceRulesTurnSourceImpactsIntoViolations() throws Exception {
        ObjectNode input = compatInput(OLD, """
                syntax = "proto3";
                package ex;
                message Doc {
                  int32 count = 1;
                  reserved 2;
                  reserved "note";
                }
                """);
        input.put("includeSourceRules", true);
        ObjectNode result = catalog.execute("check-compat", input);
        // FIELD_REMOVED carries SOURCE impact, so it violates once source rules are on
        assertThat(result.get("compatible").asBoolean()).isFalse();
        assertThat(result.get("violations").findValuesAsText("ruleId")).contains("FIELD_REMOVED");
    }

    @Test
    void unknownModeIsInvalidInput() {
        ObjectNode input = compatInput(OLD, OLD);
        input.put("mode", "SIDEWAYS");
        assertThatThrownBy(() -> catalog.execute("check-compat", input))
                .isInstanceOfSatisfying(ActionException.class, e -> {
                    assertThat(e.code()).isEqualTo("invalid-input");
                    assertThat(e.details().orElseThrow().get("pointer").asText()).isEqualTo("/mode");
                    assertThat(e.getMessage()).contains("BACKWARD");
                });
    }
}
