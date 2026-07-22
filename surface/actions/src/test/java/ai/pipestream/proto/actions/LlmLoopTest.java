package ai.pipestream.proto.actions;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import static ai.pipestream.proto.actions.TestFixtures.obj;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The round-trip an LLM drives blind: compile sources, ground itself with list-types, validate a
 * failing message, repair it with map-message, and validate again — every step through the
 * catalog's JSON envelopes, with the compile output feeding every later schema reference.
 */
class LlmLoopTest {

    private static final String PERSON_PROTO = """
            syntax = "proto3";
            package loop;
            import "ai/pipestream/proto/validate/v1/validate.proto";
            message Person {
              string name = 1 [(ai.pipestream.proto.validate.v1.field).string.min_len = 3];
              string fallback_name = 2;
            }
            """;

    @Test
    void compileListValidateMapValidateComposes() throws Exception {
        ActionCatalog catalog = ActionCatalog.defaults(ActionContext.create());

        // 1. compile: sources in, portable descriptor set out
        ObjectNode compileInput = obj("{\"sources\": {}}");
        ObjectNode sources = (ObjectNode) compileInput.get("sources");
        sources.put("loop/person.proto", PERSON_PROTO);
        sources.put("ai/pipestream/proto/validate/v1/validate.proto",
                TestFixtures.validateProtoSource());
        ObjectNode compiled = catalog.execute("compile", compileInput);
        assertThat(compiled.get("ok").asBoolean()).isTrue();
        String descriptorSetBase64 = compiled.get("descriptorSetBase64").asText();

        // 2. list-types: ground the exact type name and field shape
        ObjectNode listInput = schemaInput(descriptorSetBase64);
        ((ObjectNode) listInput).put("filter", "loop.");
        ObjectNode listed = catalog.execute("list-types", listInput);
        JsonNode person = listed.get("types").get(0);
        assertThat(person.get("fullName").asText()).isEqualTo("loop.Person");
        assertThat(person.get("fields").findValuesAsText("name"))
                .containsExactly("name", "fallback_name");

        // 3. validate-message: the candidate message fails string.min_len
        ObjectNode validateInput = schemaInput(descriptorSetBase64);
        validateInput.put("type", "loop.Person");
        validateInput.set("message", obj("{\"name\": \"Jo\", \"fallback_name\": \"Joseph\"}"));
        ObjectNode failing = catalog.execute("validate-message", validateInput);
        assertThat(failing.get("valid").asBoolean()).isFalse();
        assertThat(failing.get("violations").get(0).get("field").asText()).isEqualTo("name");
        assertThat(failing.get("violations").get(0).get("ruleId").asText())
                .isEqualTo("string.min_len");

        // 4. map-message: repair the offending field from the fallback
        ObjectNode mapInput = schemaInput(descriptorSetBase64);
        mapInput.put("type", "loop.Person");
        mapInput.set("message", obj("{\"name\": \"Jo\", \"fallback_name\": \"Joseph\"}"));
        mapInput.set("celRules", obj("""
                {"celRules": [{"filter": "size(input.name) < 3",
                               "selector": "input.fallback_name",
                               "target": "name"}]}
                """).get("celRules"));
        ObjectNode mapped = catalog.execute("map-message", mapInput);
        assertThat(mapped.get("message").get("name").asText()).isEqualTo("Joseph");

        // 5. validate-message: the repaired message passes
        ObjectNode revalidateInput = schemaInput(descriptorSetBase64);
        revalidateInput.put("type", "loop.Person");
        revalidateInput.set("message", mapped.get("message"));
        ObjectNode passing = catalog.execute("validate-message", revalidateInput);
        assertThat(passing).isEqualTo(obj("{\"valid\": true, \"violations\": []}"));
    }

    private static ObjectNode schemaInput(String descriptorSetBase64) {
        ObjectNode input = TestFixtures.MAPPER.createObjectNode();
        input.putObject("schema").put("descriptorSetBase64", descriptorSetBase64);
        return input;
    }
}
