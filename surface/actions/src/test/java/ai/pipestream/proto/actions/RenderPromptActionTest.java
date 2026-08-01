package ai.pipestream.proto.actions;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import static ai.pipestream.proto.actions.TestFixtures.obj;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RenderPromptActionTest {

    private final ActionCatalog catalog = ActionCatalog.defaults(TestFixtures.personContext());

    @Test
    void rendersThePacketForARegistryType() throws Exception {
        ObjectNode result = catalog.execute("render-prompt",
                obj("{\"schema\": {\"type\": \"actions.test.Person\"}}"));

        assertThat(result.get("target_type").asText()).isEqualTo("actions.test.Person");
        assertThat(result.get("descriptor_set_ref").asText()).isEmpty();
        String instructions = result.get("instructions").asText();
        assertThat(instructions)
                .contains("You are filling the form \"actions.test.Person\"")
                .contains("Full name")
                .contains("must be at least 3 characters")
                .contains("Respond with exactly one JSON object")
                .doesNotContain("Persona:");
        assertThat(result.get("response_json_schema").get("$ref").asText())
                .isEqualTo("#/$defs/actions.test.Person");
        assertThat(result.has("persona")).isFalse();
        assertThat(result.get("few_shot").isArray()).isTrue();
    }

    @Test
    void rendersAResolvedPersona() throws Exception {
        ObjectNode result = catalog.execute("render-prompt", obj("""
                {"schema": {"type": "actions.test.Person"},
                 "descriptor_set_ref": "repo://forms/v3",
                 "persona": {"id": "litigator", "version": "1.0.0",
                             "instructions": "You fill forms as a practicing litigator.",
                             "safeguards": ["Never speculate about intent."]}}
                """));

        assertThat(result.get("descriptor_set_ref").asText()).isEqualTo("repo://forms/v3");
        assertThat(result.get("instructions").asText())
                .contains("Persona: litigator (version 1.0.0)")
                .contains("Never speculate about intent.");
        assertThat(result.get("persona").get("id").asText()).isEqualTo("litigator");
    }

    @Test
    void rejectsAMalformedPersona() {
        // a JSON object where a string field belongs cannot parse as a Persona at all
        assertThatThrownBy(() -> catalog.execute("render-prompt",
                obj("{\"schema\": {\"type\": \"actions.test.Person\"},"
                        + " \"persona\": {\"id\": {\"nested\": true}}}}}")))
                .isInstanceOfSatisfying(ActionException.class,
                        e -> assertThat(e.code()).isEqualTo("invalid-persona"));
    }

    @Test
    void rejectsAnInvalidPersona() {
        // version and instructions are required by prompt.v1's own validate.v1 rules
        assertThatThrownBy(() -> catalog.execute("render-prompt",
                obj("{\"schema\": {\"type\": \"actions.test.Person\"},"
                        + " \"persona\": {\"id\": \"litigator\"}}")))
                .isInstanceOfSatisfying(ActionException.class,
                        e -> assertThat(e.code()).isEqualTo("render-failed"));
    }

    @Test
    void unknownTypeIsUnknownType() {
        assertThatThrownBy(() -> catalog.execute("render-prompt",
                obj("{\"schema\": {\"type\": \"actions.test.Ghost\"}}")))
                .isInstanceOfSatisfying(ActionException.class,
                        e -> assertThat(e.code()).isEqualTo("unknown-type"));
    }
}
