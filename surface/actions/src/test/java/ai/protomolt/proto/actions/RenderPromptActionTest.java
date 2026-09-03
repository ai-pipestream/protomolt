package ai.protomolt.proto.actions;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import static ai.protomolt.proto.actions.TestFixtures.obj;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RenderPromptActionTest {

    private final ActionCatalog catalog = ActionCatalog.defaults(TestFixtures.personContext());

    @Test
    void rendersThePacketForARegistryType() throws Exception {
        ObjectNode result = catalog.execute("render-prompt",
                obj("{\"schema\": {\"type\": \"actions.test.Person\"}}"));

        assertThat(result.get("targetType").asText()).isEqualTo("actions.test.Person");
        assertThat(result.get("descriptorSetRef").asText()).isEmpty();
        String instructions = result.get("instructions").asText();
        assertThat(instructions)
                .contains("You are filling the form \"actions.test.Person\"")
                .contains("Full name")
                .contains("must be at least 3 characters")
                .contains("Respond with exactly one JSON object")
                .doesNotContain("Persona:");
        assertThat(result.get("responseJsonSchema").get("$ref").asText())
                .isEqualTo("#/$defs/actions.test.Person");
        assertThat(result.has("persona")).isFalse();
        assertThat(result.get("fewShot").isArray()).isTrue();
    }

    @Test
    void rendersAResolvedPersona() throws Exception {
        ObjectNode result = catalog.execute("render-prompt", obj("""
                {"schema": {"type": "actions.test.Person"},
                 "descriptorSetRef": "repo://forms/v3",
                 "persona": {"id": "litigator", "version": "1.0.0",
                             "instructions": "You fill forms as a practicing litigator.",
                             "safeguards": ["Never speculate about intent."]}}
                """));

        assertThat(result.get("descriptorSetRef").asText()).isEqualTo("repo://forms/v3");
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

    /**
     * The live path: compile inline sources, then render from the descriptor-set base64.
     * {@code SchemaResolver}'s extension registry must know llm.v1 and quality.v1 or their
     * options drop to unknown fields at parse time and silently never render.
     */
    @Test
    void compiledSourcesKeepLlmAndQualityAnnotations() throws Exception {
        String probeProto = """
                syntax = "proto3";
                package probe;
                import "ai/protomolt/proto/llm/v1/llm.proto";
                import "ai/protomolt/proto/quality/v1/quality.proto";
                message Probe {
                  option (ai.protomolt.proto.quality.v1.quality) = {
                    dimension: {id: "completeness", cel: "size(this.court) > 0 ? 1.0 : 0.0"}
                  };
                  // The issuing court.
                  string court = 1 [(ai.protomolt.proto.llm.v1.field) = {
                    instruction: "Name the court exactly as it appears in the caption."
                    volatile: true
                  }];
                }
                """;
        ObjectNode compileInput = obj("{\"sources\": {}}");
        ObjectNode sources = (ObjectNode) compileInput.get("sources");
        sources.put("probe/probe.proto", probeProto);
        sources.put("ai/protomolt/proto/llm/v1/llm.proto",
                classpathProto("/ai/protomolt/proto/llm/v1/llm.proto"));
        sources.put("ai/protomolt/proto/quality/v1/quality.proto",
                classpathProto("/ai/protomolt/proto/quality/v1/quality.proto"));
        ObjectNode compiled = catalog.execute("compile", compileInput);
        assertThat(compiled.get("ok").asBoolean()).isTrue();

        ObjectNode renderInput = obj("{\"schema\": {}, \"type\": \"probe.Probe\"}");
        ((ObjectNode) renderInput.get("schema")).put("descriptorSetBase64",
                compiled.get("descriptorSetBase64").asText());
        ObjectNode result = catalog.execute("render-prompt", renderInput);

        assertThat(result.get("instructions").asText())
                .contains("Name the court exactly as it appears in the caption.")
                .contains("time-relative")
                .contains("completeness");
    }

    private static String classpathProto(String path) {
        try (var in = RenderPromptActionTest.class.getResourceAsStream(path)) {
            assertThat(in).as("proto on the test classpath: %s", path).isNotNull();
            return new String(in.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
        } catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
    }
}
