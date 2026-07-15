package ai.pipestream.proto.actions;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import java.util.List;

import static ai.pipestream.proto.actions.TestFixtures.MAPPER;
import static ai.pipestream.proto.actions.TestFixtures.obj;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ActionCatalogTest {

    private static final List<String> BUILT_INS = List.of(
            "compile", "validate-message", "diff-schemas", "check-compat", "render-json-schema",
            "render-index-mappings", "eval-cel", "map-message", "synthesize-shape",
            "join-messages", "merge-schemas", "check-rules", "infer-schema", "mask-message",
            "extract-metadata", "list-types");

    private final ActionCatalog catalog = ActionCatalog.defaults(TestFixtures.personContext());

    @Test
    void defaultsRegistersAllSixteenBuiltIns() {
        assertThat(catalog.names()).containsExactlyInAnyOrderElementsOf(BUILT_INS);
    }

    @Test
    void listReturnsNameDescriptionAndInputSchemaPerAction() {
        ArrayNode manifest = catalog.list();
        assertThat(manifest).hasSize(BUILT_INS.size());
        for (JsonNode entry : manifest) {
            assertThat(entry.get("name").asText()).matches("[a-z0-9]+(-[a-z0-9]+)*");
            assertThat(entry.get("description").asText()).isNotBlank();
            JsonNode inputSchema = entry.get("inputSchema");
            assertThat(inputSchema.isObject()).isTrue();
            assertThat(inputSchema.get("$schema").asText())
                    .isEqualTo("https://json-schema.org/draft/2020-12/schema");
            assertThat(inputSchema.get("type").asText()).isEqualTo("object");
            assertThat(inputSchema.has("properties")).isTrue();
        }
        assertThat(manifest.findValuesAsText("name")).containsAll(BUILT_INS);
    }

    @Test
    void unknownActionListsAvailableNames() {
        assertThatThrownBy(() -> catalog.execute("frobnicate", MAPPER.createObjectNode()))
                .isInstanceOfSatisfying(ActionException.class, e -> {
                    assertThat(e.code()).isEqualTo("unknown-action");
                    assertThat(e.getMessage()).contains("frobnicate").contains("validate-message");
                    assertThat(e.details().orElseThrow().get("available"))
                            .extracting(JsonNode::asText)
                            .containsExactlyInAnyOrderElementsOf(BUILT_INS);
                });
    }

    @Test
    void getReturnsRegisteredAction() throws Exception {
        assertThat(catalog.get("compile").name()).isEqualTo("compile");
    }

    @Test
    void executeDispatchesToTheNamedAction() throws Exception {
        ObjectNode result = catalog.execute("list-types", obj("{\"filter\": \"Person\"}"));
        assertThat(result.get("types").get(0).get("fullName").asText())
                .isEqualTo("actions.test.Person");
    }

    @Test
    void nullInputIsInvalidInput() {
        assertThatThrownBy(() -> catalog.execute("list-types", null))
                .isInstanceOfSatisfying(ActionException.class,
                        e -> assertThat(e.code()).isEqualTo("invalid-input"));
    }

    @Test
    void registeringACustomActionExtendsTheCatalog() throws Exception {
        ProtoAction custom = new ProtoAction() {
            @Override
            public String name() {
                return "noop";
            }

            @Override
            public String description() {
                return "Does nothing.";
            }

            @Override
            public ObjectNode inputSchema() {
                return ActionJson.baseInputSchema();
            }

            @Override
            public ObjectNode execute(ObjectNode input, ActionContext context) {
                return MAPPER.createObjectNode().put("ok", true);
            }
        };
        catalog.register(custom);
        assertThat(catalog.execute("noop", MAPPER.createObjectNode()).get("ok").asBoolean()).isTrue();
    }

    @Test
    void duplicateNamesAreRejectedUnlessReplaceIsExplicit() throws Exception {
        ProtoAction shadow = new ProtoAction() {
            @Override
            public String name() {
                return "list-types";
            }

            @Override
            public String description() {
                return "An impostor.";
            }

            @Override
            public ObjectNode inputSchema() {
                return ActionJson.baseInputSchema();
            }

            @Override
            public ObjectNode execute(ObjectNode input, ActionContext context) {
                return MAPPER.createObjectNode().put("impostor", true);
            }
        };
        assertThatThrownBy(() -> catalog.register(shadow))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("list-types")
                .hasMessageContaining("replace()");

        // The explicit override path works and is visibly intentional.
        catalog.replace(shadow);
        assertThat(catalog.execute("list-types", MAPPER.createObjectNode())
                .get("impostor").asBoolean()).isTrue();
    }

    @Test
    void namesKeepRegistrationOrder() {
        assertThat(catalog.names()).startsWith("compile", "validate-message")
                .endsWith("extract-metadata", "list-types");
    }
}
