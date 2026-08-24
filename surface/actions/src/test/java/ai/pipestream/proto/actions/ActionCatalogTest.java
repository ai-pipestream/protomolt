package ai.pipestream.proto.actions;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Struct;
import static ai.pipestream.proto.actions.TestFixtures.MAPPER;
import static ai.pipestream.proto.actions.TestFixtures.obj;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ActionCatalogTest {

    private static final List<String> BUILT_INS = List.of(
            "compile", "validate-message", "diff-schemas", "check-compat", "render-json-schema",
            "render-prompt", "render-index-mappings", "eval-cel", "map-message",
            "synthesize-shape", "join-messages", "merge-schemas", "check-rules", "infer-schema",
            "mask-message", "extract-metadata", "list-types");

    private final ActionCatalog catalog = ActionCatalog.defaults(TestFixtures.personContext());

    @Test
    void executeStreamingFallsBackToSingleEmissionForUnaryActions() throws Exception {
        List<ObjectNode> emitted = new ArrayList<>();
        // putObject returns the nested node, so the envelope has to be held separately;
        // chaining off it would send the inner object as the whole request.
        ObjectNode input = JsonNodeFactory.instance.objectNode();
        input.putObject("schema").put("type", "actions.test.Person");
        catalog.executeStreaming("list-types", input, emitted::add);
        assertThat(emitted).hasSize(1);
    }

    @Test
    void executeStreamingDispatchesToStreamingActions() throws Exception {
        AtomicInteger emissions = new AtomicInteger();
        catalog.register(new JsonStreamingAction() {
            @Override
            public String name() {
                return "test-stream";
            }

            @Override
            public String description() {
                return "emits two documents";
            }

            @Override
            public Descriptor requestType() {
                // Struct accepts any JSON object, so a fixture is not constrained by a
                // contract it is not testing.
                return Struct.getDescriptor();
            }

            @Override
            public Descriptor responseType() {
                // Struct accepts any JSON object, so a fixture is not constrained by a
                // contract it is not testing.
                return Struct.getDescriptor();
            }

            @Override
            public ObjectNode execute(ObjectNode input, ActionContext context) {
                return JsonNodeFactory.instance.objectNode().put("unary", true);
            }

            @Override
            public void executeStreaming(ObjectNode input, ActionContext context,
                    JsonStreamEmitter emitter) throws ActionException {
                emitter.emit(JsonNodeFactory.instance.objectNode().put("n", 1));
                emitter.emit(JsonNodeFactory.instance.objectNode().put("n", 2));
                emissions.addAndGet(2);
            }
        });
        List<ObjectNode> emitted = new ArrayList<>();
        catalog.executeStreaming("test-stream", JsonNodeFactory.instance.objectNode(),
                emitted::add);
        assertThat(emitted).hasSize(2);
        assertThat(emitted.get(0).get("n").asInt()).isEqualTo(1);
        assertThat(emitted.get(1).get("n").asInt()).isEqualTo(2);
    }

    @Test
    void defaultsRegistersAllSeventeenBuiltIns() {
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
            // MCP clients require the root inputSchema.type to be "object";
            // combinators carrying item-level types live below the root.
            assertThat(inputSchema.get("type").asText()).isEqualTo("object");
            // Root combinators are banned: they cannot satisfy both the MCP
            // client (root type required) and strict API validators (no type
            // beside oneOf/anyOf) at once.
            assertThat(inputSchema.has("oneOf")).isFalse();
            assertThat(inputSchema.has("anyOf")).isFalse();
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
        assertThatThrownBy(() -> catalog.execute("list-types", (ObjectNode) null))
                .isInstanceOfSatisfying(ActionException.class,
                        e -> assertThat(e.code()).isEqualTo("invalid-input"));
    }

    @Test
    void registeringACustomActionExtendsTheCatalog() throws Exception {
        ProtoAction custom = new JsonAction() {
            @Override
            public String name() {
                return "noop";
            }

            @Override
            public String description() {
                return "Does nothing.";
            }

            @Override
            public Descriptor requestType() {
                // Struct accepts any JSON object, so a fixture is not constrained by a
                // contract it is not testing.
                return Struct.getDescriptor();
            }

            @Override
            public Descriptor responseType() {
                // Struct accepts any JSON object, so a fixture is not constrained by a
                // contract it is not testing.
                return Struct.getDescriptor();
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
        ProtoAction shadow = new JsonAction() {
            @Override
            public String name() {
                return "list-types";
            }

            @Override
            public String description() {
                return "An impostor.";
            }

            @Override
            public Descriptor requestType() {
                // Struct accepts any JSON object, so a fixture is not constrained by a
                // contract it is not testing.
                return Struct.getDescriptor();
            }

            @Override
            public Descriptor responseType() {
                // Struct accepts any JSON object, so a fixture is not constrained by a
                // contract it is not testing.
                return Struct.getDescriptor();
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

    @Test
    void forkPreservesManifestAndIsolatesExtensions() throws Exception {
        ActionCatalog fork = catalog.fork();

        assertThat(fork.names()).containsExactlyElementsOf(catalog.names());
        assertThat(fork.list()).isEqualTo(catalog.list());

        fork.register(noop("mcp-extension"));
        assertThat(fork.get("mcp-extension")).isNotNull();
        assertThat(catalog.names()).doesNotContain("mcp-extension");

        catalog.register(noop("host-extension"));
        assertThat(catalog.get("host-extension")).isNotNull();
        assertThat(fork.names()).doesNotContain("host-extension");
    }

    @Test
    void registrationAndSnapshotsAreSafeUnderConcurrency() throws Exception {
        int additions = 100;
        try (var executor = Executors.newFixedThreadPool(12)) {
            List<java.util.concurrent.Callable<Void>> tasks = new ArrayList<>();
            for (int i = 0; i < additions; i++) {
                int index = i;
                tasks.add(() -> {
                    catalog.register(noop("concurrent-" + index));
                    catalog.names();
                    catalog.list();
                    return null;
                });
            }
            for (var result : executor.invokeAll(tasks)) {
                result.get();
            }
        }

        assertThat(catalog.names()).hasSize(BUILT_INS.size() + additions);
        assertThat(catalog.list()).hasSize(BUILT_INS.size() + additions);
        for (int i = 0; i < additions; i++) {
            assertThat(catalog.get("concurrent-" + i)).isNotNull();
        }
    }

    private static ProtoAction noop(String name) {
        return new JsonAction() {
            @Override
            public String name() {
                return name;
            }

            @Override
            public String description() {
                return "Concurrent test action.";
            }

            @Override
            public Descriptor requestType() {
                // Struct accepts any JSON object, so a fixture is not constrained by a
                // contract it is not testing.
                return Struct.getDescriptor();
            }

            @Override
            public Descriptor responseType() {
                // Struct accepts any JSON object, so a fixture is not constrained by a
                // contract it is not testing.
                return Struct.getDescriptor();
            }

            @Override
            public ObjectNode execute(ObjectNode input, ActionContext context) {
                return MAPPER.createObjectNode().put("ok", true);
            }
        };
    }
}
