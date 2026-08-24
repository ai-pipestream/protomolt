package ai.pipestream.proto.mcp;

import ai.pipestream.proto.actions.ActionCatalog;
import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.JsonAction;
import ai.pipestream.proto.actions.ProtoAction;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Struct;
import static org.assertj.core.api.Assertions.assertThat;

class WorkspaceResourcesTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void bootstrapIsCompactAndTracksTheExactLiveCatalog() throws Exception {
        ActionCatalog catalog = ActionCatalog.defaults(ActionContext.create());
        WorkspaceResources resources = new WorkspaceResources(catalog, "protomolt", "1.2.3",
                "Follow reflected descriptors.");

        assertThat(resources.list(mapper).findValuesAsText("uri"))
                .containsExactly(WorkspaceResources.URI);
        JsonNode before = document(resources);
        assertThat(before.path("server").path("name").asText()).isEqualTo("protomolt");
        assertThat(before.path("server").path("version").asText()).isEqualTo("1.2.3");
        assertThat(before.path("toolCatalog").path("count").asInt())
                .isEqualTo(catalog.names().size());
        assertThat(before.path("toolCatalog").path("names"))
                .anySatisfy(name -> assertThat(name.asText()).isEqualTo("compile"));
        assertThat(before.path("toolCatalog").path("fingerprint").asText())
                .matches("sha256:[0-9a-f]{64}");
        assertThat(before.toString()).doesNotContain("inputSchema", "description");

        String firstFingerprint = before.path("toolCatalog").path("fingerprint").asText();
        catalog.register(markerAction());
        JsonNode after = document(resources);
        assertThat(after.path("toolCatalog").path("count").asInt())
                .isEqualTo(before.path("toolCatalog").path("count").asInt() + 1);
        assertThat(after.path("toolCatalog").path("fingerprint").asText())
                .isNotEqualTo(firstFingerprint);
        assertThat(after.path("toolCatalog").path("names"))
                .anySatisfy(name -> assertThat(name.asText()).isEqualTo("marker"));
    }

    private JsonNode document(WorkspaceResources resources) throws Exception {
        ObjectNode contents = resources.read(mapper, WorkspaceResources.URI).orElseThrow();
        return mapper.readTree(contents.path("text").asText());
    }

    private ProtoAction markerAction() {
        return new JsonAction() {
            @Override
            public String name() {
                return "marker";
            }

            @Override
            public String description() {
                return "Marker action whose schema must stay out of the compact bootstrap.";
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
                return context.objectMapper().createObjectNode();
            }
        };
    }
}
