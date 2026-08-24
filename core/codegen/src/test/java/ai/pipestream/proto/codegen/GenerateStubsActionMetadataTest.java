package ai.pipestream.proto.codegen;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The action's self-description: name, description, and the JSON Schema it advertises.
 * The schema is the caller-facing contract, so its shape is pinned here.
 */
class GenerateStubsActionMetadataTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final GenerateStubsAction action = new GenerateStubsAction();

    @Test
    void hasStableActionName() {
        assertThat(action.name()).isEqualTo("generate-stubs");
    }

    @Test
    void descriptionNamesEveryGenerator() {
        String description = action.description();

        assertThat(description).isNotBlank();
        for (String generator : List.of(
                "java", "kotlin", "python", "cpp", "csharp", "ruby", "php", "objc", "grpc-java")) {
            assertThat(description).contains(generator);
        }
    }

    @Test
    void inputSchemaDeclaresClosedObjectRequiringSchema() {
        ObjectNode schema = action.inputSchema();

        assertThat(schema.get("$schema").asText())
                .isEqualTo("https://json-schema.org/draft/2020-12/schema");
        assertThat(schema.get("type").asText()).isEqualTo("object");
        assertThat(schema.path("required")).hasSize(1);
        assertThat(schema.path("required").get(0).asText()).isEqualTo("schema");
        assertThat(schema.path("properties").properties())
                .extracting(java.util.Map.Entry::getKey)
                .containsExactlyInAnyOrder("schema", "generators", "files", "parameter");
    }

    @Test
    void inputSchemaDeclaresAllGeneratorNamesWithMinimumOne() {
        JsonNode generators = action.inputSchema().path("properties").path("generators");

        assertThat(generators.path("type").asText()).isEqualTo("array");
        // Each generator is a CodeGenerator value. proto3 JSON accepts an enum by name or
        // by number, so the item schema is the choice between the two; the names are what
        // a caller writes.
        assertThat(generators.path("items").toString())
                .contains("CODE_GENERATOR_JAVA")
                .contains("CODE_GENERATOR_GRPC_JAVA")
                .contains("CODE_GENERATOR_OBJC");
    }

    @Test
    void inputSchemaDeclaresFilesAndParameter() {
        JsonNode properties = action.inputSchema().path("properties");

        assertThat(properties.path("files").path("type").asText()).isEqualTo("array");
        assertThat(properties.path("files").path("items").path("type").asText())
                .isEqualTo("string");
        assertThat(properties.path("parameter").path("type").asText()).isEqualTo("string");
    }
}
