package ai.pipestream.proto.mcp;

import ai.pipestream.proto.registry.InMemorySchemaRegistryStore;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class RegistryResourcesTest {

    private static final String ORDER_PROTO = """
            syntax = "proto3";
            package shop;
            message Order { string id = 1; }
            """;

    private final ObjectMapper mapper = new ObjectMapper();
    private InMemorySchemaRegistryStore store;
    private RegistryResources resources;

    @BeforeEach
    void setUp() throws Exception {
        store = new InMemorySchemaRegistryStore();
        store.register("orders-value", ORDER_PROTO, List.of());
        store.register("orders-value", ORDER_PROTO + "\n// v2\n", List.of());
        store.register("shipping/events", ORDER_PROTO, List.of());
        resources = new RegistryResources(store);
    }

    @Test
    void listContainsIndexAndOnePerSubject() {
        ArrayNode list = resources.list(mapper);
        assertThat(list.findValuesAsText("uri")).containsExactly(
                "protomolt://registry/subjects",
                "protomolt://registry/subjects/orders-value",
                "protomolt://registry/subjects/shipping%2Fevents");
    }

    @Test
    void subjectsIndexReadsAsJson() throws Exception {
        Optional<ObjectNode> contents = resources.read(mapper, "protomolt://registry/subjects");
        assertThat(contents).isPresent();
        JsonNode doc = mapper.readTree(contents.get().get("text").asText());
        List<String> subjects = new java.util.ArrayList<>();
        doc.get("subjects").forEach(node -> subjects.add(node.asText()));
        assertThat(subjects).containsExactlyInAnyOrder("orders-value", "shipping/events");
        assertThat(doc.get("globalCompatibilityMode").asText()).isNotEmpty();
    }

    @Test
    void subjectResourceCarriesVersionsAndLatestSchema() throws Exception {
        Optional<ObjectNode> contents =
                resources.read(mapper, "protomolt://registry/subjects/orders-value");
        assertThat(contents).isPresent();
        JsonNode doc = mapper.readTree(contents.get().get("text").asText());
        assertThat(doc.get("subject").asText()).isEqualTo("orders-value");
        assertThat(doc.get("versions").size()).isEqualTo(2);
        assertThat(doc.get("latest").get("version").asInt()).isEqualTo(2);
        assertThat(doc.get("latest").get("schemaText").asText()).contains("message Order");
    }

    @Test
    void slashSubjectsRoundTripThroughUrlEncoding() throws Exception {
        Optional<ObjectNode> contents =
                resources.read(mapper, "protomolt://registry/subjects/shipping%2Fevents");
        assertThat(contents).isPresent();
        JsonNode doc = mapper.readTree(contents.get().get("text").asText());
        assertThat(doc.get("subject").asText()).isEqualTo("shipping/events");
    }

    @Test
    void exactVersionIsAddressable() throws Exception {
        Optional<ObjectNode> contents =
                resources.read(mapper, "protomolt://registry/subjects/orders-value/versions/1");
        assertThat(contents).isPresent();
        JsonNode doc = mapper.readTree(contents.get().get("text").asText());
        assertThat(doc.get("version").asInt()).isEqualTo(1);
        assertThat(doc.get("schemaText").asText()).doesNotContain("// v2");
    }

    @Test
    void unknownUrisAreEmpty() {
        assertThat(resources.read(mapper, "protomolt://registry/subjects/nope")).isEmpty();
        assertThat(resources.read(mapper, "protomolt://registry/subjects/orders-value/versions/99")).isEmpty();
        assertThat(resources.read(mapper, "protomolt://registry/subjects/orders-value/versions/x")).isEmpty();
        assertThat(resources.read(mapper, "https://example.com/other")).isEmpty();
    }
}
