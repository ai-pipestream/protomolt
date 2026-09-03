package ai.protomolt.proto.mcp;

import ai.protomolt.proto.registry.InMemorySchemaRegistryStore;
import ai.protomolt.proto.registry.SchemaReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The schema-document shapes {@link RegistryResources} renders: references, per-subject
 * compatibility modes, and the version-suffix URI grammar's reject cases.
 */
class RegistryResourcesDocumentTest {

    private static final String COMMON_PROTO = """
            syntax = "proto3";
            package shop;
            message Id { string value = 1; }
            """;

    private static final String ORDER_PROTO = """
            syntax = "proto3";
            package shop;
            import "common.proto";
            message Order { Id id = 1; }
            """;

    // v2 stands alone (no import), so it registers with no references — letting the document
    // tests cover both the with-references and without-references shapes on one subject.
    private static final String ORDER_PROTO_V2 = """
            syntax = "proto3";
            package shop;
            message Order { string id = 1; }
            // v2
            """;

    private final ObjectMapper mapper = new ObjectMapper();
    private InMemorySchemaRegistryStore store;
    private RegistryResources resources;

    @BeforeEach
    void setUp() throws Exception {
        store = new InMemorySchemaRegistryStore();
        store.register("common", COMMON_PROTO, List.of());
        store.register("orders-value", ORDER_PROTO,
                List.of(new SchemaReference("common.proto", "common", 1)));
        store.register("orders-value", ORDER_PROTO_V2, List.of());
        resources = new RegistryResources(store);
    }

    private JsonNode readDocument(String uri) throws Exception {
        Optional<ObjectNode> contents = resources.read(mapper, uri);
        assertThat(contents).isPresent();
        return mapper.readTree(contents.get().get("text").asText());
    }

    @Test
    void schemaDocumentsRenderTheirReferences() throws Exception {
        JsonNode doc = readDocument("protomolt://registry/subjects/orders-value/versions/1");
        assertThat(doc.get("subject").asText()).isEqualTo("orders-value");
        assertThat(doc.get("globalId").asInt()).isPositive();
        JsonNode references = doc.get("references");
        assertThat(references.size()).isEqualTo(1);
        assertThat(references.get(0).get("name").asText()).isEqualTo("common.proto");
        assertThat(references.get(0).get("subject").asText()).isEqualTo("common");
        assertThat(references.get(0).get("version").asInt()).isEqualTo(1);
    }

    @Test
    void aVersionWithoutReferencesRendersAnEmptyArray() throws Exception {
        JsonNode doc = readDocument("protomolt://registry/subjects/orders-value/versions/2");
        assertThat(doc.get("schemaText").asText()).contains("// v2");
        assertThat(doc.get("references").size()).isZero();
    }

    @Test
    void theSubjectDocumentCarriesTheCompatibilityModeWhenSet() throws Exception {
        store.setCompatibilityMode("orders-value", "FULL");
        JsonNode doc = readDocument("protomolt://registry/subjects/orders-value");
        assertThat(doc.get("compatibilityMode").asText()).isEqualTo("FULL");
        assertThat(doc.get("latest").get("version").asInt()).isEqualTo(2);
    }

    @Test
    void theSubjectDocumentOmitsTheCompatibilityModeWhenUnset() throws Exception {
        JsonNode doc = readDocument("protomolt://registry/subjects/orders-value");
        assertThat(doc.has("compatibilityMode")).isFalse();
    }

    @Test
    void uriTailsThatAreNotVersionPathsAreNotServed() {
        assertThat(resources.read(mapper, "protomolt://registry/subjects/orders-value/garbage")).isEmpty();
        assertThat(resources.read(mapper, "protomolt://registry/subjects/orders-value/versions")).isEmpty();
        assertThat(resources.read(mapper, "protomolt://registry/subjects/orders-value/versions/")).isEmpty();
        assertThat(resources.read(mapper, "protomolt://registry/subjects/orders-value/versions/-1")).isEmpty();
        assertThat(resources.read(mapper, "protomolt://registry/subjects/orders-value/versions/1/extra")).isEmpty();
    }

    @Test
    void aSubjectWithNoVersionsIsNotServed() {
        // The store only lists subjects that were registered; a name that never was has no
        // version index, so its document cannot exist.
        assertThat(resources.read(mapper, "protomolt://registry/subjects/never-registered")).isEmpty();
    }

    @Test
    void listEntriesCarryNameDescriptionAndMimeType() {
        var list = resources.list(mapper);
        for (JsonNode entry : list) {
            assertThat(entry.get("uri").asText()).startsWith("protomolt://registry/subjects");
            assertThat(entry.get("name").asText()).isNotEmpty();
            assertThat(entry.get("description").asText()).isNotEmpty();
            assertThat(entry.get("mimeType").asText()).isEqualTo("application/json");
        }
    }
}
