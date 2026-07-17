package ai.pipestream.proto.actions;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import static ai.pipestream.proto.actions.TestFixtures.obj;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RenderIndexMappingsActionTest {

    private final ActionContext context = ActionContext.create();
    private final ActionCatalog catalog = ActionCatalog.defaults(context);

    RenderIndexMappingsActionTest() {
        context.registry().registerFile(TestFixtures.hintedFile());
    }

    private ObjectNode render(String engine) throws Exception {
        return catalog.execute("render-index-mappings", obj("""
                {"schema": {"type": "actions.test.HintedDoc"}, "engine": "%s"}
                """.formatted(engine)));
    }

    @Test
    void opensearchMappingsHonorHintsAndInference() throws Exception {
        ObjectNode result = render("opensearch");
        JsonNode properties = result.get("properties");
        JsonNode title = properties.get("title");
        assertThat(title.get("type").asText()).isEqualTo("text");
        assertThat(title.get("analyzer").asText()).isEqualTo("english");
        assertThat(title.get("fields").get("raw").get("type").asText()).isEqualTo("keyword");
        assertThat(properties.get("id").get("type").asText()).isEqualTo("keyword");
        // no hint on count: inference kicks in
        assertThat(properties.get("count").get("type").asText()).isEqualTo("integer");
    }

    @Test
    void sensitivityRendersSecurityFragmentAndEncryptedContainers() throws Exception {
        // mask: the field stays searchable; the security plugin hashes it at query time.
        ObjectNode masked = catalog.execute("render-index-mappings", obj("""
                {"schema": {"type": "actions.test.HintedDoc"}, "engine": "opensearch",
                 "sensitivity": {"mask": ["pii"]}}
                """));
        assertThat(masked.get("security").get("maskedFields"))
                .extracting(JsonNode::asText).containsExactly("title");
        assertThat(masked.get("mappings").get("properties").get("title")
                .get("type").asText()).isEqualTo("text");

        // encrypt: the field becomes a store-only ciphertext container.
        ObjectNode encrypted = catalog.execute("render-index-mappings", obj("""
                {"schema": {"type": "actions.test.HintedDoc"}, "engine": "opensearch",
                 "sensitivity": {"encrypt": ["pii"], "exclude": ["secret"]}}
                """));
        JsonNode title = encrypted.get("mappings").get("properties").get("title");
        assertThat(title.get("type").asText()).isEqualTo("keyword");
        assertThat(title.get("index").asBoolean()).isFalse();
        assertThat(title.get("doc_values").asBoolean()).isFalse();
        assertThat(encrypted.get("security").get("fls")).isEmpty();
    }

    /** The complete role body, ready to PUT at _plugins/_security/api/roles/{name}. */
    @Test
    void sensitivityRoleRendersAnApplyableRoleBody() throws Exception {
        ObjectNode result = catalog.execute("render-index-mappings", obj("""
                {"schema": {"type": "actions.test.HintedDoc"}, "engine": "opensearch",
                 "sensitivity": {"mask": ["pii"],
                                 "maskFormat": {"pii": "::SHA-512"},
                                 "role": {"indexPatterns": ["docs-*"]}}}
                """));

        // The per-class format rides on the masked_fields entry itself.
        assertThat(result.get("security").get("maskedFields"))
                .extracting(JsonNode::asText).containsExactly("title::SHA-512");

        JsonNode permission = result.get("security").get("role")
                .get("index_permissions").get(0);
        assertThat(permission.get("index_patterns"))
                .extracting(JsonNode::asText).containsExactly("docs-*");
        assertThat(permission.get("allowed_actions"))
                .extracting(JsonNode::asText).containsExactly("read");
        assertThat(permission.get("masked_fields"))
                .extracting(JsonNode::asText).containsExactly("title::SHA-512");
        // Nothing excluded: fls is omitted, because absent and empty mean different things.
        assertThat(permission.has("fls")).isFalse();
    }

    @Test
    void sensitivityRoleRequiresIndexPatterns() {
        assertThatThrownBy(() -> catalog.execute("render-index-mappings", obj("""
                {"schema": {"type": "actions.test.HintedDoc"}, "engine": "opensearch",
                 "sensitivity": {"mask": ["pii"], "role": {}}}
                """)))
                .hasMessageContaining("indexPatterns");
    }

    @Test
    void solrSchemaHonorsHintsAndInference() throws Exception {
        ObjectNode result = render("solr");
        assertThat(result.has("fieldTypes")).isTrue();
        assertThat(result.has("copyFields")).isTrue();
        JsonNode fields = result.get("fields");
        JsonNode title = fieldNamed(fields, "title");
        // Solr analysis lives on the field type; 'english' maps to _default's text_en.
        assertThat(title.get("type").asText()).isEqualTo("text_en");
        assertThat(fieldNamed(fields, "id").get("type").asText()).isEqualTo("string");
        assertThat(fieldNamed(fields, "count").get("type").asText()).isEqualTo("pint");
    }

    @Test
    void luceneFieldSpecsHonorHintsAndInference() throws Exception {
        ObjectNode result = render("lucene");
        assertThat(result.get("messageFullName").asText()).isEqualTo("actions.test.HintedDoc");
        JsonNode fields = result.get("fields");
        JsonNode title = fieldNamed(fields, "title");
        assertThat(title.get("kind").asText()).isEqualTo("TEXT");
        assertThat(title.get("analyzer").asText()).isEqualTo("english");
        JsonNode id = fieldNamed(fields, "id");
        assertThat(id.get("kind").asText()).isEqualTo("KEYWORD");
        assertThat(id.get("sortable").asBoolean()).isTrue();
        assertThat(fieldNamed(fields, "count").get("kind").asText()).isEqualTo("INT32");
    }

    @Test
    void unhintedInlineSchemaFallsBackToInference() throws Exception {
        ObjectNode input = obj("""
                {"schema": {"sources": {}}, "engine": "opensearch"}
                """);
        ((ObjectNode) input.get("schema").get("sources")).put("doc.proto", """
                syntax = "proto3";
                package infer;
                message Doc {
                  string doc_id = 1;
                  string title = 2;
                  int64 views = 3;
                  bool archived = 4;
                }
                """);
        ObjectNode result = catalog.execute("render-index-mappings", input);
        JsonNode properties = result.get("properties");
        assertThat(properties.get("doc_id").get("type").asText()).isEqualTo("keyword");
        assertThat(properties.get("title").get("type").asText()).isEqualTo("text");
        assertThat(properties.get("views").get("type").asText()).isEqualTo("long");
        assertThat(properties.get("archived").get("type").asText()).isEqualTo("boolean");
    }

    @Test
    void unknownEngineIsInvalidInput() {
        assertThatThrownBy(() -> render("elasticsearch"))
                .isInstanceOfSatisfying(ActionException.class, e -> {
                    assertThat(e.code()).isEqualTo("invalid-input");
                    assertThat(e.details().orElseThrow().get("pointer").asText()).isEqualTo("/engine");
                });
    }

    @Test
    void missingEngineIsInvalidInput() {
        assertThatThrownBy(() -> catalog.execute("render-index-mappings",
                obj("{\"schema\": {\"type\": \"actions.test.HintedDoc\"}}")))
                .isInstanceOfSatisfying(ActionException.class, e -> {
                    assertThat(e.code()).isEqualTo("invalid-input");
                    assertThat(e.details().orElseThrow().get("pointer").asText()).isEqualTo("/engine");
                });
    }

    private static JsonNode fieldNamed(JsonNode fields, String name) {
        for (JsonNode field : fields) {
            if (field.get("name").asText().equals(name)) {
                return field;
            }
        }
        throw new AssertionError("no field named " + name + " in " + fields);
    }
}
