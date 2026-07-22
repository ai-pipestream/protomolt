package ai.pipestream.proto.actions;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import static ai.pipestream.proto.actions.TestFixtures.obj;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RenderJsonSchemaActionTest {

    private final ActionCatalog catalog = ActionCatalog.defaults(TestFixtures.personContext());

    @Test
    void rendersTheJsonSchemaDocumentForARegistryType() throws Exception {
        ObjectNode result = catalog.execute("render-json-schema",
                obj("{\"schema\": {\"type\": \"actions.test.Person\"}}"));
        assertThat(result.get("$schema").asText())
                .isEqualTo("https://json-schema.org/draft/2020-12/schema");
        assertThat(result.get("$ref").asText()).isEqualTo("#/$defs/actions.test.Person");
        JsonNode def = result.get("$defs").get("actions.test.Person");
        assertThat(def.get("type").asText()).isEqualTo("object");
        assertThat(def.get("properties").fieldNames())
                .toIterable()
                .containsExactlyInAnyOrder("name", "age", "nickname");
        // declared validation rules fold into JSON Schema constraints
        assertThat(def.get("properties").get("name").get("minLength").asInt()).isEqualTo(3);
    }

    @Test
    void rendersForAnInlineSchemaWithoutExplicitType() throws Exception {
        ObjectNode input = obj("{\"schema\": {\"sources\": {}}}");
        ((ObjectNode) input.get("schema").get("sources")).put("doc.proto", TestFixtures.DOC_PROTO);
        ObjectNode result = catalog.execute("render-json-schema", input);
        assertThat(result.get("$ref").asText()).isEqualTo("#/$defs/t.Doc");
        assertThat(result.get("$defs").get("t.Doc").get("properties").fieldNames())
                .toIterable()
                .containsExactlyInAnyOrder("title", "alt");
    }

    @Test
    void unknownTypeIsUnknownType() {
        assertThatThrownBy(() -> catalog.execute("render-json-schema",
                obj("{\"schema\": {\"type\": \"actions.test.Ghost\"}}")))
                .isInstanceOfSatisfying(ActionException.class,
                        e -> assertThat(e.code()).isEqualTo("unknown-type"));
    }

    @Test
    void missingSchemaIsInvalidInput() {
        assertThatThrownBy(() -> catalog.execute("render-json-schema", obj("{}")))
                .isInstanceOfSatisfying(ActionException.class,
                        e -> assertThat(e.code()).isEqualTo("invalid-input"));
    }
}
