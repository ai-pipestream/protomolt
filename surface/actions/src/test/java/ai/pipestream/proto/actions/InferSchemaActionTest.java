package ai.pipestream.proto.actions;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import static ai.pipestream.proto.actions.TestFixtures.obj;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Struct-to-proto through the verb surface, and the inferred output feeding list-types. */
class InferSchemaActionTest {

    private final ActionCatalog catalog = ActionCatalog.defaults(TestFixtures.personContext());

    @Test
    void infersRegistrableSchemaFromSamples() throws Exception {
        ObjectNode result = catalog.execute("infer-schema", obj("""
                {"name": "inferred.v1.Event",
                 "samples": [
                   {"id": "e-1", "count": 3, "meta": {"region": "us"}},
                   {"id": "e-2", "count": 4.5, "tags": ["a"]}
                 ]}
                """));
        assertThat(result.get("type").asText()).isEqualTo("inferred.v1.Event");
        assertThat(result.get("protoSource").asText())
                .contains("double count")
                .contains("message Meta")
                .contains("repeated string tags");

        // The inferred descriptor set is immediately usable by the other verbs.
        ObjectNode types = catalog.execute("list-types", obj("""
                {"schema": {"descriptorSetBase64": "%s"}}
                """.formatted(result.get("descriptorSetBase64").asText())));
        assertThat(types.get("types").findValuesAsText("fullName"))
                .contains("inferred.v1.Event");
    }

    @Test
    void degenerateSamplesAreInvalidInput() {
        assertThatThrownBy(() -> catalog.execute("infer-schema", obj("""
                {"name": "x.Y", "samples": [{}]}
                """)))
                .isInstanceOf(ActionException.class)
                .hasMessageContaining("no keys");
    }
}
