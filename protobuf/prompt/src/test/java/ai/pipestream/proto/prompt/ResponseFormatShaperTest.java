package ai.pipestream.proto.prompt;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ResponseFormatShaperTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void wrapsTheSchemaInTheStructuredOutputEnvelope() throws Exception {
        String envelope = ResponseFormatShaper.jsonSchemaEnvelope(
                "example.Form", "{\"type\":\"object\",\"properties\":{\"name\":{\"type\":\"string\"}}}", true);

        JsonNode root = MAPPER.readTree(envelope);
        assertThat(root.get("type").asText()).isEqualTo("json_schema");
        JsonNode inner = root.get("json_schema");
        assertThat(inner.get("name").asText()).isEqualTo("example.Form");
        assertThat(inner.get("strict").asBoolean()).isTrue();
        assertThat(inner.get("schema").get("properties").get("name").get("type").asText())
                .isEqualTo("string");
    }

    @Test
    void rejectsMalformedSchemaJson() {
        assertThatThrownBy(() -> ResponseFormatShaper.jsonSchemaEnvelope(
                "example.Form", "{not json", true))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not valid JSON");
    }

    @Test
    void rejectsABlankName() {
        assertThatThrownBy(() -> ResponseFormatShaper.jsonSchemaEnvelope(
                "  ", "{}", true))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("name");
    }
}
