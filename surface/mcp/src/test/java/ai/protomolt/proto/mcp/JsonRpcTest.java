package ai.protomolt.proto.mcp;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class JsonRpcTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void resultCarriesVersionIdAndPayload() {
        ObjectNode payload = mapper.createObjectNode();
        payload.put("ok", true);
        ObjectNode response = JsonRpc.result(mapper, mapper.getNodeFactory().numberNode(3), payload);

        assertThat(response.get("jsonrpc").asText()).isEqualTo("2.0");
        assertThat(response.get("id").asInt()).isEqualTo(3);
        assertThat(response.get("result").get("ok").asBoolean()).isTrue();
        assertThat(response.has("error")).isFalse();
    }

    @Test
    void errorEchoesAStringId() {
        ObjectNode response = JsonRpc.error(mapper, mapper.getNodeFactory().textNode("req-9"),
                JsonRpc.METHOD_NOT_FOUND, "Method not found: prompts/list");

        assertThat(response.get("jsonrpc").asText()).isEqualTo("2.0");
        assertThat(response.get("id").asText()).isEqualTo("req-9");
        assertThat(response.get("error").get("code").asInt()).isEqualTo(-32601);
        assertThat(response.get("error").get("message").asText()).isEqualTo("Method not found: prompts/list");
        assertThat(response.has("result")).isFalse();
    }

    @Test
    void errorWithNullIdSerializesJsonNull() {
        ObjectNode response = JsonRpc.error(mapper, null, JsonRpc.PARSE_ERROR, "Parse error");

        assertThat(response.has("id")).isTrue();
        assertThat(response.get("id").isNull()).isTrue();
        assertThat(response.get("error").get("code").asInt()).isEqualTo(-32700);
    }

    @Test
    void aMethodWithoutAnIdIsANotification() {
        ObjectNode message = mapper.createObjectNode();
        message.put("method", "notifications/initialized");
        assertThat(JsonRpc.isNotification(message)).isTrue();
    }

    @Test
    void anExplicitNullIdIsStillANotification() {
        ObjectNode message = mapper.createObjectNode();
        message.put("method", "notifications/initialized");
        message.putNull("id");
        assertThat(JsonRpc.isNotification(message)).isTrue();
    }

    @Test
    void aMethodWithAnIdIsARequest() {
        ObjectNode message = mapper.createObjectNode();
        message.put("method", "ping");
        message.put("id", 1);
        assertThat(JsonRpc.isNotification(message)).isFalse();
    }

    @Test
    void aMessageWithoutAMethodIsNotANotification() {
        ObjectNode message = mapper.createObjectNode();
        message.put("id", 1);
        assertThat(JsonRpc.isNotification(message)).isFalse();
    }
}
