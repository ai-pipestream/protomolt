package ai.protomolt.proto.mcp;

import ai.protomolt.proto.actions.ActionCatalog;
import ai.protomolt.proto.actions.ActionContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class McpMoonshotDialectTest {

    private final ObjectMapper mapper = new ObjectMapper();
    private McpServer server;

    @BeforeEach
    void setUp() {
        server = new McpServer(ActionCatalog.defaults(ActionContext.create()), null,
                "protomolt-test", "0.0-test");
    }

    @Test
    void isMoonshotClientRecognizesKimiAndMoonshotNames() {
        assertThat(McpServer.isMoonshotClient("kimi-code")).isTrue();
        assertThat(McpServer.isMoonshotClient("kimi")).isTrue();
        assertThat(McpServer.isMoonshotClient("KIMI-CLI")).isTrue();
        assertThat(McpServer.isMoonshotClient("moonshot-agent")).isTrue();
        assertThat(McpServer.isMoonshotClient("moonshot")).isTrue();

        assertThat(McpServer.isMoonshotClient("claude-code")).isFalse();
        assertThat(McpServer.isMoonshotClient("cursor")).isFalse();
        assertThat(McpServer.isMoonshotClient("generic-client")).isFalse();
        assertThat(McpServer.isMoonshotClient("")).isFalse();
        assertThat(McpServer.isMoonshotClient(null)).isFalse();
    }

    @Test
    void sessionDetectsKimiClientOnInitialize() {
        McpServer.Session session = server.openSession();
        assertThat(session.isMoonshotDialect()).isFalse();

        ObjectNode initParams = mapper.createObjectNode();
        initParams.put("protocolVersion", McpServer.PROTOCOL_VERSION);
        initParams.putObject("clientInfo").put("name", "kimi-code");

        ObjectNode initRequest = mapper.createObjectNode();
        initRequest.put("jsonrpc", "2.0");
        initRequest.put("id", 1);
        initRequest.put("method", "initialize");
        initRequest.set("params", initParams);

        Optional<ObjectNode> response = session.handle(initRequest);
        assertThat(response).isPresent();
        assertThat(response.get().has("result")).isTrue();
        assertThat(session.isMoonshotDialect()).isTrue();
    }

    @Test
    void sessionRetainsStandardDialectForNonKimiClients() {
        McpServer.Session session = server.openSession();

        ObjectNode initParams = mapper.createObjectNode();
        initParams.put("protocolVersion", McpServer.PROTOCOL_VERSION);
        initParams.putObject("clientInfo").put("name", "claude-code");

        ObjectNode initRequest = mapper.createObjectNode();
        initRequest.put("jsonrpc", "2.0");
        initRequest.put("id", 1);
        initRequest.put("method", "initialize");
        initRequest.set("params", initParams);

        session.handle(initRequest);
        assertThat(session.isMoonshotDialect()).isFalse();
    }

    @Test
    void sanitizeSchemaForMoonshotRemovesParentTypeBesideAnyOf() {
        ObjectNode property = mapper.createObjectNode();
        ArrayNode typeArray = property.putArray("type");
        typeArray.add("integer");
        typeArray.add("string");
        property.put("pattern", "^-?[0-9]+$");
        property.put("minimum", 0);

        ArrayNode anyOf = property.putArray("anyOf");
        anyOf.addObject().put("type", "integer").put("minimum", 0);
        anyOf.addObject().put("type", "string").put("pattern", "^(?:0|[1-9][0-9]*)$");

        McpServer.sanitizeSchemaForMoonshot(property);

        assertThat(property.has("type")).isFalse();
        assertThat(property.has("pattern")).isFalse();
        assertThat(property.has("minimum")).isFalse();
        assertThat(property.has("anyOf")).isTrue();
        assertThat(property.get("anyOf")).hasSize(2);
        assertThat(property.get("anyOf").get(0).get("type").asText()).isEqualTo("integer");
        assertThat(property.get("anyOf").get(1).get("type").asText()).isEqualTo("string");
    }

    @Test
    void sanitizeSchemaForMoonshotPushesParentTypeIntoSubschemasWhenMissing() {
        ObjectNode schema = mapper.createObjectNode();
        schema.put("type", "object");
        ArrayNode oneOf = schema.putArray("oneOf");
        oneOf.addObject().putArray("required").add("fieldA");
        oneOf.addObject().putArray("required").add("fieldB");

        McpServer.sanitizeSchemaForMoonshot(schema);

        assertThat(schema.has("type")).isFalse();
        assertThat(schema.get("oneOf").get(0).get("type").asText()).isEqualTo("object");
        assertThat(schema.get("oneOf").get(1).get("type").asText()).isEqualTo("object");
    }

    @Test
    void sanitizeSchemaForMoonshotPushesTextualParentTypeIntoAnyOfBranches() {
        ObjectNode property = mapper.createObjectNode();
        property.put("type", "string");
        ArrayNode anyOf = property.putArray("anyOf");
        anyOf.addObject().put("format", "date-time");
        anyOf.addObject().put("type", "string").put("pattern", "^[a-z]+$");

        McpServer.sanitizeSchemaForMoonshot(property);

        assertThat(property.has("type")).isFalse();
        // The branch with its own type keeps it; the branch without one inherits the parent's.
        assertThat(property.get("anyOf").get(0).get("type").asText()).isEqualTo("string");
        assertThat(property.get("anyOf").get(1).get("type").asText()).isEqualTo("string");
        assertThat(property.get("anyOf").get(1).get("pattern").asText()).isEqualTo("^[a-z]+$");
    }

    @Test
    void sanitizeForMoonshotReturnsNullForNull() {
        assertThat(McpServer.sanitizeForMoonshot(null)).isNull();
    }

    @Test
    void sanitizeForMoonshotDoesNotMutateTheInputManifest() {
        ArrayNode manifest = mapper.createArrayNode();
        ObjectNode tool = manifest.addObject();
        tool.put("name", "example");
        ObjectNode inputSchema = tool.putObject("inputSchema");
        inputSchema.put("type", "object");
        inputSchema.putArray("oneOf").addObject().putArray("required").add("fieldA");
        String before = manifest.toString();

        ArrayNode sanitized = McpServer.sanitizeForMoonshot(manifest);

        assertThat(manifest.toString()).isEqualTo(before);
        assertThat(sanitized).isNotSameAs(manifest);
        assertThat(sanitized.get(0).get("inputSchema").has("type")).isFalse();
    }

    @Test
    void toolsListAppliesSanitizationOnlyWhenMoonshotDialectActive() {
        McpServer.Session kimiSession = server.openSession();
        ObjectNode kimiInit = mapper.createObjectNode();
        kimiInit.put("protocolVersion", McpServer.PROTOCOL_VERSION);
        kimiInit.putObject("clientInfo").put("name", "kimi-code");
        kimiSession.handle(request(1, "initialize", kimiInit));

        ObjectNode readyNotif = mapper.createObjectNode();
        readyNotif.put("jsonrpc", "2.0");
        readyNotif.put("method", "notifications/initialized");
        kimiSession.handle(readyNotif);

        ObjectNode kimiToolsRes = kimiSession.handle(request(2, "tools/list", null)).orElseThrow();
        ArrayNode kimiTools = (ArrayNode) kimiToolsRes.get("result").get("tools");

        // Verify that NO tool in kimiTools has both type and anyOf/oneOf on the same object
        assertNoParentTypeBesideUnion(kimiTools);

        // In contrast, direct handle without moonshot dialect preserves the standard schema.
        // The standalone catalog carries no parent-type-beside-union nodes, so the end-to-end
        // sanitization proof with the full catalog lives in McpHttpTest (apps/serve).
        ObjectNode standardToolsRes = server.handle(request(3, "tools/list", null)).orElseThrow();
        ArrayNode standardTools = (ArrayNode) standardToolsRes.get("result").get("tools");
        assertThat(standardTools).isNotEmpty();
    }

    private void assertNoParentTypeBesideUnion(JsonNode node) {
        if (node.isObject()) {
            if (node.has("anyOf") || node.has("oneOf")) {
                assertThat(node.has("type"))
                        .withFailMessage("Found parent 'type' beside anyOf/oneOf in: " + node)
                        .isFalse();
            }
            node.properties().forEach(entry -> assertNoParentTypeBesideUnion(entry.getValue()));
        } else if (node.isArray()) {
            node.forEach(this::assertNoParentTypeBesideUnion);
        }
    }

    private ObjectNode request(int id, String method, ObjectNode params) {
        ObjectNode message = mapper.createObjectNode();
        message.put("jsonrpc", "2.0");
        message.put("id", id);
        message.put("method", method);
        if (params != null) {
            message.set("params", params);
        }
        return message;
    }
}
