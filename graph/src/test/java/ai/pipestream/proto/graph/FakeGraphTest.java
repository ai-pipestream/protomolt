package ai.pipestream.proto.graph;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The whole client stack against a fake Graph on a local JDK server: both token flows,
 * bearer propagation, file round trips including list-item metadata, the async
 * schema-registration handshake, external item PUT, throttling honored, and error mapping.
 * Every request the fake sees must carry the bearer token — a request without one fails the
 * test, not just the call.
 */
class FakeGraphTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    private static HttpServer server;
    private static String base;
    private static final Map<String, byte[]> uploads = new ConcurrentHashMap<>();
    private static final Map<String, String> itemFields = new ConcurrentHashMap<>();
    private static final Map<String, String> externalItems = new ConcurrentHashMap<>();
    private static final AtomicInteger throttleCount = new AtomicInteger();
    private static final AtomicInteger schemaPolls = new AtomicInteger();
    private static final AtomicInteger devicePolls = new AtomicInteger();

    @BeforeAll
    static void start() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        base = "http://127.0.0.1:" + server.getAddress().getPort();

        // ---- token endpoints -------------------------------------------------------
        server.createContext("/tenant-1/oauth2/v2.0/token", exchange -> {
            String body = new String(exchange.getRequestBody().readAllBytes(),
                    StandardCharsets.UTF_8);
            ObjectNode response = JSON.createObjectNode();
            if (body.contains("client_credentials")) {
                assertThat(body).contains("client_secret=s3cret");
                response.put("access_token", "app-token").put("expires_in", 3600);
            } else if (body.contains("device_code")) {
                if (devicePolls.incrementAndGet() < 2) {
                    response.put("error", "authorization_pending");
                } else {
                    response.put("access_token", "user-token")
                            .put("refresh_token", "refresh-1")
                            .put("expires_in", 3600);
                }
            }
            respond(exchange, 200, response.toString());
        });
        server.createContext("/tenant-1/oauth2/v2.0/devicecode", exchange -> {
            ObjectNode response = JSON.createObjectNode()
                    .put("device_code", "dc-1").put("user_code", "ABCD-1234")
                    .put("verification_uri", "https://microsoft.com/devicelogin")
                    .put("message", "go log in").put("interval", 1).put("expires_in", 60);
            respond(exchange, 200, response.toString());
        });

        // ---- graph routes (all require the bearer) ---------------------------------
        graph("/v1.0/me", exchange -> JSON.createObjectNode()
                .put("displayName", "Pat").put("userPrincipalName", "pat@x.example")
                .toString());
        graph("/v1.0/throttled", exchange -> {
            if (throttleCount.incrementAndGet() < 3) {
                exchange.getResponseHeaders().set("Retry-After", "1");
                respond(exchange, 429, "{\"error\":{\"code\":\"tooManyRequests\"}}");
                return null;
            }
            return "{\"ok\": true}";
        });
        graph("/v1.0/forbidden", exchange -> {
            respond(exchange, 403, "{\"error\":{\"code\":\"accessDenied\","
                    + "\"message\":\"Insufficient privileges\"}}");
            return null;
        });
        server.createContext("/v1.0/drives/d1/", exchange -> {
            requireBearer(exchange);
            String path = exchange.getRequestURI().getPath();
            String method = exchange.getRequestMethod();
            if (path.endsWith(":/content") && method.equals("PUT")) {
                uploads.put(path, exchange.getRequestBody().readAllBytes());
                respond(exchange, 201, "{\"id\": \"item-9\", \"name\": \"up.bin\"}");
            } else if (path.endsWith("/items/item-9/content")) {
                byte[] stored = uploads.values().iterator().next();
                exchange.sendResponseHeaders(200, stored.length);
                exchange.getResponseBody().write(stored);
                exchange.close();
            } else if (path.endsWith("/listItem") || path.contains("listItem?")) {
                respond(exchange, 200, "{\"fields\": {\"Title\": \""
                        + itemFields.getOrDefault("Title", "original") + "\"}}");
            } else if (path.endsWith("/listItem/fields") && method.equals("PATCH")) {
                JsonNode patch = JSON.readTree(exchange.getRequestBody());
                itemFields.put("Title", patch.path("Title").asText());
                respond(exchange, 200, patch.toString());
            } else {
                respond(exchange, 404, "{}");
            }
        });
        server.createContext("/v1.0/external/connections", exchange -> {
            requireBearer(exchange);
            String path = exchange.getRequestURI().getPath();
            String method = exchange.getRequestMethod();
            if (path.endsWith("/schema") && method.equals("POST")) {
                exchange.getResponseHeaders().set("Location",
                        base + "/v1.0/external/connections/conn1/operations/op1");
                exchange.sendResponseHeaders(202, -1);
                exchange.close();
            } else if (path.endsWith("/operations/op1")) {
                respond(exchange, 200, schemaPolls.incrementAndGet() < 2
                        ? "{\"status\": \"inprogress\"}" : "{\"status\": \"completed\"}");
            } else if (path.contains("/items/") && method.equals("PUT")) {
                externalItems.put(path, new String(exchange.getRequestBody().readAllBytes(),
                        StandardCharsets.UTF_8));
                respond(exchange, 200, "{}");
            } else if (method.equals("POST")) {
                JsonNode body = JSON.readTree(exchange.getRequestBody());
                respond(exchange, 201, JSON.createObjectNode()
                        .put("id", body.path("id").asText())
                        .put("state", "draft").toString());
            } else {
                respond(exchange, 200, "{\"value\": []}");
            }
        });
        server.start();
    }

    @AfterAll
    static void stop() {
        server.stop(0);
    }

    private interface Handler {
        String handle(HttpExchange exchange) throws IOException;
    }

    private static void graph(String path, Handler handler) {
        server.createContext(path, exchange -> {
            requireBearer(exchange);
            String body = handler.handle(exchange);
            if (body != null) {
                respond(exchange, 200, body);
            }
        });
    }

    private static void requireBearer(HttpExchange exchange) {
        assertThat(exchange.getRequestHeaders().getFirst("authorization"))
                .as("every Graph request carries the bearer")
                .isNotNull()
                .startsWith("Bearer ");
    }

    private static void respond(HttpExchange exchange, int status, String body)
            throws IOException {
        FakeGraphSupport.respond(exchange, status, body);
    }

    private static GraphClient client(String token) {
        return new GraphClient(base + "/v1.0", () -> token);
    }

    // --------------------------------------------------------------------- the tests

    @Test
    void clientCredentialsFlowIssuesAnAppToken() throws Exception {
        GraphAuth auth = new GraphAuth(new GraphAuth.Config(base, "tenant-1", "app-1", "s3cret"));
        GraphAuth.Token token = auth.clientCredentials();
        assertThat(token.accessToken()).isEqualTo("app-token");
        assertThat(token.refreshToken()).isNull();
        assertThat(token.expired()).isFalse();
    }

    @Test
    void deviceCodeFlowPromptsThenPollsToAToken() throws Exception {
        GraphAuth auth = new GraphAuth(new GraphAuth.Config(base, "tenant-1", "app-1", null));
        StringBuilder prompted = new StringBuilder();
        GraphAuth.Token token = auth.deviceCode("User.Read",
                prompt -> prompted.append(prompt.userCode()));
        assertThat(prompted.toString()).isEqualTo("ABCD-1234");
        assertThat(token.accessToken()).isEqualTo("user-token");
        assertThat(token.refreshToken()).isEqualTo("refresh-1");
    }

    @Test
    void filesRoundTripUploadDownloadAndMetadataPatch() throws Exception {
        GraphFiles files = new GraphFiles(client("t"));
        assertThat(files.me().path("displayName").asText()).isEqualTo("Pat");

        byte[] content = "hello graph".getBytes(StandardCharsets.UTF_8);
        JsonNode uploaded = files.upload("d1", "/docs", "up.bin", content, "text/plain");
        assertThat(uploaded.path("id").asText()).isEqualTo("item-9");
        assertThat(files.download("d1", "item-9")).isEqualTo(content);

        ObjectNode patch = GraphClient.object().put("Title", "Quarterly report");
        files.updateListItemFields("d1", "item-9", patch);
        assertThat(files.listItemFields("d1", "item-9")
                .path("fields").path("Title").asText()).isEqualTo("Quarterly report");
    }

    @Test
    void connectionLifecycleSchemaHandshakeAndItemPut() throws Exception {
        GraphConnections connections = new GraphConnections(client("t"));
        JsonNode created = connections.create("conn1", "ProtoMolt", "test connection");
        assertThat(created.path("id").asText()).isEqualTo("conn1");

        ObjectNode schema = GraphClient.object().put("baseType",
                "microsoft.graph.externalItem");
        connections.registerSchema("conn1", schema, Duration.ofSeconds(30));
        assertThat(schemaPolls.get()).isGreaterThanOrEqualTo(2); // 202 + polled to completed

        ObjectNode properties = GraphClient.object().put("title", "Order 42");
        connections.putItem("conn1", "item-42", properties, "full text",
                GraphConnections.everyoneAcl());
        String stored = externalItems.values().iterator().next();
        assertThat(stored).contains("\"title\":\"Order 42\"")
                .contains("\"type\":\"everyone\"")
                .contains("full text");
    }

    @Test
    void throttlingIsRetriedAfterTheServerDirectedDelay() throws Exception {
        JsonNode ok = client("t").get("/throttled");
        assertThat(ok.path("ok").asBoolean()).isTrue();
        assertThat(throttleCount.get()).isEqualTo(3);
    }

    @Test
    void graphErrorsCarryTheServiceCodeAndStatus() {
        assertThatThrownBy(() -> client("t").get("/forbidden"))
                .isInstanceOfSatisfying(GraphClient.GraphApiException.class, e -> {
                    assertThat(e.status()).isEqualTo(403);
                    assertThat(e.code()).isEqualTo("accessDenied");
                    assertThat(e.getMessage()).contains("Insufficient privileges");
                    // The verbose probe lane prints this raw body so a tenant-provisioning
                    // failure can be told apart from a genuine permissions denial.
                    assertThat(e.body()).contains("accessDenied").contains("Insufficient privileges");
                });
    }
}
