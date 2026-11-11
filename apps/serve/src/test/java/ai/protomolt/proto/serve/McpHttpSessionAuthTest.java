package ai.protomolt.proto.serve;

import static org.assertj.core.api.Assertions.assertThat;

import ai.protomolt.proto.actions.ActionCatalog;
import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.actions.Caller;
import ai.protomolt.proto.actions.ProtoAction;
import ai.protomolt.proto.actions.Scopes;
import ai.protomolt.proto.authz.CallerResolver;
import ai.protomolt.proto.mcp.McpServer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Message;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Per-request authentication on the streamable HTTP MCP transport (#845). The session id
 * alone never authorizes: each request's credential is resolved again, a credential the
 * resolver no longer knows is refused with 401, a valid credential for a different
 * principal is refused with 403 on a session it did not open, and the scopes the
 * credential resolves to now are the ones the request runs with.
 */
class McpHttpSessionAuthTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String OPERATOR_TOKEN = "operator-secret";
    private static final String ALICE_TOKEN = "alice-secret";
    private static final String ALICE_ROTATED_TOKEN = "alice-rotated-secret";
    private static final String BOB_TOKEN = "bob-secret";
    private static final String NAMED_OPERATOR_TOKEN = "named-operator-secret";
    private static final String CAROL_TOKEN = "carol-secret";

    private final Map<String, Caller> credentials = new ConcurrentHashMap<>();
    private final AtomicInteger executions = new AtomicInteger();
    private McpHttpHandler handler;
    private HttpServer local;
    private ExecutorService executor;
    private HttpClient client;
    private String url;

    @BeforeEach
    void start() throws Exception {
        credentials.put(ALICE_TOKEN, Caller.scoped("alice", Set.of(Scopes.SCHEMA_READ)));
        credentials.put(ALICE_ROTATED_TOKEN,
                Caller.scoped("alice", Set.of(Scopes.SCHEMA_READ)));
        credentials.put(BOB_TOKEN, Caller.scoped("bob", Set.of(Scopes.SCHEMA_READ)));
        // A policy principal that happens to share the operator's name holds no
        // operator authority and must not pass for the operator.
        credentials.put(NAMED_OPERATOR_TOKEN,
                Caller.scoped("operator", Set.of(Scopes.SCHEMA_READ)));
        CallerResolver resolver = credential -> Optional.ofNullable(credentials.get(credential));

        ActionCatalog catalog = ActionCatalog.defaults(ActionContext.create())
                .register(new WhoAmI());
        handler = new McpHttpHandler(new McpServer(catalog, null, "test", "0"),
                OPERATOR_TOKEN, resolver);
        local = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        executor = Executors.newCachedThreadPool();
        local.setExecutor(executor);
        local.createContext("/mcp", handler);
        local.start();
        client = HttpClient.newHttpClient();
        url = "http://127.0.0.1:" + local.getAddress().getPort() + "/mcp";
    }

    @AfterEach
    void stop() {
        handler.close();
        local.stop(0);
        executor.shutdownNow();
    }

    @Test
    void aRevokedCredentialIsRefusedOnItsNextRequestDespiteALiveSession() throws Exception {
        Session alice = open(ALICE_TOKEN);
        assertThat(readWorkspace(alice, ALICE_TOKEN).statusCode()).isEqualTo(200);

        credentials.remove(ALICE_TOKEN);

        for (HttpResponse<String> refused : List.of(
                readWorkspace(alice, ALICE_TOKEN),
                post(alice, ALICE_TOKEN, """
                        {"jsonrpc":"2.0","id":31,"method":"resources/list","params":{}}
                        """),
                callWhoAmI(alice, ALICE_TOKEN))) {
            assertThat(refused.statusCode()).isEqualTo(401);
            assertThat(MAPPER.readTree(refused.body()).has("result")).isFalse();
        }
        assertThat(executions).hasValue(0);
    }

    @Test
    void anotherPrincipalsCredentialCannotDriveTheSession() throws Exception {
        Session alice = open(ALICE_TOKEN);

        assertForeign(callWhoAmI(alice, BOB_TOKEN));
        assertForeign(readWorkspace(alice, BOB_TOKEN));
        assertForeign(post(alice, BOB_TOKEN, """
                [{"jsonrpc":"2.0","id":41,"method":"tools/call","params":
                  {"name":"whoami","arguments":{}}}]
                """));
        assertForeign(delete(alice, BOB_TOKEN));
        assertThat(executions).hasValue(0);

        // The refused DELETE left the session alone; its owner still runs as itself.
        HttpResponse<String> own = callWhoAmI(alice, ALICE_TOKEN);
        assertThat(own.statusCode()).isEqualTo(200);
        assertThat(whoami(own)).isEqualTo("alice");
    }

    @Test
    void theOperatorTokenCannotDriveAScopedPrincipalsSession() throws Exception {
        Session alice = open(ALICE_TOKEN);

        assertForeign(callWhoAmI(alice, OPERATOR_TOKEN));
        assertForeign(readWorkspace(alice, OPERATOR_TOKEN));
        assertThat(executions).hasValue(0);
    }

    @Test
    void aScopedCredentialCannotDriveTheOperatorsSession() throws Exception {
        Session operator = open(OPERATOR_TOKEN);

        assertForeign(callWhoAmI(operator, ALICE_TOKEN));
        assertForeign(callWhoAmI(operator, NAMED_OPERATOR_TOKEN));
        assertForeign(readWorkspace(operator, NAMED_OPERATOR_TOKEN));
        assertThat(executions).hasValue(0);

        HttpResponse<String> own = callWhoAmI(operator, OPERATOR_TOKEN);
        assertThat(own.statusCode()).isEqualTo(200);
        assertThat(whoami(own)).isEqualTo("operator");
    }

    @Test
    void aSecondCredentialForTheSamePrincipalKeepsTheSession() throws Exception {
        Session alice = open(ALICE_TOKEN);

        HttpResponse<String> rotated = callWhoAmI(alice, ALICE_ROTATED_TOKEN);
        assertThat(rotated.statusCode()).isEqualTo(200);
        assertThat(whoami(rotated)).isEqualTo("alice");
        assertThat(delete(alice, ALICE_ROTATED_TOKEN).statusCode()).isEqualTo(204);
    }

    @Test
    void aNarrowedScopeAppliesFromTheNextRequest() throws Exception {
        Session alice = open(ALICE_TOKEN);
        assertThat(whoami(callWhoAmI(alice, ALICE_TOKEN))).isEqualTo("alice");
        assertThat(executions).hasValue(1);

        credentials.put(ALICE_TOKEN, Caller.scoped("alice", Set.of(Scopes.SEARCH_QUERY)));

        assertThat(toolNames(alice, ALICE_TOKEN)).doesNotContain("whoami");
        HttpResponse<String> denied = callWhoAmI(alice, ALICE_TOKEN);
        assertThat(denied.statusCode()).isEqualTo(200);
        JsonNode result = MAPPER.readTree(denied.body()).path("result");
        assertThat(result.path("isError").asBoolean()).isTrue();
        assertThat(result.path("content").get(0).path("text").asText())
                .contains(Scopes.SCHEMA_READ);
        assertThat(executions).hasValue(1);
    }

    @Test
    void aWidenedScopeAppliesFromTheNextRequest() throws Exception {
        credentials.put(CAROL_TOKEN, Caller.scoped("carol", Set.of(Scopes.SEARCH_QUERY)));
        Session carol = open(CAROL_TOKEN);
        assertThat(toolNames(carol, CAROL_TOKEN)).doesNotContain("whoami");

        credentials.put(CAROL_TOKEN, Caller.scoped("carol",
                Set.of(Scopes.SEARCH_QUERY, Scopes.SCHEMA_READ)));

        assertThat(toolNames(carol, CAROL_TOKEN)).contains("whoami");
        assertThat(whoami(callWhoAmI(carol, CAROL_TOKEN))).isEqualTo("carol");
    }

    @Test
    void aForeignLookupDoesNotRefreshSessionRecencyBeforeThe403() throws Exception {
        Session alice = open(ALICE_TOKEN);
        Session second = open(BOB_TOKEN);
        for (int i = 2; i < McpHttpHandler.MAX_SESSIONS; i++) {
            initializeOnly(BOB_TOKEN);
        }

        assertForeign(callWhoAmI(alice, BOB_TOKEN));
        assertForeign(post(alice, BOB_TOKEN, """
                [{"jsonrpc":"2.0","id":41,"method":"ping"}]
                """));
        assertForeign(delete(alice, BOB_TOKEN));
        assertThat(get(alice, BOB_TOKEN).statusCode()).isEqualTo(405);

        initializeOnly(BOB_TOKEN);

        assertThat(callWhoAmI(alice, ALICE_TOKEN).statusCode()).isEqualTo(404);
        HttpResponse<String> surviving = callWhoAmI(second, BOB_TOKEN);
        assertThat(surviving.statusCode()).isEqualTo(200);
        assertThat(whoami(surviving)).isEqualTo("bob");
    }

    private List<String> toolNames(Session session, String token) throws Exception {
        HttpResponse<String> listed = post(session, token, """
                {"jsonrpc":"2.0","id":51,"method":"tools/list","params":{}}
                """);
        assertThat(listed.statusCode()).isEqualTo(200);
        return MAPPER.readTree(listed.body()).path("result").path("tools")
                .findValuesAsText("name");
    }

    private static void assertForeign(HttpResponse<String> response) throws Exception {
        assertThat(response.statusCode()).isEqualTo(403);
        JsonNode body = MAPPER.readTree(response.body());
        assertThat(body.has("result")).isFalse();
        assertThat(body.path("error").path("message").asText())
                .isEqualTo("MCP session was opened by a different principal");
        assertThat(response.headers().firstValue("Mcp-Session-Id")).isEmpty();
    }

    private Session open(String token) throws Exception {
        Session session = initializeOnly(token);
        assertThat(post(session, token, """
                {"jsonrpc":"2.0","method":"notifications/initialized"}
                """).statusCode()).isEqualTo(202);
        return session;
    }

    private Session initializeOnly(String token) throws Exception {
        HttpResponse<String> init = client.send(request("""
                {"jsonrpc":"2.0","id":1,"method":"initialize","params":
                 {"protocolVersion":"2025-06-18","capabilities":{},
                  "clientInfo":{"name":"auth-test","version":"0"}}}
                """, "api_token", token), HttpResponse.BodyHandlers.ofString());
        assertThat(init.statusCode()).isEqualTo(200);
        return new Session(
                init.headers().firstValue("Mcp-Session-Id").orElseThrow(),
                MAPPER.readTree(init.body()).path("result").path("protocolVersion").asText());
    }

    private HttpResponse<String> readWorkspace(Session session, String token) throws Exception {
        return post(session, token, """
                {"jsonrpc":"2.0","id":21,"method":"resources/read","params":
                 {"uri":"protomolt://workspace"}}
                """);
    }

    private HttpResponse<String> callWhoAmI(Session session, String token) throws Exception {
        return post(session, token, """
                {"jsonrpc":"2.0","id":11,"method":"tools/call","params":
                 {"name":"whoami","arguments":{}}}
                """);
    }

    private static String whoami(HttpResponse<String> response) throws Exception {
        return MAPPER.readTree(response.body()).path("result").path("structuredContent")
                .path("caller").asText();
    }

    private HttpResponse<String> post(Session session, String token, String body)
            throws Exception {
        return client.send(request(body, "api_token", token,
                        "Mcp-Session-Id", session.id(), "MCP-Protocol-Version", session.version()),
                HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<String> delete(Session session, String token) throws Exception {
        return client.send(HttpRequest.newBuilder(URI.create(url))
                        .header("api_token", token)
                        .header("Mcp-Session-Id", session.id())
                        .header("MCP-Protocol-Version", session.version())
                        .DELETE().build(),
                HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<String> get(Session session, String token) throws Exception {
        return client.send(HttpRequest.newBuilder(URI.create(url))
                        .header("api_token", token)
                        .header("Mcp-Session-Id", session.id())
                        .header("MCP-Protocol-Version", session.version())
                        .GET().build(),
                HttpResponse.BodyHandlers.ofString());
    }

    private HttpRequest request(String body, String... headers) {
        HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(url))
                .header("content-type", "application/json")
                .header("accept", "application/json, text/event-stream")
                .POST(HttpRequest.BodyPublishers.ofString(body));
        for (int i = 0; i < headers.length; i += 2) {
            request.header(headers[i], headers[i + 1]);
        }
        return request.build();
    }

    private record Session(String id, String version) {
    }

    /** Answers with the name of the caller it ran as, and counts its runs. */
    private final class WhoAmI implements ProtoAction {

        @Override
        public String name() {
            return "whoami";
        }

        @Override
        public String description() {
            return "Names the caller the request ran as.";
        }

        @Override
        public String requiredScope() {
            return Scopes.SCHEMA_READ;
        }

        @Override
        public Descriptor requestType() {
            return Struct.getDescriptor();
        }

        @Override
        public Descriptor responseType() {
            return Struct.getDescriptor();
        }

        @Override
        public Message execute(Message request, ActionContext context) {
            throw new UnsupportedOperationException("runs only with a caller");
        }

        @Override
        public Message execute(Message request, ActionContext context, Caller caller) {
            executions.incrementAndGet();
            return Struct.newBuilder()
                    .putFields("caller", Value.newBuilder().setStringValue(caller.name()).build())
                    .build();
        }
    }
}
