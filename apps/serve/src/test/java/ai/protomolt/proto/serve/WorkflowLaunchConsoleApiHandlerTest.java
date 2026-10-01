package ai.protomolt.proto.serve;

import ai.protomolt.proto.actions.ActionCatalog;
import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.actions.ActionException;
import ai.protomolt.proto.actions.Caller;
import ai.protomolt.proto.actions.ProtoAction;
import ai.protomolt.proto.actions.Scopes;
import ai.protomolt.proto.authz.ConsoleSessions;
import ai.protomolt.proto.samples.starter.v1.WorkflowAcceptedCandidate;
import ai.protomolt.proto.workflow.authoring.v1.PrepareWorkflowLaunchInputRequest;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Empty;
import com.google.protobuf.Message;
import com.google.protobuf.Struct;
import com.google.protobuf.util.JsonFormat;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WorkflowLaunchConsoleApiHandlerTest {
    private static final String PREFIX = "/api/workflow-launch";
    private static final String TOKEN = "console-token-with-at-least-32-characters";
    private static final String[] ACTIONS = {
            "get-accepted-workflow", "get-workflow-launch-input-contract",
            "prepare-workflow-launch-input", "launch-accepted-workflow", "get-workflow-launch-status"
    };
    private final HttpClient client = HttpClient.newHttpClient();
    private final List<Call> calls = new CopyOnWriteArrayList<>();
    private final Map<String, String> failures = new ConcurrentHashMap<>();
    private HttpServer server;
    private ConsoleSessions sessions;
    private String base;

    @BeforeEach void start() throws Exception {
        sessions = TaskConsoleSessions.secured(TOKEN, Duration.ofHours(1), null);
        ActionCatalog catalog = ActionCatalog.defaults(ActionContext.create());
        for (String name : ACTIONS) catalog.register(new FakeAction(name));
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext(PREFIX, new WorkflowLaunchConsoleApiHandler(catalog, sessions));
        server.start();
        base = "http://127.0.0.1:" + server.getAddress().getPort();
    }

    @AfterEach void stop() {
        if (server != null) server.stop(0);
    }

    @Test void requiresSecureSessionAndBothScopesBeforeDispatch() throws Exception {
        assertThat(post("/accepted", "{}", null, base, null).statusCode()).isEqualTo(401);
        String ordinary = cookie(TaskConsoleSessions.CONSOLE);
        assertThat(post("/accepted", "{}", ordinary, base, null).statusCode()).isEqualTo(403);
        String author = cookie(Caller.scoped("author", Set.of(Scopes.WORKFLOW_AUTHOR)));
        assertThat(post("/accepted", "{}", author, base, null).statusCode()).isEqualTo(403);
        String launchOnly = cookie(Caller.scoped("launch-only", Set.of(Scopes.WORKFLOW_LAUNCH)));
        assertThat(post("/accepted", "{}", launchOnly, base, null).statusCode()).isEqualTo(403);
        assertThat(calls).isEmpty();
        assertThatThrownBy(() -> new WorkflowLaunchConsoleApiHandler(
                ActionCatalog.defaults(ActionContext.create()), TaskConsoleSessions.open()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test void fixedRoutesUseTheUnchangedSessionCaller() throws Exception {
        String cookie = cookie(Caller.scoped("browser-launcher",
                Set.of(Scopes.WORKER_COORDINATE, Scopes.WORKFLOW_LAUNCH)));
        for (String route : List.of("accepted", "contract", "prepare", "launch", "status")) {
            assertThat(post("/" + route, route.equals("prepare") ? smallPrepare() : "{}",
                    cookie, base, null).statusCode()).isEqualTo(200);
        }
        assertThat(calls.stream().map(Call::action)).containsExactly(ACTIONS);
        assertThat(calls).allSatisfy(call -> {
            assertThat(call.caller().name()).isEqualTo("browser-launcher");
            assertThat(call.caller().unrestricted()).isFalse();
            assertThat(call.caller().scopes()).containsExactlyInAnyOrder(
                    Scopes.WORKER_COORDINATE, Scopes.WORKFLOW_LAUNCH);
        });
        assertThat(post("/arbitrary", "{}", cookie, base, null).statusCode()).isEqualTo(404);
        assertThat(post("/accepted/more", "{}", cookie, base, null).statusCode()).isEqualTo(404);
        assertThat(get("/accepted", cookie).statusCode()).isEqualTo(405);
        assertThat(calls).hasSize(5);
    }

    @Test void rejectsCrossSiteMetadataWithoutTrustingForwardedHost() throws Exception {
        String cookie = launcher();
        assertThat(post("/launch", "{}", cookie, "https://evil.example", null).statusCode())
                .isEqualTo(403);
        assertThat(post("/launch", "{}", cookie, base, "cross-site").statusCode())
                .isEqualTo(403);
        assertThat(post("/launch", "{}", cookie, base, "same-site").statusCode())
                .isEqualTo(403);
        assertThat(post("/launch", "{}", cookie, "null", null).statusCode())
                .isEqualTo(403);
        // A TLS-terminating proxy forwards Host unchanged to this HTTP listener.
        assertThat(post("/launch", "{}", cookie,
                base.replace("http:", "https:"), "same-origin").statusCode()).isEqualTo(200);
        assertThat(calls).hasSize(1);
    }

    @Test void rejectsMalformedNonObjectAndOversizedBodiesBeforeDispatch() throws Exception {
        String cookie = launcher();
        assertThat(post("/launch", "{", cookie, base, null).statusCode()).isEqualTo(400);
        assertThat(post("/launch", "[]", cookie, base, null).statusCode()).isEqualTo(400);
        assertThat(post("/launch", "{} {}", cookie, base, null).statusCode()).isEqualTo(400);
        assertThat(post("/launch", "x".repeat(8 * 1024 * 1024 + 1),
                cookie, base, null).statusCode()).isEqualTo(413);
        assertThat(calls).isEmpty();
    }

    @Test void requiresJsonContentType() throws Exception {
        String cookie = launcher();
        var request = HttpRequest.newBuilder(URI.create(base + PREFIX + "/launch"))
                .header("Cookie", cookie).header("Content-Type", "text/plain")
                .POST(HttpRequest.BodyPublishers.ofString("{}"))
                .build();
        assertThat(client.send(request, HttpResponse.BodyHandlers.ofString()).statusCode())
                .isEqualTo(415);
        assertThat(calls).isEmpty();
    }

    @Test void acceptsAProtobufJsonEnvelopeLargerThanFiveMiBWithinTheEightMiBBound()
            throws Exception {
        var request = PrepareWorkflowLaunchInputRequest.newBuilder()
                .setAcceptance(WorkflowAcceptedCandidate.newBuilder()
                        .setTaskId("00000000-0000-4000-8000-000000000001")
                        .setAttempt(1).setRevision(1)
                        .setTaskSpecSha256("a".repeat(64))
                        .setCandidateSha256("b".repeat(64))
                        .setAcceptedEntrySha256("c".repeat(64)))
                .setInputJson(ByteString.copyFrom(new byte[4 * 1024 * 1024])).build();
        String body = JsonFormat.printer().print(request);
        assertThat(body.length()).isGreaterThan(5 * 1024 * 1024).isLessThan(8 * 1024 * 1024);
        assertThat(post("/prepare", body, launcher(), base, null).statusCode()).isEqualTo(200);
        assertThat(calls).hasSize(1);
        assertThat(calls.getFirst().request().getDescriptorForType().getFullName())
                .isEqualTo(PrepareWorkflowLaunchInputRequest.getDescriptor().getFullName());
        assertThat(PrepareWorkflowLaunchInputRequest.parseFrom(calls.getFirst().request().toByteArray())
                .getInputJson().size()).isEqualTo(4 * 1024 * 1024);
    }

    @Test void reportsStableCodesAndNeverExposesBackendMessages() throws Exception {
        String cookie = launcher();
        for (var expected : Map.of(
                "invalid-input", 400,
                "permission-denied", 403,
                "workflow-launch-conflict", 409,
                "workflow-authoring-rejected", 412,
                "resource-exhausted", 429,
                "invalid-upstream-response", 502,
                "workflow-authoring-unavailable", 503,
                "workflow-authoring-storage-failed", 503,
                "workflow-authoring-deadline", 504).entrySet()) {
            failures.put("get-accepted-workflow", expected.getKey());
            var response = post("/accepted", "{}", cookie, base, null);
            assertThat(response.statusCode()).isEqualTo(expected.getValue());
            assertThat(response.body()).contains(expected.getKey()).doesNotContain("private secret");
        }
    }

    private String launcher() {
        return cookie(Caller.scoped("browser-launcher",
                Set.of(Scopes.WORKER_COORDINATE, Scopes.WORKFLOW_LAUNCH)));
    }

    private String cookie(Caller caller) {
        return TaskConsoleSessions.COOKIE + "=" + sessions.issue(caller, "test-credential");
    }

    private HttpResponse<String> post(String path, String body, String cookie, String origin,
            String fetchSite) throws Exception {
        HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(base + PREFIX + path))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body));
        if (cookie != null) request.header("Cookie", cookie);
        if (origin != null) request.header("Origin", origin);
        if (fetchSite != null) request.header("Sec-Fetch-Site", fetchSite);
        return client.send(request.build(), HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<String> get(String path, String cookie) throws Exception {
        return client.send(HttpRequest.newBuilder(URI.create(base + PREFIX + path))
                .header("Cookie", cookie).GET().build(), HttpResponse.BodyHandlers.ofString());
    }

    private static String smallPrepare() {
        return "{\"acceptance\":{\"taskId\":\"00000000-0000-4000-8000-000000000001\","
                + "\"attempt\":1,\"revision\":1,\"taskSpecSha256\":\"" + "a".repeat(64)
                + "\",\"candidateSha256\":\"" + "b".repeat(64)
                + "\",\"acceptedEntrySha256\":\"" + "c".repeat(64)
                + "\"},\"inputJson\":\"e30=\"}";
    }

    private record Call(String action, Caller caller, Message request) {}

    private final class FakeAction implements ProtoAction {
        private final String action;

        FakeAction(String action) { this.action = action; }

        @Override public String name() { return action; }
        @Override public String description() { return "test workflow launch bridge"; }
        @Override public String requiredScope() { return Scopes.WORKFLOW_LAUNCH; }
        @Override public Descriptor requestType() {
            return action.equals("prepare-workflow-launch-input")
                    ? PrepareWorkflowLaunchInputRequest.getDescriptor() : Struct.getDescriptor();
        }
        @Override public Descriptor responseType() { return Empty.getDescriptor(); }
        @Override public Message execute(Message request, ActionContext context) {
            throw new AssertionError("the cookie bridge must pass the authenticated caller");
        }
        @Override public Message execute(Message request, ActionContext context, Caller caller)
                throws ActionException {
            if (failures.containsKey(action)) {
                throw new ActionException(failures.get(action), "private secret");
            }
            calls.add(new Call(action, caller, request));
            return Empty.getDefaultInstance();
        }
    }
}
