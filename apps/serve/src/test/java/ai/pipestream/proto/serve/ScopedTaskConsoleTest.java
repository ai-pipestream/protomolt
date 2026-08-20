package ai.pipestream.proto.serve;

import static org.assertj.core.api.Assertions.assertThat;

import ai.pipestream.proto.actions.Scopes;
import ai.pipestream.proto.authz.AccessPolicy;
import ai.pipestream.proto.authz.AccessPolicyCallers;
import ai.pipestream.proto.authz.Principal;
import com.google.protobuf.util.JsonFormat;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Console sessions bound to principals: a policy credential logs in and its session runs
 * as that principal for its lifetime, a principal without worker-coordinate is refused the
 * task API by name, the console login token binds to the task-console identity rather than
 * the operator, and unknown credentials never get a session.
 */
class ScopedTaskConsoleTest {

    private static final String API_TOKEN = "process-api-token";
    private static final String CONSOLE_TOKEN = "console-token-with-at-least-32-characters";
    private static final String STEWARD = "steward-credential";
    private static final String READER = "reader-credential";

    private static ProtoMoltServe serve;
    private static HttpClient http;
    private static String base;

    @BeforeAll
    static void start() throws Exception {
        Path policyFile = Files.createTempDirectory("protomolt-console-authz")
                .resolve("policy.json");
        AccessPolicy policy = AccessPolicy.newBuilder()
                .addPrincipals(Principal.newBuilder()
                        .setName("steward")
                        .addCredentialSha256(AccessPolicyCallers.sha256Hex(STEWARD))
                        .addScopes(Scopes.WORKER_COORDINATE))
                .addPrincipals(Principal.newBuilder()
                        .setName("ci-reader")
                        .addCredentialSha256(AccessPolicyCallers.sha256Hex(READER))
                        .addScopes(Scopes.SCHEMA_READ))
                .build();
        Files.writeString(policyFile, JsonFormat.printer().print(policy));
        serve = ProtoMoltServe.start(new ProtoMoltServe.Options(
                "127.0.0.1", 0, 0, null, 0, API_TOKEN, false, null,
                null, List.of(), null, null, null, null,
                new ProtoMoltServe.TaskConsoleOptions(CONSOLE_TOKEN, Duration.ofHours(1)),
                null, policyFile));
        http = HttpClient.newHttpClient();
        base = "http://127.0.0.1:" + serve.httpPort();
    }

    @AfterAll
    static void stop() {
        serve.close();
    }

    private static HttpResponse<String> post(String path, String body, String cookie)
            throws Exception {
        HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(base + path))
                .header("content-type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body));
        if (cookie != null) {
            request.header("Cookie", cookie);
        }
        return http.send(request.build(), HttpResponse.BodyHandlers.ofString());
    }

    private static HttpResponse<String> get(String path, String cookie) throws Exception {
        HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(base + path)).GET();
        if (cookie != null) {
            request.header("Cookie", cookie);
        }
        return http.send(request.build(), HttpResponse.BodyHandlers.ofString());
    }

    private static String login(String credential) throws Exception {
        HttpResponse<String> login = post("/api/task-session",
                "{\"token\":\"" + credential + "\"}", null);
        assertThat(login.statusCode()).isEqualTo(200);
        String setCookie = login.headers().firstValue("set-cookie").orElseThrow();
        return setCookie.substring(0, setCookie.indexOf(';'));
    }

    @Test
    void aWorkerCoordinatePrincipalLogsInAndSteersTasks() throws Exception {
        String cookie = login(STEWARD);
        assertThat(get("/api/tasks", cookie).statusCode()).isEqualTo(200);
    }

    @Test
    void aPrincipalWithoutWorkerCoordinateIsRefusedByName() throws Exception {
        String cookie = login(READER);
        HttpResponse<String> denied = get("/api/tasks", cookie);
        assertThat(denied.statusCode()).isEqualTo(403);
        assertThat(denied.body())
                .contains("ci-reader").contains(Scopes.WORKER_COORDINATE);
    }

    @Test
    void theConsoleTokenBindsToTheConsoleIdentityAndStillSteers() throws Exception {
        String cookie = login(CONSOLE_TOKEN);
        assertThat(get("/api/tasks", cookie).statusCode()).isEqualTo(200);
    }

    @Test
    void anUnknownCredentialNeverGetsASession() throws Exception {
        HttpResponse<String> refused = post("/api/task-session",
                "{\"token\":\"guessed\"}", null);
        assertThat(refused.statusCode()).isEqualTo(401);
        assertThat(refused.headers().firstValue("set-cookie")).isEmpty();
    }

    @Test
    void theConsoleIdentityIsBoundedNotTheOperator() {
        TaskConsoleSessions sessions =
                TaskConsoleSessions.secured(CONSOLE_TOKEN, Duration.ofHours(1));
        var caller = sessions.loginCaller(CONSOLE_TOKEN);
        assertThat(caller.unrestricted())
                .as("the console token must never carry process authority")
                .isFalse();
        assertThat(caller.name()).isEqualTo("task-console");
        assertThat(caller.holds(Scopes.WORKER_COORDINATE)).isTrue();
        assertThat(caller.holds(Scopes.SCHEMA_WRITE)).isFalse();
    }

    @Test
    void theOperatorApiTokenIsNotABrowserLogin() throws Exception {
        HttpResponse<String> refused = post("/api/task-session",
                "{\"token\":\"" + API_TOKEN + "\"}", null);
        assertThat(refused.statusCode()).isEqualTo(401);
    }
}
