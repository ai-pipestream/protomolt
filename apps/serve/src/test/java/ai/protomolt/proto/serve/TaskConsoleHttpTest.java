package ai.protomolt.proto.serve;

import ai.protomolt.proto.delegation.DelegationBridge;
import ai.protomolt.proto.delegation.v1.AcceptanceCheck;
import ai.protomolt.proto.delegation.v1.CheckEvidence;
import ai.protomolt.proto.delegation.v1.CheckVerdict;
import ai.protomolt.proto.delegation.v1.CommitReference;
import ai.protomolt.proto.delegation.v1.CompletionCandidate;
import ai.protomolt.proto.delegation.v1.TaskSpec;
import ai.protomolt.proto.delegation.v1.WorkerCapability;
import ai.protomolt.proto.delegation.v1.WorkerHello;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.protobuf.util.Timestamps;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** HTTP acceptance tests for the task console's scoped browser boundary. */
class TaskConsoleHttpTest {

    private static final String API_TOKEN = "process-api-token";
    private static final String CONSOLE_TOKEN = "console-token-with-at-least-32-characters";
    private static final ObjectMapper JSON = new ObjectMapper();

    private static ProtoMoltServe serve;
    private static HttpClient http;
    private static String base;
    private static String cookie;
    private static String taskId;

    @BeforeAll
    static void start() throws Exception {
        serve = ProtoMoltServe.start(new ProtoMoltServe.Options(
                "127.0.0.1", 0, 0, null, 0, API_TOKEN, false, null,
                null, List.of(), null, null, null, null,
                new ProtoMoltServe.TaskConsoleOptions(CONSOLE_TOKEN, Duration.ofHours(1))));
        http = HttpClient.newHttpClient();
        base = "http://127.0.0.1:" + serve.httpPort();

        DelegationBridge bridge = serve.delegationBridge();
        bridge.registerWorker(WorkerHello.newBuilder()
                .setWorkerId("console-worker")
                .setProtocolVersion(1)
                .setProvider("scripted")
                .setModel("reviewer")
                .addCapabilities(WorkerCapability.newBuilder().setName("java-build"))
                .build());
        taskId = UUID.randomUUID().toString();
        bridge.offer("console-worker", taskId, TaskSpec.newBuilder()
                        .setObjective("Build the bounded task console")
                        .addAllowedScope("apps/console")
                        .addRequiredChecks(AcceptanceCheck.newBuilder()
                                .setName("unit-tests")
                                .setDescription("focused tests pass"))
                        .build(),
                Duration.ofMinutes(5), null);
        bridge.accept("console-worker", taskId, 1);
        bridge.progress("console-worker", taskId, 1, "wired the HTTP boundary");
        bridge.checkpoint("console-worker", taskId, 1, "console-stage-1",
                "ready for browser work", null);

        HttpResponse<String> login = post("/api/task-session",
                "{\"token\":\"" + CONSOLE_TOKEN + "\"}", null);
        assertThat(login.statusCode()).isEqualTo(200);
        String setCookie = login.headers().firstValue("set-cookie").orElseThrow();
        cookie = setCookie.substring(0, setCookie.indexOf(';'));
    }

    @AfterAll
    static void stop() {
        serve.close();
    }

    @Test
    void loginCreatesOnlyAScopedSecureBrowserSession() throws Exception {
        assertThat(get("/api/task-session", null).statusCode()).isEqualTo(401);
        assertThat(get("/api/tasks", null).statusCode()).isEqualTo(401);
        assertThat(get("/api/task-session-other", null).statusCode()).isEqualTo(404);
        assertThat(get("/api/tasks-other", cookie).statusCode()).isEqualTo(404);

        HttpResponse<String> bad = post("/api/task-session",
                "{\"token\":\"not-the-token\"}", null);
        assertThat(bad.statusCode()).isEqualTo(401);
        assertThat(bad.headers().firstValue("set-cookie")).isEmpty();
        assertThat(bad.body()).doesNotContain(CONSOLE_TOKEN).doesNotContain(API_TOKEN);

        HttpResponse<String> good = post("/api/task-session",
                "{\"token\":\"" + CONSOLE_TOKEN + "\"}", null);
        assertThat(good.statusCode()).isEqualTo(200);
        assertThat(good.headers().firstValue("set-cookie").orElseThrow())
                .contains("HttpOnly")
                .contains("Secure")
                .contains("SameSite=Strict")
                .doesNotContain(CONSOLE_TOKEN)
                .doesNotContain(API_TOKEN);
        assertThat(get("/api/task-session", cookie).statusCode()).isEqualTo(200);
    }

    @Test
    void tokenModeExposesTasksButNotTheGeneralBrowserProxies() throws Exception {
        assertThat(get("/console/tasks", null).statusCode()).isEqualTo(200);
        assertThat(get("/api/protomolt/subjects", cookie).statusCode()).isEqualTo(503);
        assertThat(get("/api/serve/health", cookie).statusCode()).isEqualTo(503);

        JsonNode tasks = body(get("/api/tasks", cookie));
        JsonNode task = tasks.path("tasks").get(0);
        assertThat(task.path("taskId").asText()).isEqualTo(taskId);
        assertThat(task.path("workerId").asText()).isEqualTo("console-worker");
        assertThat(task.path("objective").asText())
                .isEqualTo("Build the bounded task console");
        assertThat(task.path("phase").asText()).isEqualTo("leased");
        assertThat(task.path("lastProgressSeq").asInt()).isEqualTo(1);
        assertThat(task.path("lastCheckpointSeq").asInt()).isEqualTo(1);

        JsonNode workers = body(get("/api/tasks/workers", cookie));
        assertThat(workers.path("workers").get(0).path("workerId").asText())
                .isEqualTo("console-worker");
        assertThat(workers.path("workers").get(0).path("connected").asBoolean()).isTrue();
        assertThat(workers.path("workers").get(0).path("capabilities").get(0).asText())
                .isEqualTo("java-build");
    }

    @Test
    void detailAndCursorResumeReturnEachDurableFrameOnce() throws Exception {
        JsonNode detail = body(get("/api/tasks/" + taskId, cookie));
        assertThat(detail.path("task").path("taskId").asText()).isEqualTo(taskId);
        assertThat(detail.path("events").size()).isGreaterThanOrEqualTo(4);
        assertThat(detail.path("events").toString()).contains("console-stage-1");

        JsonNode first = body(get("/api/tasks/events?taskId=" + taskId
                + "&after=0&timeoutMs=0&maxEvents=256", cookie));
        assertThat(first.path("events").size()).isGreaterThanOrEqualTo(4);
        long cursor = first.path("cursor").asLong();

        JsonNode resumed = body(get("/api/tasks/events?taskId=" + taskId
                + "&after=" + cursor + "&timeoutMs=0", cookie));
        assertThat(resumed.path("events")).isEmpty();
        assertThat(resumed.path("cursor").asLong()).isEqualTo(cursor);
    }

    @Test
    void longPollWakesForUserGuidanceWithoutReplayingOldFrames() throws Exception {
        long cursor = body(get("/api/tasks/events?taskId=" + taskId
                + "&after=0&timeoutMs=0&maxEvents=256", cookie))
                .path("cursor").asLong();
        CompletableFuture<HttpResponse<String>> watching = http.sendAsync(
                request("/api/tasks/events?taskId=" + taskId + "&after=" + cursor
                        + "&timeoutMs=5000", cookie).GET().build(),
                HttpResponse.BodyHandlers.ofString());

        HttpResponse<String> sent = post("/api/tasks/" + taskId + "/messages", """
                {"recipient":"console-worker","kind":"guidance",
                 "text":"Keep the scoped session boundary intact"}
                """, cookie);
        assertThat(sent.statusCode()).isEqualTo(201);

        JsonNode update = body(watching.get(5, TimeUnit.SECONDS));
        assertThat(update.path("events")).hasSize(1);
        assertThat(update.path("events").get(0).toString())
                .contains("Keep the scoped session boundary intact");
        long nextCursor = update.path("cursor").asLong();
        assertThat(nextCursor).isGreaterThan(cursor);

        JsonNode resumed = body(get("/api/tasks/events?taskId=" + taskId
                + "&after=" + nextCursor + "&timeoutMs=0", cookie));
        assertThat(resumed.path("events")).isEmpty();
    }

    /**
     * The console session is the external reviewer the manual policy leaves
     * candidates pending for: a revision returns the task to its worker with the
     * reviewer's feedback on the transcript, acceptance closes it with a verdict,
     * and both demand the reviewer's words.
     */
    @Test
    void aConsoleReviewJudgesTheCandidateOnTheRecord() throws Exception {
        DelegationBridge bridge = serve.delegationBridge();
        String reviewed = UUID.randomUUID().toString();
        bridge.offer("console-worker", reviewed, TaskSpec.newBuilder()
                        .setObjective("Prove the review lane")
                        .addRequiredChecks(AcceptanceCheck.newBuilder()
                                .setName("unit-tests")
                                .setDescription("focused tests pass"))
                        .build(),
                Duration.ofMinutes(5), null);
        bridge.accept("console-worker", reviewed, 1);
        bridge.submitCandidate("console-worker", reviewed, candidate(1));

        // A judgement without a reason is refused by name.
        HttpResponse<String> unreasoned = post("/api/tasks/" + reviewed + "/review",
                "{\"decision\":\"accept\"}", cookie);
        assertThat(unreasoned.statusCode()).isEqualTo(400);
        assertThat(unreasoned.body()).contains("verdict");

        HttpResponse<String> revised = post("/api/tasks/" + reviewed + "/review", """
                {"decision":"revise","feedback":"tests cover the happy path only",
                 "failedChecks":["unit-tests"]}
                """, cookie);
        assertThat(revised.statusCode()).isEqualTo(200);
        assertThat(body(revised).path("phase").asText()).isEqualTo("leased");

        // The reviewer's feedback is now a recorded protocol fact.
        JsonNode detail = body(get("/api/tasks/" + reviewed, cookie));
        assertThat(detail.path("events").toString())
                .contains("tests cover the happy path only");

        // With no open candidate there is nothing to judge.
        HttpResponse<String> early = post("/api/tasks/" + reviewed + "/review",
                "{\"decision\":\"accept\",\"verdict\":\"fine\"}", cookie);
        assertThat(early.statusCode()).isEqualTo(409);

        bridge.submitCandidate("console-worker", reviewed, candidate(2));
        HttpResponse<String> accepted = post("/api/tasks/" + reviewed + "/review", """
                {"decision":"accept","verdict":"checks green and the diff is scoped"}
                """, cookie);
        assertThat(accepted.statusCode()).isEqualTo(200);
        assertThat(body(accepted).path("phase").asText()).isEqualTo("accepted");
        assertThat(body(get("/api/tasks/" + reviewed, cookie)).path("events").toString())
                .contains("checks green and the diff is scoped");
    }

    private static CompletionCandidate candidate(int revision) {
        return CompletionCandidate.newBuilder()
                .setAttempt(1)
                .setRevision(revision)
                .setSummary("console review fixture, revision " + revision)
                .addEvidence(CheckEvidence.newBuilder()
                        .setCheckName("unit-tests")
                        .setVerdict(CheckVerdict.CHECK_VERDICT_PASSED)
                        .setDetail("focused suite green")
                        .setRanAt(Timestamps.now()))
                .addCommits(CommitReference.newBuilder()
                        .setRepository("protomolt")
                        .setCommit("0123456789abcdef0123456789abcdef01234567")
                        .setSubject("prove the review lane"))
                .build();
    }

    /** The one coordinator move the browser could not make: offering a task. */
    @Test
    void aConsoleOfferLandsOnTheTranscriptWithItsContract() throws Exception {
        HttpResponse<String> offered = post("/api/tasks/offer", """
                {"workerId":"console-worker",
                 "objective":"Wire the offer lane",
                 "allowedScopes":["apps/console"],
                 "requiredChecks":[{"name":"unit-tests","description":"focused tests pass"}],
                 "leaseMinutes":10}
                """, cookie);
        assertThat(offered.statusCode()).isEqualTo(201);
        String offeredTask = body(offered).path("taskId").asText();
        assertThat(offeredTask).isNotBlank();

        JsonNode detail = body(get("/api/tasks/" + offeredTask, cookie));
        assertThat(detail.path("task").path("phase").asText()).isEqualTo("offered");
        assertThat(detail.path("task").path("objective").asText())
                .isEqualTo("Wire the offer lane");
        assertThat(detail.path("events").toString()).contains("unit-tests");

        // A worker the coordinator does not know is a state conflict, not a parse
        // failure: there is nobody to offer to.
        HttpResponse<String> unknown = post("/api/tasks/offer", """
                {"workerId":"nobody","objective":"x"}
                """, cookie);
        assertThat(unknown.statusCode()).isEqualTo(409);
    }

    /** Without signing configured, the record route refuses by naming what it needs. */
    @Test
    void anUnsignedServerRefusesRecordExportByName() throws Exception {
        HttpResponse<String> refused = post("/api/tasks/" + taskId + "/record", "{}", cookie);
        assertThat(refused.statusCode()).isEqualTo(503);
        assertThat(refused.body()).contains("PROTOMOLT_RECEIPT_KEY_FILE");
    }

    @Test
    void logoutRevokesOnlyThatBrowserSession() throws Exception {
        HttpResponse<String> login = post("/api/task-session",
                "{\"token\":\"" + CONSOLE_TOKEN + "\"}", null);
        String ownCookie = login.headers().firstValue("set-cookie").orElseThrow()
                .split(";", 2)[0];
        HttpResponse<String> logout = send(request("/api/task-session", ownCookie)
                .DELETE().build());
        assertThat(logout.statusCode()).isEqualTo(204);
        assertThat(logout.headers().firstValue("set-cookie").orElseThrow())
                .contains("Max-Age=0");
        assertThat(get("/api/tasks", ownCookie).statusCode()).isEqualTo(401);
        assertThat(get("/api/tasks", cookie).statusCode()).isEqualTo(200);
    }

    private static HttpResponse<String> get(String path, String session) throws Exception {
        return send(request(path, session).GET().build());
    }

    private static HttpResponse<String> post(String path, String json, String session)
            throws Exception {
        return send(request(path, session)
                .header("content-type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(json)).build());
    }

    private static HttpRequest.Builder request(String path, String session) {
        HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(base + path))
                .timeout(Duration.ofSeconds(10));
        if (session != null) {
            builder.header("cookie", session);
        }
        return builder;
    }

    private static HttpResponse<String> send(HttpRequest request) throws Exception {
        return http.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private static JsonNode body(HttpResponse<String> response) throws Exception {
        assertThat(response.statusCode()).isBetween(200, 299);
        return JSON.readTree(response.body());
    }
}
