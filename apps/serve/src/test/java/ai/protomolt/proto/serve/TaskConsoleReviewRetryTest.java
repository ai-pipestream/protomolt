package ai.protomolt.proto.serve;

import ai.protomolt.proto.actions.Caller;
import ai.protomolt.proto.actions.Scopes;
import ai.protomolt.proto.authz.ConsoleSessions;
import ai.protomolt.proto.delegation.*;
import ai.protomolt.proto.delegation.v1.*;
import ai.protomolt.proto.grpc.workflow.v1.ArtifactReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class TaskConsoleReviewRetryTest {
    private static final ObjectMapper JSON = new ObjectMapper();
    private final HttpClient http = HttpClient.newHttpClient();
    private String base;

    @Test
    void scopedRetryUsesTheRecordedIdentityAndReplaysAfterAcceptance() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        try (var coordinator = new InProcessDelegationCoordinator(AdmissionPolicy.allowAll(), context -> {
            if (calls.incrementAndGet() == 1) throw new IOException("private backend diagnostic");
            return CandidateReviewer.ReviewDecision.accept("verified");
        }); var bridge = new DelegationBridge(coordinator)) {
            var sessions = ConsoleSessions.secured(TaskConsoleSessions.COOKIE, Duration.ofMinutes(5), token ->
                    switch (token) {
                        case "steward" -> Optional.of(Caller.scoped("steward", Set.of(Scopes.WORKER_COORDINATE)));
                        case "author" -> Optional.of(Caller.scoped("author", Set.of(Scopes.WORKFLOW_AUTHOR)));
                        default -> Optional.empty();
                    });
            var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext("/api/tasks", new TaskConsoleApiHandler(bridge, sessions));
            server.createContext("/api/task-session", new TaskConsoleSessionHandler(sessions));
            server.start();
            base = "http://127.0.0.1:" + server.getAddress().getPort();
            try {
                String steward = login("steward");
                String author = login("author");
                bridge.registerWorker(WorkerHello.newBuilder().setWorkerId("retry-worker")
                        .setProtocolVersion(1).setProvider("scripted").build());
                String task = UUID.randomUUID().toString();
                bridge.offer("retry-worker", task, TaskSpec.newBuilder().setObjective("Prove retry")
                                .addRequiredChecks(AcceptanceCheck.newBuilder().setName("verified"))
                                .build(),
                        Duration.ofMinutes(5), null);
                bridge.accept("retry-worker", task, 1);
                bridge.submitCandidate("retry-worker", task, CompletionCandidate.newBuilder()
                        .setAttempt(1).setRevision(1).setSummary("candidate")
                        .addEvidence(CheckEvidence.newBuilder().setCheckName("verified")
                                .setVerdict(CheckVerdict.CHECK_VERDICT_PASSED)
                                .setDetail("candidate ready for independent review")
                                .setRanAt(com.google.protobuf.util.Timestamps.now()))
                        .addArtifacts(ArtifactReference.newBuilder().setSha256("a".repeat(64))
                                .setMediaType("text/plain").setSizeBytes(1)).build());
                awaitReview(coordinator, task, DelegationReducer.ReviewStatus.FAILED);
                var detail = http.send(HttpRequest.newBuilder(URI.create(base + "/api/tasks/" + task))
                        .header("Cookie", steward).GET().build(), HttpResponse.BodyHandlers.ofString());
                var review = JSON.readTree(detail.body()).path("task").path("review");
                assertThat(detail.statusCode()).as(detail.body()).isEqualTo(200);
                assertThat(review.path("status").asText()).isEqualTo("failed");
                assertThat(detail.body()).doesNotContain("private backend diagnostic");
                var body = JSON.createObjectNode().put("attempt", 1).put("revision", 1)
                        .put("expectedInvocationId", review.path("invocationId").asText())
                        .put("retryId", UUID.randomUUID().toString());
                String path = "/api/tasks/" + task + "/review-retry";
                var before = coordinator.transcript();
                assertThat(post(path, body.toString(), null).statusCode()).isEqualTo(401);
                assertThat(post(path, body.toString(), author).statusCode()).isEqualTo(403);
                assertThat(post(path, body.deepCopy().put("unknown", true).toString(), steward).statusCode())
                        .isEqualTo(400);
                assertThat(post(path, body.deepCopy().put("attempt", 0).toString(), steward).statusCode())
                        .isEqualTo(400);
                assertThat(post(path, body.deepCopy().put("taskId", UUID.randomUUID().toString()).toString(),
                        steward).statusCode()).isEqualTo(400);
                assertThat(coordinator.transcript()).isEqualTo(before);
                var retried = post(path, body.toString(), steward);
                assertThat(retried.statusCode()).as(retried.body()).isEqualTo(200);
                awaitReview(coordinator, task, DelegationReducer.ReviewStatus.ACCEPTED);
                var replay = post(path, body.toString(), steward);
                assertThat(replay.statusCode()).isEqualTo(200);
                assertThat(JSON.readTree(replay.body())).isEqualTo(JSON.readTree(retried.body()));
                assertThat(calls).hasValue(2);
                assertThat(coordinator.transcript().getEntriesList().stream()
                        .filter(entry -> entry.getCoordinatorFrame().hasReviewStarted())).hasSize(2);
                assertThat(post(path, body.deepCopy().put("revision", 2).toString(), steward).statusCode())
                        .isEqualTo(400);
                assertThat(calls).hasValue(2);
            } finally {
                server.stop(0);
            }
        }
    }

    private String login(String token) throws Exception {
        var response = post("/api/task-session", JSON.createObjectNode().put("token", token).toString(), null);
        assertThat(response.statusCode()).isEqualTo(200);
        return response.headers().firstValue("set-cookie").orElseThrow().split(";", 2)[0];
    }

    private HttpResponse<String> post(String path, String body, String cookie) throws Exception {
        var request = HttpRequest.newBuilder(URI.create(base + path)).timeout(Duration.ofSeconds(5))
                .header("Content-Type", "application/json").POST(HttpRequest.BodyPublishers.ofString(body));
        if (cookie != null) request.header("Cookie", cookie);
        return http.send(request.build(), HttpResponse.BodyHandlers.ofString());
    }

    private static void awaitReview(InProcessDelegationCoordinator coordinator, String task,
            DelegationReducer.ReviewStatus status) throws Exception {
        long end = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        long cursor = 0;
        while (System.nanoTime() < end) {
            if (coordinator.state().tasks().get(task).review().status() == status) return;
            var event = coordinator.waitForEvent(task, cursor, Duration.ofMillis(100));
            if (event.isPresent()) cursor = event.get().cursor();
        }
        throw new AssertionError("review did not reach " + status);
    }
}
