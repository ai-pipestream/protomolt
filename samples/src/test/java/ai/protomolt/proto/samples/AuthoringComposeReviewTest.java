package ai.protomolt.proto.samples;

import ai.protomolt.proto.delegation.v1.AcceptTaskRequest;
import ai.protomolt.proto.delegation.v1.CheckVerdict;
import ai.protomolt.proto.delegation.v1.CompletionCandidate;
import ai.protomolt.proto.delegation.v1.SubmitCandidateRequest;
import ai.protomolt.proto.samples.authoring.v1.WriteRecordRequest;
import ai.protomolt.proto.workflow.authoring.v1.GetWorkflowAuthorContextRequest;
import ai.protomolt.proto.workflow.authoring.v1.GetWorkflowAuthorContextResponse;
import ai.protomolt.proto.workflow.authoring.v1.PrepareWorkflowCandidateRequest;
import ai.protomolt.proto.workflow.authoring.v1.WorkflowAuthorTaskServiceGrpc;
import ai.protomolt.proto.workflow.authoring.v1.WorkflowPreparationServiceGrpc;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.util.JsonFormat;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.MetadataUtils;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Packaged-browser proof of persisted review failure, bound retry, and stale refusal. */
@Tag("integration")
class AuthoringComposeReviewTest {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final HttpClient HTTP = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5)).build();
    private static final Duration WAIT = Duration.ofMinutes(3);
    private static final String WORKER_ID = "scripted-author";

    @TempDir Path directory;

    @Test
    void packagedBrowserShowsFailedReviewAndRetriesOnlyCurrentInvocation() throws Exception {
        String authoringImage = System.getenv("PROTOMOLT_AUTHORING_REVIEW_IMAGE");
        String repositoryImage = System.getenv("PROTOMOLT_REPO_REVIEW_IMAGE");
        assertTrue(authoringImage != null || repositoryImage != null,
                "Set both digest image inputs for the packaged review gate");
        assertThat(authoringImage).isNotNull();
        assertThat(repositoryImage).isNotNull();
        assertDigestImage(authoringImage);
        assertDigestImage(repositoryImage);
        Path base = locate("deploy/authoring/compose.yml");
        int httpPort = freePort();
        int grpcPort = freePort();
        Path override = directory.resolve("review-override.yml");
        Files.writeString(override, """
                services:
                  author:
                    restart: "no"
                  fixture:
                    restart: "no"
                """);
        Compose compose = new Compose("authoring-review-" + UUID.randomUUID().toString().substring(0, 8),
                base, override, authoringImage, repositoryImage, httpPort, grpcPort);
        Path evidence = Path.of("build/authoring-review-evidence").toAbsolutePath();
        Files.createDirectories(evidence);
        Files.deleteIfExists(evidence.resolve("review-failure.txt"));
        Files.deleteIfExists(evidence.resolve("compose-logs.txt"));
        String browserToken = null;
        var channel = ManagedChannelBuilder.forAddress("127.0.0.1", grpcPort)
                .usePlaintext().maxInboundMessageSize(16 * 1024 * 1024).build();
        try {
            compose.run(Duration.ofMinutes(7), "up", "-d", "--pull", "never");
            awaitReady(compose, httpPort);
            awaitAuthorReady(compose);
            String authorToken = compose.secret("author", "/run/author/token");
            String operatorToken = compose.secret("serve", "/run/operator/token");
            browserToken = compose.secret("serve", "/run/browser/token");
            compose.run(Duration.ofSeconds(30), "pause", "author");
            browser(base, httpPort, browserToken, "start", null, evidence);
            String cookie = login(httpPort, browserToken);
            String taskId = onlyTask(httpPort, cookie);

            var tasks = WorkflowAuthorTaskServiceGrpc.newBlockingStub(channel)
                    .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(bearer(authorToken)));
            var preparation = WorkflowPreparationServiceGrpc.newBlockingStub(channel)
                    .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(bearer(authorToken)));
            GetWorkflowAuthorContextResponse context = tasks.withDeadlineAfter(10, TimeUnit.SECONDS)
                    .getWorkflowAuthorContext(GetWorkflowAuthorContextRequest.newBuilder()
                            .setTaskId(taskId).setAttempt(1).build());
            assertThat(context.getOfferEntry().getWorkerId()).isEqualTo(WORKER_ID);
            var accepted = tasks.withDeadlineAfter(10, TimeUnit.SECONDS)
                    .acceptWorkflowTask(AcceptTaskRequest.newBuilder().setTaskId(taskId)
                            .setWorkerId(WORKER_ID).setAttempt(1).build());
            assertThat(accepted.getOk()).isTrue();
            JsonNode eventsBeforeInvalid = task(httpPort, cookie, taskId).path("events");
            assertThat(eventsBeforeInvalid.isArray()).isTrue();
            assertThat(eventsBeforeInvalid.size()).isPositive();

            // Invalid content is refused before a candidate or review invocation is committed.
            SubmitCandidateRequest malformed = SubmitCandidateRequest.newBuilder()
                    .setWorkerId(WORKER_ID).setTaskId(taskId)
                    .setCandidate(CompletionCandidate.newBuilder().setAttempt(1).setRevision(1).build())
                    .build();
            assertThatThrownBy(() -> tasks.withDeadlineAfter(10, TimeUnit.SECONDS)
                    .submitWorkflowCandidate(malformed))
                    .isInstanceOfSatisfying(StatusRuntimeException.class,
                            failure -> assertThat(failure.getStatus().getCode())
                                    .isEqualTo(Status.Code.INVALID_ARGUMENT));
            assertThat(task(httpPort, cookie, taskId).path("task").path("review").path("status").asText())
                    .isEqualTo("none");
            assertThat(task(httpPort, cookie, taskId).path("events"))
                    .isEqualTo(eventsBeforeInvalid);

            String source = source(context);
            String preparationId = UUID.randomUUID().toString();
            var prepared = preparation.withDeadlineAfter(60, TimeUnit.SECONDS)
                    .prepareWorkflowCandidate(PrepareWorkflowCandidateRequest.newBuilder()
                            .setTaskId(taskId).setAttempt(1).setRevision(1)
                            .setPreparationId(preparationId)
                            .setExecutableSourceJson(ByteString.copyFromUtf8(source)).build());
            assertThat(prepared.getBinding().getPreparationId()).isEqualTo(preparationId);
            assertThat(prepared.getBinding().getOfferEntrySha256())
                    .isEqualTo(context.getOfferEntrySha256());
            var authored = prepared.getAuthored();
            var deliverable = authored.getDeliverable();
            assertThat(deliverable.getChecksCount()).isPositive();
            assertThat(deliverable.getChecksList()).allSatisfy(check ->
                    assertThat(check.getVerdict()).isEqualTo(CheckVerdict.CHECK_VERDICT_PASSED));
            CompletionCandidate candidate = CompletionCandidate.newBuilder()
                    .setAttempt(1).setRevision(1)
                    .setSummary("Prepared and verified against the pinned fixture")
                    .addAllEvidence(deliverable.getChecksList())
                    .addArtifacts(deliverable.getWorkflowArtifact())
                    .addArtifacts(authored.getExecutableSource())
                    .addArtifacts(deliverable.getReceipt())
                    .setResult(Any.pack(authored)).build();
            CompletionCandidate invalidDeliverable = candidate.toBuilder()
                    .setResult(Any.pack(authored.toBuilder()
                            .setDeliverable(deliverable.toBuilder().clearWorkflowArtifact()).build()))
                    .build();
            JsonNode eventsBeforeInvalidDeliverable = task(httpPort, cookie, taskId).path("events");
            assertThat(eventsBeforeInvalidDeliverable.isArray()).isTrue();
            assertThat(eventsBeforeInvalidDeliverable.size()).isPositive();
            assertThatThrownBy(() -> tasks.withDeadlineAfter(10, TimeUnit.SECONDS)
                    .submitWorkflowCandidate(SubmitCandidateRequest.newBuilder()
                            .setWorkerId(WORKER_ID).setTaskId(taskId)
                            .setCandidate(invalidDeliverable).build()))
                    .isInstanceOfSatisfying(StatusRuntimeException.class,
                            failure -> assertThat(failure.getStatus().getCode())
                                    .isEqualTo(Status.Code.INVALID_ARGUMENT));
            assertThat(task(httpPort, cookie, taskId).path("events"))
                    .isEqualTo(eventsBeforeInvalidDeliverable);
            assertThat(task(httpPort, cookie, taskId).path("task").path("review")
                    .path("status").asText()).isEqualTo("none");

            // Preparation has completed; take down only the reviewer fixture before submission.
            compose.run(Duration.ofSeconds(30), "stop", "fixture");
            assertThat(compose.containerRunning("fixture")).isFalse();
            var submitted = tasks.withDeadlineAfter(10, TimeUnit.SECONDS)
                    .submitWorkflowCandidate(SubmitCandidateRequest.newBuilder()
                            .setWorkerId(WORKER_ID).setTaskId(taskId).setCandidate(candidate).build());
            assertThat(submitted.getOk()).isTrue();
            assertThat(submitted.getAttempt()).isEqualTo(1);
            assertThat(submitted.getRevision()).isEqualTo(1);
            JsonNode failed = awaitReview(httpPort, cookie, taskId, "failed");
            String failedInvocation = failed.path("invocationId").asText();
            assertThat(failedInvocation).matches("[0-9a-f-]{36}");
            assertThat(failed.path("failureCode").asText())
                    .isIn("infrastructure", "deadline");
            browser(base, httpPort, browserToken, "failed", taskId, evidence);

            compose.run(Duration.ofMinutes(2), "up", "-d", "--no-deps", "--pull", "never", "fixture");
            awaitFixtureReflect(httpPort, operatorToken);
            browser(base, httpPort, browserToken, "retry", taskId, evidence);
            JsonNode acceptedReview = awaitReview(httpPort, cookie, taskId, "accepted");
            assertThat(acceptedReview.path("invocationId").asText()).isNotEqualTo(failedInvocation);
            browser(base, httpPort, browserToken, "accepted", taskId, evidence);
            JsonNode acceptedTask = task(httpPort, cookie, taskId);

            String stale = JSON.writeValueAsString(Map.of("attempt", 1, "revision", 1,
                    "expectedInvocationId", failedInvocation, "retryId", UUID.randomUUID().toString()));
            HttpResponse<String> refused = taskPost(httpPort, cookie,
                    "/api/tasks/" + taskId + "/review-retry", stale);
            // The console maps the coordinator's stale-request IllegalArgumentException to 400.
            assertThat(refused.statusCode()).isEqualTo(400);
            assertThat(refused.body()).contains("review retry does not match the latest failed candidate");
            JsonNode afterStale = task(httpPort, cookie, taskId);
            assertThat(afterStale.path("task").path("phase").asText()).isEqualTo("accepted");
            assertThat(afterStale.path("task").path("review"))
                    .isEqualTo(acceptedTask.path("task").path("review"));
            assertThat(afterStale.path("events")).isEqualTo(acceptedTask.path("events"));
        } catch (Exception | AssertionError failed) {
            Files.writeString(evidence.resolve("review-failure.txt"), failed.toString());
            compose.captureLogs(evidence.resolve("compose-logs.txt"));
            throw failed;
        } finally {
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            compose.runBestEffort(Duration.ofSeconds(30), "unpause", "author");
            compose.runBestEffort(Duration.ofMinutes(2), "down", "-v", "--remove-orphans");
        }
    }

    private static String source(GetWorkflowAuthorContextResponse context) throws Exception {
        var root = JSON.createObjectNode();
        root.put("name", "normalize-record-v1");
        root.put("validateContract", true);
        root.putObject("schema").put("descriptorSetBase64",
                java.util.Base64.getEncoder().encodeToString(context.getDescriptorSet().toByteArray()));
        root.put("inputType", WriteRecordRequest.getDescriptor().getFullName());
        var steps = root.putArray("steps");
        var normalize = steps.addObject();
        normalize.put("name", "normalize");
        normalize.put("target", "fixture:9778");
        normalize.put("method", "ai.protomolt.proto.samples.authoring.v1.AuthoringFixtureService/NormalizeText");
        normalize.put("validate", true);
        normalize.putArray("rules").add("text = input.content");
        var write = steps.addObject();
        write.put("name", "write");
        write.put("target", "fixture:9778");
        write.put("method", "ai.protomolt.proto.samples.authoring.v1.AuthoringFixtureService/WriteRecord");
        write.put("validate", true);
        write.putArray("rules").add("operation_id = input.operation_id")
                .add("content = normalize.text");
        return JSON.writeValueAsString(root);
    }

    private static JsonNode awaitReview(int port, String cookie, String taskId, String status)
            throws Exception {
        long deadline = System.nanoTime() + WAIT.toNanos();
        while (System.nanoTime() < deadline) {
            JsonNode review = task(port, cookie, taskId).path("task").path("review");
            if (status.equals(review.path("status").asText())) return review;
            Thread.sleep(250);
        }
        throw new AssertionError("Review did not reach " + status);
    }

    private static JsonNode task(int port, String cookie, String taskId) throws Exception {
        var response = HTTP.send(HttpRequest.newBuilder(URI.create(
                        "http://127.0.0.1:" + port + "/api/tasks/" + taskId))
                .header("Cookie", cookie).timeout(Duration.ofSeconds(10)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(200);
        return JSON.readTree(response.body());
    }

    private static String onlyTask(int port, String cookie) throws Exception {
        var response = HTTP.send(HttpRequest.newBuilder(URI.create(
                        "http://127.0.0.1:" + port + "/api/tasks"))
                .header("Cookie", cookie).timeout(Duration.ofSeconds(10)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(200);
        JsonNode tasks = JSON.readTree(response.body()).path("tasks");
        assertThat(tasks).hasSize(1);
        return tasks.get(0).path("taskId").asText();
    }

    private static HttpResponse<String> taskPost(int port, String cookie, String path, String body)
            throws Exception {
        String origin = "http://127.0.0.1:" + port;
        return HTTP.send(HttpRequest.newBuilder(URI.create(origin + path))
                .timeout(Duration.ofSeconds(20)).header("Origin", origin).header("Cookie", cookie)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body)).build(),
                HttpResponse.BodyHandlers.ofString());
    }

    private static String login(int port, String token) throws Exception {
        var response = HTTP.send(HttpRequest.newBuilder(URI.create(
                        "http://127.0.0.1:" + port + "/api/task-session"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(JSON.writeValueAsString(Map.of("token", token))))
                .build(), HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(200);
        return response.headers().firstValue("Set-Cookie").orElseThrow().split(";", 2)[0];
    }

    private static void browser(Path base, int port, String token, String phase,
            String taskId, Path evidence) throws Exception {
        Path script = base.getParent().resolve("browser-review-smoke.mjs");
        ProcessBuilder builder = new ProcessBuilder("node", script.toString());
        builder.environment().put("REVIEW_PHASE", phase);
        builder.environment().put("REVIEW_BROWSER_TOKEN", token);
        builder.environment().put("REVIEW_TASK_ID", taskId == null ? "" : taskId);
        builder.environment().put("HTTP_BASE", "http://127.0.0.1:" + port);
        builder.environment().put("SCREENSHOT_PATH", evidence.resolve("browser-" + phase + ".png").toString());
        Path log = evidence.resolve("browser-" + phase + ".log");
        Process process = builder.redirectErrorStream(true).redirectOutput(log.toFile()).start();
        if (!process.waitFor(3, TimeUnit.MINUTES)) {
            List<ProcessHandle> children = process.descendants().toList();
            for (int index = children.size() - 1; index >= 0; index--) {
                children.get(index).destroyForcibly();
            }
            process.destroyForcibly();
            process.waitFor(5, TimeUnit.SECONDS);
            throw new AssertionError("Browser " + phase + " timed out");
        }
        assertThat(process.exitValue()).as("Browser " + phase + " failed: " + Files.readString(log))
                .isZero();
    }

    private static void awaitReady(Compose compose, int port) throws Exception {
        long deadline = System.nanoTime() + Duration.ofMinutes(5).toNanos();
        while (System.nanoTime() < deadline) {
            try {
                var response = HTTP.send(HttpRequest.newBuilder(URI.create(
                                "http://127.0.0.1:" + port + "/health"))
                        .timeout(Duration.ofSeconds(3)).GET().build(),
                        HttpResponse.BodyHandlers.discarding());
                if (response.statusCode() == 200) return;
            } catch (Exception ignored) { }
            Thread.sleep(300);
        }
        throw new AssertionError("Compose Serve did not become healthy");
    }

    private static void awaitAuthorReady(Compose compose) throws Exception {
        long deadline = System.nanoTime() + Duration.ofMinutes(5).toNanos();
        while (System.nanoTime() < deadline) {
            if (compose.run(Duration.ofSeconds(15), "logs", "--no-color", "author")
                    .contains("AuthoringWorker ready")) return;
            Thread.sleep(300);
        }
        throw new AssertionError("Compose author did not register");
    }

    private static void awaitFixtureReflect(int port, String operatorToken) throws Exception {
        long deadline = System.nanoTime() + Duration.ofMinutes(2).toNanos();
        while (System.nanoTime() < deadline) {
            try {
                var response = HTTP.send(HttpRequest.newBuilder(URI.create(
                                "http://127.0.0.1:" + port + "/grpc-json/ProtoMoltService/Reflect"))
                        .timeout(Duration.ofSeconds(5)).header("api_token", operatorToken)
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(
                                "{\"target\":\"fixture:9778\",\"deadlineMs\":1000}"))
                        .build(), HttpResponse.BodyHandlers.ofString());
                if (response.statusCode() == 200) {
                    JsonNode reflected = JSON.readTree(response.body());
                    if (reflected.path("ok").asBoolean()
                            && reflected.path("services").isArray()
                            && reflected.path("services").toString().contains(
                                    "ai.protomolt.proto.samples.authoring.v1.AuthoringFixtureService")) return;
                } else if (response.statusCode() == 401 || response.statusCode() == 403) {
                    throw new AssertionError("Operator reflection readiness was denied");
                }
            } catch (java.io.IOException transientFailure) {
                // The real mounted reflection endpoint is the readiness authority.
            }
            Thread.sleep(300);
        }
        throw new AssertionError("Restarted fixture was not reachable through Serve reflection");
    }

    private static Metadata bearer(String token) {
        Metadata headers = new Metadata();
        headers.put(Metadata.Key.of("api_token", Metadata.ASCII_STRING_MARSHALLER), token);
        return headers;
    }

    private static void assertDigestImage(String image) {
        assertThat(image).as("image must be a registry digest or explicit local content ID")
                .matches("(?:[^\\s@]+@)?sha256:[0-9a-f]{64}");
    }

    private static Path locate(String relative) {
        Path directory = Path.of("").toAbsolutePath();
        for (int level = 0; level < 5 && directory != null; level++, directory = directory.getParent()) {
            Path candidate = directory.resolve(relative);
            if (Files.isRegularFile(candidate)) return candidate;
        }
        throw new AssertionError("Cannot locate " + relative);
    }

    private static int freePort() throws Exception {
        try (ServerSocket socket = new ServerSocket()) {
            socket.bind(new InetSocketAddress("127.0.0.1", 0));
            return socket.getLocalPort();
        }
    }

    private record Compose(String project, Path base, Path override, String authoringImage,
            String repositoryImage, int httpPort, int grpcPort) {
        private ProcessBuilder builder(String... arguments) {
            List<String> command = new ArrayList<>(List.of("docker", "compose", "-p", project,
                    "-f", base.toString(), "-f", override.toString()));
            command.addAll(List.of(arguments));
            ProcessBuilder builder = new ProcessBuilder(command);
            builder.directory(base.getParent().toFile());
            builder.environment().put("PROTOMOLT_AUTHORING_IMAGE", authoringImage);
            builder.environment().put("PROTOMOLT_REPO_IMAGE", repositoryImage);
            builder.environment().put("PROTOMOLT_HTTP_PORT", Integer.toString(httpPort));
            builder.environment().put("PROTOMOLT_GRPC_PORT", Integer.toString(grpcPort));
            return builder;
        }

        String run(Duration timeout, String... arguments) throws Exception {
            Path outputFile = Files.createTempFile("authoring-review-compose-", ".log");
            try {
                Process process = builder(arguments).redirectErrorStream(true)
                        .redirectOutput(outputFile.toFile()).start();
                if (!process.waitFor(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
                    process.destroyForcibly();
                    throw new AssertionError("Docker Compose timed out: " + String.join(" ", arguments));
                }
                String output = Files.readString(outputFile);
                assertThat(process.exitValue()).as("Docker Compose " + String.join(" ", arguments)
                        + " failed:\n" + output).isZero();
                return output;
            } finally {
                Files.deleteIfExists(outputFile);
            }
        }

        void runBestEffort(Duration timeout, String... arguments) {
            try { run(timeout, arguments); } catch (Exception | AssertionError ignored) { }
        }

        String secret(String service, String path) throws Exception {
            Process process = builder("exec", "-T", service, "cat", path).start();
            if (!process.waitFor(15, TimeUnit.SECONDS)) {
                process.destroyForcibly();
                throw new AssertionError("Timed out reading test-project credential");
            }
            assertThat(process.exitValue()).as("Could not read test-project credential").isZero();
            return new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8).trim();
        }

        boolean containerRunning(String service) throws Exception {
            return run(Duration.ofSeconds(15), "ps", "--status", "running", "--services")
                    .lines().anyMatch(service::equals);
        }

        void captureLogs(Path destination) {
            try { Files.writeString(destination, run(Duration.ofSeconds(30), "logs", "--no-color")); }
            catch (Exception | AssertionError ignored) { }
        }
    }
}
