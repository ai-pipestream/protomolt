package ai.protomolt.proto.samples;

import ai.protomolt.proto.samples.authoring.v1.FixtureStoredRecord;
import ai.protomolt.proto.samples.authoring.v1.WriteRecordRequest;
import ai.protomolt.proto.samples.starter.v1.WorkflowAcceptedCandidate;
import ai.protomolt.proto.samples.starter.v1.WorkflowAuthoringLaunchRequest;
import ai.protomolt.proto.receipt.WorkRecords;
import ai.protomolt.proto.workflow.authoring.v1.GetAcceptedWorkflowRequest;
import ai.protomolt.proto.workflow.authoring.v1.GetWorkflowAuthoringTemplateRequest;
import ai.protomolt.proto.workflow.authoring.v1.GetWorkflowAuthoringTemplateResponse;
import ai.protomolt.proto.workflow.authoring.v1.PrepareWorkflowLaunchInputRequest;
import ai.protomolt.proto.workflow.authoring.v1.PrepareWorkflowLaunchInputResponse;
import ai.protomolt.proto.workflow.authoring.v1.StartWorkflowAuthoringRequest;
import ai.protomolt.proto.workflow.authoring.v1.WorkflowAuthoringServiceGrpc;
import ai.protomolt.proto.workflow.authoring.v1.WorkflowPreparationRecord;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.MetadataUtils;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
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
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Container-package proof of a durable remote effect before its local checkpoint. */
@Tag("integration")
class AuthoringComposeCrashTest {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final HttpClient HTTP = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5)).build();
    private static final long CHECKPOINT_LOCK = 451455L;
    private static final Duration STARTUP_TIMEOUT = Duration.ofMinutes(5);
    private static final Duration WORK_TIMEOUT = Duration.ofMinutes(3);
    private static final String WORKER_ID = "scripted-author";

    @TempDir Path directory;

    @Test
    void killedComposeExecutorReplaysOnlyTheUncheckpointedWrite() throws Exception {
        String authoringImage = System.getenv("PROTOMOLT_AUTHORING_CRASH_IMAGE");
        String repositoryImage = System.getenv("PROTOMOLT_REPO_CRASH_IMAGE");
        assumeTrue(authoringImage != null || repositoryImage != null,
                "Set both digest-pinned image inputs to opt into the Compose crash gate");
        assertThat(authoringImage).as("PROTOMOLT_AUTHORING_IMAGE").isNotNull();
        assertThat(repositoryImage).as("PROTOMOLT_REPO_IMAGE").isNotNull();
        assertDigestImage(authoringImage);
        assertDigestImage(repositoryImage);

        Path composeFile = locateCompose();
        assertThat(composeFile).exists();
        int httpPort = freePort();
        int grpcPort = freePort();
        int jdbcPort = freePort();
        String project = "authoring-crash-" + UUID.randomUUID().toString().substring(0, 8);
        Path override = directory.resolve("crash-override.yml");
        Files.writeString(override, """
                services:
                  serve:
                    restart: "no"
                  fixture:
                    restart: "no"
                  jobs-postgres:
                    ports:
                      - "127.0.0.1:%d:5432"
                """.formatted(jdbcPort));
        Compose compose = new Compose(project, composeFile, override, authoringImage,
                repositoryImage, httpPort, grpcPort);
        Connection barrier = null;
        var channel = ManagedChannelBuilder.forAddress("127.0.0.1", grpcPort)
                .usePlaintext().maxInboundMessageSize(16 * 1024 * 1024).build();
        try {
            compose.run(Duration.ofMinutes(7), "up", "-d", "--pull", "never");
            awaitHealth(compose, httpPort);
            awaitAuthorReady(compose);
            String databasePassword = compose.secret("jobs-postgres", "/run/jobs-database/password");
            String jdbc = "jdbc:postgresql://127.0.0.1:" + jdbcPort + "/jobs";
            String operatorToken = compose.secret("serve", "/run/operator/token");
            String consoleToken = compose.secret("serve", "/run/console/token");
            String browserToken = compose.secret("serve", "/run/browser/token");
            String taskId = UUID.randomUUID().toString();
            String operationId = UUID.randomUUID().toString();
            String launchId = UUID.randomUUID().toString();

            String consoleCookie = login(httpPort, consoleToken);
            var template = GetWorkflowAuthoringTemplateResponse.newBuilder();
            JsonFormat.parser().merge(browserPost(httpPort, "/api/workflow-authoring/template",
                    GetWorkflowAuthoringTemplateRequest.getDefaultInstance(), consoleCookie), template);
            StartWorkflowAuthoringRequest start = StartWorkflowAuthoringRequest.newBuilder()
                    .setTaskId(taskId).setWorkerId(WORKER_ID)
                    .setTemplateSha256(template.getTemplateSha256())
                    .setObjective("Prove a packaged executor crash after the durable fixture effect")
                    .build();
            browserPost(httpPort, "/api/workflow-authoring/start", start, consoleCookie);

            var authoring = WorkflowAuthoringServiceGrpc.newBlockingStub(channel)
                    .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(bearer(operatorToken)));
            WorkflowAcceptedCandidate accepted = awaitAccepted(authoring, taskId, compose);
            String preparationKey = preparationKey(taskId, accepted.getAttempt(), accepted.getRevision());
            Path preparationFile = directory.resolve("accepted-preparation.pb");
            compose.copyFile("serve", "/data/serve/preparation-intents/records/"
                    + preparationKey + ".pb", preparationFile);
            WorkflowPreparationRecord preparation = WorkflowPreparationRecord.parseFrom(
                    Files.readAllBytes(preparationFile));
            assertThat(preparation.hasCompleted()).isTrue();
            String sourceSha = preparation.getCompleted().getAuthored()
                    .getExecutableSource().getSha256();
            Path acceptedSourceFile = directory.resolve("accepted-source.json");
            compose.copyFile("serve", "/data/serve/workflows/artifacts/" + sourceSha,
                    acceptedSourceFile);
            JsonNode acceptedSource = JSON.readTree(Files.readAllBytes(acceptedSourceFile));
            assertThat(acceptedSource.path("name").asText()).isEqualTo("normalize-record-v1");
            String browserCookie = login(httpPort, browserToken);
            WriteRecordRequest input = WriteRecordRequest.newBuilder()
                    .setOperationId(operationId).setContent(" \tcompose-input\r\n value\t ").build();
            var prepared = PrepareWorkflowLaunchInputResponse.newBuilder();
            JsonFormat.parser().merge(browserPost(httpPort, "/api/workflow-launch/prepare",
                    PrepareWorkflowLaunchInputRequest.newBuilder().setAcceptance(accepted)
                            .setInputJson(ByteString.copyFromUtf8(JsonFormat.printer().print(input)))
                            .build(), browserCookie), prepared);
            assertThat(prepared.getAcceptance()).isEqualTo(accepted);
            WorkflowAuthoringLaunchRequest launch = WorkflowAuthoringLaunchRequest.newBuilder()
                    .setLaunchId(launchId).setAcceptance(accepted).setInput(prepared.getInput()).build();

            installCheckpointBarrier(jdbc, databasePassword, launchId);
            barrier = database(jdbc, databasePassword);
            try (var statement = barrier.createStatement()) {
                statement.execute("SELECT pg_advisory_lock(" + CHECKPOINT_LOCK + ")");
            }
            var launched = ai.protomolt.proto.samples.starter.v1.WorkflowAuthoringLaunchResult.newBuilder();
            JsonFormat.parser().merge(browserPost(httpPort, "/api/workflow-launch/launch",
                    launch, browserCookie), launched);
            assertThat(launched.getJobId()).isEqualTo(launchId);
            awaitEffectAndBlockedCheckpoint(compose, jdbc, databasePassword, operationId);
            byte[] effectBefore = compose.copyFixtureRecord(operationId, directory.resolve("effect-before.pb"));
            FixtureStoredRecord storedBefore = FixtureStoredRecord.parseFrom(effectBefore);
            assertThat(storedBefore.getRequest().getOperationId()).isEqualTo(operationId);
            assertThat(storedBefore.getRequest().getContent()).isEqualTo("compose-input\n value");

            // Kill the actual WorkflowRunWorker host while its write-checkpoint transaction waits.
            compose.run(Duration.ofSeconds(30), "kill", "-s", "SIGKILL", "serve");
            assertThat(compose.containerRunning("serve")).isFalse();
            try (var statement = barrier.createStatement()) {
                statement.execute("SELECT pg_advisory_unlock(" + CHECKPOINT_LOCK + ")");
            }
            barrier.close();
            barrier = null;
            JsonNode firstCheckpoint = assertInterrupted(jdbc, databasePassword, launchId,
                    acceptedSource);

            compose.run(Duration.ofSeconds(30), "kill", "-s", "SIGKILL", "fixture");
            assertThat(compose.containerRunning("fixture")).isFalse();
            compose.run(Duration.ofMinutes(2), "up", "-d", "--no-deps", "--pull", "never", "fixture");
            byte[] effectAfterFixtureRestart = compose.copyFixtureRecord(operationId,
                    directory.resolve("effect-after-fixture-restart.pb"));
            assertThat(effectAfterFixtureRestart).containsExactly(effectBefore);

            // Accelerate the dead worker's lease only after verifying the interrupted row.
            try (var connection = database(jdbc, databasePassword);
                    var update = connection.prepareStatement("""
                            UPDATE workflow_run SET lease_until = clock_timestamp() - interval '1 second'
                             WHERE job_id = ? AND status = 'RUNNING' AND attempt = 1""")) {
                update.setObject(1, UUID.fromString(launchId));
                assertThat(update.executeUpdate()).isEqualTo(1);
            }
            compose.run(Duration.ofMinutes(2), "up", "-d", "--no-deps", "--pull", "never", "serve");
            awaitHealth(compose, httpPort);
            awaitCompleted(jdbc, databasePassword, launchId, compose);
            assertRecovered(jdbc, databasePassword, launchId, operationId, firstCheckpoint);
            assertThat(compose.copyFixtureRecord(operationId, directory.resolve("effect-after.pb")))
                    .containsExactly(effectBefore);
        } finally {
            if (barrier != null) barrier.close();
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            compose.runBestEffort(Duration.ofMinutes(2), "down", "-v", "--remove-orphans");
        }
    }

    private static void assertDigestImage(String image) {
        assertThat(image).as("image must be a registry digest or explicit local Docker content ID")
                .matches("(?:[^\\s@]+@)?sha256:[0-9a-f]{64}");
    }

    private static Path locateCompose() {
        Path directory = Path.of("").toAbsolutePath();
        for (int level = 0; level < 5 && directory != null; level++, directory = directory.getParent()) {
            Path candidate = directory.resolve("deploy/authoring/compose.yml");
            if (Files.isRegularFile(candidate)) return candidate;
        }
        throw new AssertionError("Cannot locate deploy/authoring/compose.yml from the test working directory");
    }

    private static Connection database(String jdbc, String password) throws Exception {
        return DriverManager.getConnection(jdbc, "starter", password);
    }

    private static String preparationKey(String taskId, int attempt, int revision) {
        byte[] task = taskId.getBytes(StandardCharsets.UTF_8);
        ByteBuffer tuple = ByteBuffer.allocate(Integer.BYTES + task.length + Integer.BYTES * 2);
        tuple.putInt(task.length).put(task).putInt(attempt).putInt(revision);
        return WorkRecords.sha256Hex(tuple.array());
    }

    private static void installCheckpointBarrier(String jdbc, String password, String launchId)
            throws Exception {
        try (var connection = database(jdbc, password); var statement = connection.createStatement()) {
            statement.execute("""
                    CREATE FUNCTION block_write_checkpoint() RETURNS trigger LANGUAGE plpgsql AS $$
                    BEGIN
                      IF NEW.event_type = 'STEP_CHECKPOINT'
                         AND NEW.kafka_key = '%s'
                         AND EXISTS (SELECT 1 FROM workflow_run r
                                      WHERE r.job_id::text = NEW.kafka_key
                                        AND jsonb_array_length(r.checkpoints) >= 2
                                        AND r.checkpoints->1->>'name' = 'write') THEN
                        PERFORM pg_advisory_xact_lock(%d);
                      END IF;
                      RETURN NEW;
                    END
                    $$""".formatted(launchId, CHECKPOINT_LOCK));
            statement.execute("CREATE TRIGGER block_write_checkpoint BEFORE INSERT "
                    + "ON workflow_run_events_outbox FOR EACH ROW "
                    + "EXECUTE FUNCTION block_write_checkpoint()");
        }
    }

    private static void awaitEffectAndBlockedCheckpoint(Compose compose, String jdbc,
            String password, String operationId) throws Exception {
        long deadline = System.nanoTime() + WORK_TIMEOUT.toNanos();
        while (System.nanoTime() < deadline) {
            assertThat(compose.containerRunning("serve")).as("Serve exited before the crash point").isTrue();
            boolean effect = compose.fixtureRecordPresent(operationId);
            try (var connection = database(jdbc, password);
                    var query = connection.prepareStatement("""
                            SELECT EXISTS (SELECT 1 FROM pg_stat_activity
                                           WHERE wait_event_type = 'Lock' AND wait_event = 'advisory'
                                             AND query LIKE '%workflow_run_events_outbox%')""");
                    var result = query.executeQuery()) {
                result.next();
                if (effect && result.getBoolean(1)) return;
            }
            Thread.sleep(200);
        }
        throw new AssertionError("Remote effect and blocked write checkpoint did not coincide");
    }

    private static JsonNode assertInterrupted(String jdbc, String password, String launchId,
            JsonNode acceptedSource)
            throws Exception {
        try (var connection = database(jdbc, password);
                var query = connection.prepareStatement("""
                        SELECT status, attempt, checkpoints, workflow_definition
                          FROM workflow_run WHERE job_id = ?""")) {
            query.setObject(1, UUID.fromString(launchId));
            try (var result = query.executeQuery()) {
                assertThat(result.next()).isTrue();
                assertThat(result.getString("status")).isEqualTo("RUNNING");
                assertThat(result.getInt("attempt")).isEqualTo(1);
                JsonNode source = JSON.readTree(result.getString("workflow_definition"));
                assertThat(source).isEqualTo(acceptedSource);
                JsonNode checkpoints = JSON.readTree(result.getString("checkpoints"));
                assertThat(checkpoints).hasSize(1);
                assertThat(checkpoints.get(0).path("name").asText()).isEqualTo("normalize");
                assertEventTypes(connection, launchId, List.of("ACCEPTED", "STEP_CHECKPOINT"));
                var interrupted = JSON.createObjectNode();
                interrupted.set("source", source);
                interrupted.set("checkpoint", checkpoints.get(0));
                return interrupted;
            }
        }
    }

    private static void awaitCompleted(String jdbc, String password, String launchId, Compose compose)
            throws Exception {
        long deadline = System.nanoTime() + WORK_TIMEOUT.toNanos();
        while (System.nanoTime() < deadline) {
            assertThat(compose.containerRunning("serve")).as("Restarted Serve exited").isTrue();
            try (var connection = database(jdbc, password);
                    var query = connection.prepareStatement(
                            "SELECT status FROM workflow_run WHERE job_id = ?")) {
                query.setObject(1, UUID.fromString(launchId));
                try (var result = query.executeQuery()) {
                    if (result.next() && "COMPLETED".equals(result.getString(1))) return;
                }
            }
            Thread.sleep(200);
        }
        throw new AssertionError("Recovered Compose executor did not complete the job");
    }

    private static void assertRecovered(String jdbc, String password, String launchId,
            String operationId,
            JsonNode interrupted) throws Exception {
        try (var connection = database(jdbc, password);
                var query = connection.prepareStatement("""
                        SELECT status, attempt, checkpoints, workflow_definition, result
                          FROM workflow_run WHERE job_id = ?""")) {
            query.setObject(1, UUID.fromString(launchId));
            try (var result = query.executeQuery()) {
                assertThat(result.next()).isTrue();
                assertThat(result.getString("status")).isEqualTo("COMPLETED");
                assertThat(result.getInt("attempt")).isEqualTo(2);
                assertThat(JSON.readTree(result.getString("workflow_definition")))
                        .isEqualTo(interrupted.path("source"));
                JsonNode checkpoints = JSON.readTree(result.getString("checkpoints"));
                assertThat(checkpoints).hasSize(2);
                assertThat(checkpoints.get(0)).isEqualTo(interrupted.path("checkpoint"));
                assertThat(checkpoints.get(1).path("name").asText()).isEqualTo("write");
                var expectedOutput = ai.protomolt.proto.samples.authoring.v1.WriteRecordResponse.newBuilder()
                        .setOperationId(operationId)
                        .setContentSha256(java.util.HexFormat.of().formatHex(
                                java.security.MessageDigest.getInstance("SHA-256").digest(
                                        "compose-input\n value".getBytes(StandardCharsets.UTF_8))))
                        .build();
                assertThat(JSON.readTree(result.getString("result")))
                        .isEqualTo(JSON.readTree(JsonFormat.printer().print(expectedOutput)));
                assertEventTypes(connection, launchId,
                        List.of("ACCEPTED", "STEP_CHECKPOINT", "STEP_CHECKPOINT", "COMPLETED"));
            }
        }
    }

    private static void assertEventTypes(Connection connection, String launchId, List<String> expected)
            throws Exception {
        try (var query = connection.prepareStatement("""
                SELECT event_type, payload FROM workflow_run_events_outbox
                 WHERE kafka_key = ? ORDER BY created_at, event_id""")) {
            query.setString(1, launchId);
            try (var result = query.executeQuery()) {
                List<String> types = new ArrayList<>();
                var events = new ArrayList<ai.protomolt.proto.jobs.v1.WorkflowRunEvent>();
                while (result.next()) {
                    types.add(result.getString(1));
                    events.add(ai.protomolt.proto.jobs.v1.WorkflowRunEvent.parseFrom(result.getBytes(2)));
                }
                assertThat(types).isEqualTo(expected);
                assertThat(events).allSatisfy(event -> assertThat(event.getJobId()).isEqualTo(launchId));
                assertThat(events.get(1).getStep()).isEqualTo("normalize");
                assertThat(events.get(1).getAttempt()).isEqualTo(1);
                if (expected.size() == 4) {
                    assertThat(events.get(2).getStep()).isEqualTo("write");
                    assertThat(events.get(2).getAttempt()).isEqualTo(2);
                    assertThat(events.get(3).getAttempt()).isEqualTo(2);
                }
            }
        }
    }

    private static WorkflowAcceptedCandidate awaitAccepted(
            WorkflowAuthoringServiceGrpc.WorkflowAuthoringServiceBlockingStub authoring,
            String taskId, Compose compose) throws Exception {
        long deadline = System.nanoTime() + WORK_TIMEOUT.toNanos();
        while (System.nanoTime() < deadline) {
            assertThat(compose.containerRunning("serve")).as("Serve exited before acceptance").isTrue();
            try {
                return authoring.withDeadlineAfter(10, TimeUnit.SECONDS)
                        .getAcceptedWorkflow(GetAcceptedWorkflowRequest.newBuilder()
                                .setTaskId(taskId).build());
            } catch (StatusRuntimeException pending) {
                if (pending.getStatus().getCode() != io.grpc.Status.Code.FAILED_PRECONDITION
                        && pending.getStatus().getCode() != io.grpc.Status.Code.NOT_FOUND) throw pending;
            }
            Thread.sleep(300);
        }
        throw new AssertionError("Idle author did not produce an accepted candidate");
    }

    private static void awaitHealth(Compose compose, int httpPort) throws Exception {
        long deadline = System.nanoTime() + STARTUP_TIMEOUT.toNanos();
        while (System.nanoTime() < deadline) {
            if (compose.containerRunning("serve")) {
                try {
                    var response = HTTP.send(HttpRequest.newBuilder(URI.create(
                                    "http://127.0.0.1:" + httpPort + "/health"))
                            .timeout(Duration.ofSeconds(3)).GET().build(),
                            HttpResponse.BodyHandlers.discarding());
                    if (response.statusCode() == 200) return;
                } catch (Exception ignored) { }
            }
            Thread.sleep(300);
        }
        throw new AssertionError("Compose Serve did not become healthy");
    }

    private static void awaitAuthorReady(Compose compose) throws Exception {
        long deadline = System.nanoTime() + STARTUP_TIMEOUT.toNanos();
        while (System.nanoTime() < deadline) {
            assertThat(compose.containerRunning("author")).as("Idle author exited").isTrue();
            if (compose.run(Duration.ofSeconds(15), "logs", "--no-color", "author")
                    .contains("AuthoringWorker ready")) return;
            Thread.sleep(300);
        }
        throw new AssertionError("Compose idle author did not register");
    }

    private static String login(int httpPort, String token) throws Exception {
        var response = HTTP.send(HttpRequest.newBuilder(URI.create(
                        "http://127.0.0.1:" + httpPort + "/api/task-session"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(JSON.writeValueAsString(Map.of("token", token))))
                .build(), HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(200);
        return response.headers().firstValue("Set-Cookie").orElseThrow().split(";", 2)[0];
    }

    private static String browserPost(int httpPort, String path, Message request, String cookie)
            throws Exception {
        String origin = "http://127.0.0.1:" + httpPort;
        var response = HTTP.send(HttpRequest.newBuilder(URI.create(origin + path))
                .timeout(Duration.ofSeconds(30)).header("Origin", origin).header("Cookie", cookie)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(JsonFormat.printer().print(request)))
                .build(), HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).as("browser route " + path + ": " + response.body())
                .isEqualTo(200);
        return response.body();
    }

    private static Metadata bearer(String token) {
        Metadata headers = new Metadata();
        headers.put(Metadata.Key.of("api_token", Metadata.ASCII_STRING_MARSHALLER), token);
        return headers;
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
            Path outputFile = Files.createTempFile("authoring-compose-", ".log");
            try {
                Process process = builder(arguments).redirectErrorStream(true)
                        .redirectOutput(outputFile.toFile()).start();
                boolean ended = process.waitFor(timeout.toMillis(), TimeUnit.MILLISECONDS);
                if (!ended) {
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
            boolean ended = process.waitFor(15, TimeUnit.SECONDS);
            if (!ended) process.destroyForcibly();
            assertThat(ended && process.exitValue() == 0)
                    .as("Unable to read required test-project secret").isTrue();
            return new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8).trim();
        }

        boolean containerRunning(String service) throws Exception {
            return run(Duration.ofSeconds(15), "ps", "--status", "running", "--services")
                    .lines().anyMatch(service::equals);
        }

        boolean fixtureRecordPresent(String operationId) throws Exception {
            Process process = builder("exec", "-T", "fixture", "test", "-s",
                    "/records/" + operationId + ".pb").start();
            if (!process.waitFor(15, TimeUnit.SECONDS)) {
                process.destroyForcibly();
                throw new AssertionError("Timed out checking the durable fixture record");
            }
            if (process.exitValue() == 0) return true;
            assertThat(process.exitValue()).as("Fixture record probe failed").isEqualTo(1);
            return false;
        }

        byte[] copyFixtureRecord(String operationId, Path destination) throws Exception {
            copyFile("fixture", "/records/" + operationId + ".pb", destination);
            return Files.readAllBytes(destination);
        }

        void copyFile(String service, String source, Path destination) throws Exception {
            run(Duration.ofSeconds(30), "cp", service + ":" + source, destination.toString());
        }
    }
}
