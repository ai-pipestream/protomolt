package ai.protomolt.proto.samples;

import ai.protomolt.proto.grpc.workflow.FileSystemArtifactRepository;
import ai.protomolt.proto.grpc.workflow.v1.ArtifactReference;
import ai.protomolt.proto.receipt.KeyState;
import ai.protomolt.proto.receipt.RecordKeys;
import ai.protomolt.proto.receipt.SignatureAlgorithm;
import ai.protomolt.proto.receipt.TrustedIssuer;
import ai.protomolt.proto.receipt.TrustedKey;
import ai.protomolt.proto.receipt.TrustSnapshot;
import ai.protomolt.proto.receipt.WorkRecords;
import ai.protomolt.proto.samples.authoring.v1.FixtureStoredRecord;
import ai.protomolt.proto.samples.authoring.v1.WriteRecordRequest;
import ai.protomolt.proto.samples.authoring.v1.WriteRecordResponse;
import ai.protomolt.proto.samples.starter.v1.WorkflowAcceptanceFixture;
import ai.protomolt.proto.samples.starter.v1.WorkflowAuthoringLaunchRequest;
import ai.protomolt.proto.samples.starter.v1.WorkflowAuthoringPolicy;
import ai.protomolt.proto.samples.starter.v1.WorkflowPermittedCall;
import ai.protomolt.proto.samples.starter.v1.WorkflowAcceptedCandidate;
import ai.protomolt.proto.workflow.authoring.FileSystemWorkflowPreparationRepository;
import ai.protomolt.proto.workflow.authoring.v1.WorkflowAuthoringServiceGrpc;
import ai.protomolt.proto.workflow.authoring.v1.GetAcceptedWorkflowRequest;
import ai.protomolt.proto.samples.starter.v1.WorkflowAuthoringDeliverable;
import ai.protomolt.proto.repo.v1.DocumentServiceGrpc;
import ai.protomolt.proto.repo.v1.FileStorageReference;
import ai.protomolt.proto.repo.v1.GetBlobRequest;
import ai.protomolt.proto.repo.v1.GetBlobResponse;
import ai.protomolt.proto.repo.v1.PutBlobRequest;
import ai.protomolt.proto.repo.v1.PutBlobResponse;
import ai.protomolt.proto.repo.v1.ConditionalBlobKey;
import ai.protomolt.proto.repo.v1.ConditionalBlobVersion;
import ai.protomolt.proto.repo.v1.GetBlobForUpdateRequest;
import ai.protomolt.proto.repo.v1.GetBlobForUpdateResponse;
import ai.protomolt.proto.repo.v1.CompareAndPutBlobRequest;
import ai.protomolt.proto.repo.v1.CompareAndPutBlobResponse;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.util.JsonFormat;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;

/** Kills the installed JVM that actually hosts WorkflowRunWorker between an external effect and checkpoint commit. */
@Tag("integration")
@Testcontainers(disabledWithoutDocker = true)
class AuthoringWorkflowWorkerCrashProcessTest {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final HttpClient HTTP = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5)).build();
    private static final String OPERATOR_TOKEN = "remote-process-operator-token";
    private static final String AUTHOR_TOKEN = "remote-process-author-token";
    private static final String BROWSER_TOKEN = "remote-process-browser-launch-token";
    private static final String CONSOLE_TOKEN = "remote-process-default-console-token";
    private static final String TASK_ID = "00000000-0000-4000-8000-000000000451";
    private static final String POLICY_OPERATION = "00000000-0000-4000-8000-000000000452";
    private static final String JOB_OPERATION = "00000000-0000-4000-8000-000000000453";
    private static final String LAUNCH_ID = "00000000-0000-4000-8000-000000000454";
    private static final long CHECKPOINT_LOCK = 451454L;
    private static final String ISSUER = "remote-authoring-coordinator";
    private static final String KEY_ID = "remote-authoring-signing-key";
    private static final String PRINCIPAL = "scripted-author";
    private static final AtomicInteger MCP_IDS = new AtomicInteger(10);

    @TempDir Path directory;

    @Test
    void killedWorkflowExecutorReplaysUncheckpointedWriteAgainstDurableFixture() throws Exception {
        var postgres = new PostgreSQLContainer<>(DockerImageName.parse("postgres:16-alpine"))
                .withDatabaseName("protomolt").withUsername("protomolt").withPassword("test-password");
        Process coordinator = null;
        Process fixture = null;
        Process author = null;
        Server repository = null;
        ManagedChannel channel = null;
        java.sql.Connection barrier = null;
        try {
            postgres.start();
            repository = ServerBuilder.forPort(0).addService(new FakeDocumentService()).build().start();
            Path fixtureRecords = directory.resolve("fixture-records");
            Path fixtureLog = directory.resolve("fixture.log");
            int fixturePort = freePort();
            fixture = startFixture(fixturePort, fixtureRecords, fixtureLog);
            assertThat(awaitFixturePort(fixture, fixtureLog)).isEqualTo(fixturePort);
            String fixtureTarget = "127.0.0.1:" + fixturePort;

            Path workspace = directory.resolve("workflow-workspace");
            Path registry = directory.resolve("registry.git");
            Path authorization = directory.resolve("launch-authorizations");
            Path preparations = directory.resolve("preparations");
            Path trustFile = directory.resolve("trust.binpb");
            Path signingKey = directory.resolve("signing.seed");
            Path accessPolicy = directory.resolve("access-policy.json");
            writeSigningAndTrust(signingKey, trustFile);
            Files.writeString(accessPolicy, accessPolicyJson());
            FileSystemArtifactRepository artifacts =
                    new FileSystemArtifactRepository(workspace.resolve("artifacts"));
            ArtifactReference policy = writePolicy(artifacts, fixtureTarget);
            int grpcPort = freePort();
            int httpPort = freePort();
            Path coordinatorLog = directory.resolve("coordinator-before.log");
            coordinator = startCoordinator(grpcPort, httpPort, postgres, registry, workspace,
                    authorization, preparations, policy.getSha256(), trustFile, signingKey,
                    accessPolicy, "127.0.0.1:" + repository.getPort(), coordinatorLog);
            awaitHealth(coordinator, httpPort, coordinatorLog);

            var mcp = initializeMcp(httpPort, OPERATOR_TOKEN);
            Path authorLog = directory.resolve("author.log");
            author = startWorker(grpcPort, fixturePort, authorLog);
            awaitOutput(author, authorLog, "AuthoringWorker ready", Duration.ofSeconds(30));
            offerTask(mcp, policy);
            awaitOutput(author, authorLog, "AuthoringWorker accepted task=" + TASK_ID,
                    Duration.ofSeconds(120));
            assertThat(author.waitFor(10, TimeUnit.SECONDS)).isTrue();
            assertThat(author.exitValue()).as(Files.readString(authorLog)).isZero();

            channel = ManagedChannelBuilder.forAddress("127.0.0.1", grpcPort).usePlaintext()
                    .maxInboundMessageSize(16 * 1024 * 1024).build();
            var authoring = WorkflowAuthoringServiceGrpc.newBlockingStub(channel)
                    .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(bearer(OPERATOR_TOKEN)));
            WorkflowAcceptedCandidate accepted = authoring.withDeadlineAfter(10, TimeUnit.SECONDS)
                    .getAcceptedWorkflow(GetAcceptedWorkflowRequest.newBuilder()
                            .setTaskId(TASK_ID).build());
            var prepared = new FileSystemWorkflowPreparationRepository(preparations)
                    .find(TASK_ID, 1, 1).orElseThrow();
            assertThat(prepared.hasCompleted()).isTrue();
            ArtifactReference sourceRef = prepared.getCompleted().getAuthored().getExecutableSource();
            JsonNode acceptedSource = JSON.readTree(artifacts.find(sourceRef.getSha256())
                    .orElseThrow().content());
            assertThat(acceptedSource.path("name").asText()).isEqualTo("normalize-record-v1");

            WriteRecordRequest jobInput = WriteRecordRequest.newBuilder().setOperationId(JOB_OPERATION)
                    .setContent(" \tprocess-input\r\n value\t ").build();
            var inputService = ai.protomolt.proto.workflow.authoring.v1.WorkflowLaunchInputServiceGrpc
                    .newBlockingStub(channel)
                    .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(bearer(OPERATOR_TOKEN)))
                    .withDeadlineAfter(10, TimeUnit.SECONDS);
            ArtifactReference input = inputService.prepareWorkflowLaunchInput(
                    ai.protomolt.proto.workflow.authoring.v1.PrepareWorkflowLaunchInputRequest
                            .newBuilder().setAcceptance(accepted)
                            .setInputJson(ByteString.copyFromUtf8(JsonFormat.printer().print(jobInput)))
                            .build()).getInput();
            WorkflowAuthoringLaunchRequest launchRequest = WorkflowAuthoringLaunchRequest.newBuilder()
                    .setLaunchId(LAUNCH_ID).setAcceptance(accepted).setInput(input).build();

            installCheckpointBarrier(postgres);
            barrier = java.sql.DriverManager.getConnection(postgres.getJdbcUrl(),
                    postgres.getUsername(), postgres.getPassword());
            try (var statement = barrier.createStatement()) {
                statement.execute("SELECT pg_advisory_lock(" + CHECKPOINT_LOCK + ")");
            }
            assertThat(authoring.withDeadlineAfter(30, TimeUnit.SECONDS)
                    .launchAcceptedWorkflow(launchRequest).getJobId()).isEqualTo(LAUNCH_ID);
            awaitEffectAndBlockedCheckpoint(postgres, fixtureRecords, coordinator, coordinatorLog);

            // The external write is committed, while the checkpoint transaction is still blocked.
            // This is the actual WorkflowRunWorker host, not the separate author worker.
            coordinator.destroyForcibly();
            assertThat(coordinator.waitFor(10, TimeUnit.SECONDS)).isTrue();
            assertThat(coordinator.exitValue()).isNotZero();
            try (var statement = barrier.createStatement()) {
                statement.execute("SELECT pg_advisory_unlock(" + CHECKPOINT_LOCK + ")");
            }
            barrier.close();
            barrier = null;
            JsonNode committedNormalize = assertInterruptedRow(postgres, acceptedSource);
            byte[] committedEffect = Files.readAllBytes(
                    fixtureRecords.resolve(JOB_OPERATION + ".pb"));

            // Kill and restart the external service too: replay must use its fsynced record.
            fixture.destroyForcibly();
            assertThat(fixture.waitFor(10, TimeUnit.SECONDS)).isTrue();
            fixtureLog = directory.resolve("fixture-after.log");
            fixture = startFixture(fixturePort, fixtureRecords, fixtureLog);
            assertThat(awaitFixturePort(fixture, fixtureLog)).isEqualTo(fixturePort);
            expireKilledLease(postgres); // Accelerated eligibility, not a wall-clock five-minute wait.

            coordinatorLog = directory.resolve("coordinator-after.log");
            coordinator = startCoordinator(grpcPort, httpPort, postgres, registry, workspace,
                    authorization, preparations, policy.getSha256(), trustFile, signingKey,
                    accessPolicy, "127.0.0.1:" + repository.getPort(), coordinatorLog);
            awaitHealth(coordinator, httpPort, coordinatorLog);
            awaitCompletedRow(postgres, coordinator, coordinatorLog);
            assertRecoveredRowAndEvents(postgres, acceptedSource, committedNormalize);

            FileSystemFixtureRecordRepository records =
                    new FileSystemFixtureRecordRepository(fixtureRecords);
            FixtureStoredRecord stored = records.find(JOB_OPERATION).orElseThrow();
            assertThat(stored.getRequest().getOperationId()).isEqualTo(JOB_OPERATION);
            assertThat(stored.getRequest().getContent()).isEqualTo("process-input\n value");
            assertThat(stored.getResponse().getContentSha256()).isEqualTo(
                    sha256(stored.getRequest().getContentBytes().toByteArray()));
            assertThat(Files.readAllBytes(fixtureRecords.resolve(JOB_OPERATION + ".pb")))
                    .containsExactly(committedEffect);
        } finally {
            if (barrier != null) barrier.close();
            if (channel != null) channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            stop(author);
            stop(coordinator);
            stop(fixture);
            if (repository != null) repository.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            postgres.stop();
        }
    }

    private static void installCheckpointBarrier(PostgreSQLContainer<?> postgres) throws Exception {
        try (var connection = java.sql.DriverManager.getConnection(postgres.getJdbcUrl(),
                postgres.getUsername(), postgres.getPassword());
             var statement = connection.createStatement()) {
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
                    $$""".formatted(LAUNCH_ID, CHECKPOINT_LOCK));
            statement.execute("CREATE TRIGGER block_write_checkpoint BEFORE INSERT "
                    + "ON workflow_run_events_outbox FOR EACH ROW "
                    + "EXECUTE FUNCTION block_write_checkpoint()");
        }
    }

    private static void awaitEffectAndBlockedCheckpoint(PostgreSQLContainer<?> postgres,
            Path fixtureRecords, Process coordinator, Path log) throws Exception {
        FileSystemFixtureRecordRepository records =
                new FileSystemFixtureRecordRepository(fixtureRecords);
        long deadline = System.nanoTime() + Duration.ofSeconds(90).toNanos();
        while (System.nanoTime() < deadline) {
            if (!coordinator.isAlive()) throw new AssertionError("executor exited:\n" + Files.readString(log));
            boolean effect = records.find(JOB_OPERATION).isPresent();
            try (var connection = java.sql.DriverManager.getConnection(postgres.getJdbcUrl(),
                    postgres.getUsername(), postgres.getPassword());
                 var query = connection.prepareStatement("""
                         SELECT EXISTS (
                           SELECT 1 FROM pg_stat_activity
                            WHERE wait_event_type = 'Lock' AND wait_event = 'advisory'
                              AND query LIKE '%workflow_run_events_outbox%'
                         )""");
                 var result = query.executeQuery()) {
                result.next();
                if (effect && result.getBoolean(1)) return;
            }
            Thread.sleep(100);
        }
        throw new AssertionError("remote effect / blocked checkpoint boundary not reached:\n"
                + Files.readString(log));
    }

    private static JsonNode assertInterruptedRow(PostgreSQLContainer<?> postgres,
            JsonNode acceptedSource) throws Exception {
        JsonNode committedNormalize;
        try (var connection = java.sql.DriverManager.getConnection(postgres.getJdbcUrl(),
                postgres.getUsername(), postgres.getPassword());
             var query = connection.prepareStatement("""
                     SELECT status, attempt, checkpoints, workflow_definition
                       FROM workflow_run WHERE job_id = ?""")) {
            query.setObject(1, java.util.UUID.fromString(LAUNCH_ID));
            try (var result = query.executeQuery()) {
                assertThat(result.next()).isTrue();
                assertThat(result.getString("status")).isEqualTo("RUNNING");
                assertThat(result.getInt("attempt")).isEqualTo(1);
                assertThat(JSON.readTree(result.getString("workflow_definition")))
                        .isEqualTo(acceptedSource);
                JsonNode checkpoints = JSON.readTree(result.getString("checkpoints"));
                assertThat(checkpoints).hasSize(1);
                assertThat(checkpoints.get(0).path("name").asText()).isEqualTo("normalize");
                committedNormalize = checkpoints.get(0);
            }
        }
        assertEventTypes(postgres, List.of("ACCEPTED", "STEP_CHECKPOINT"));
        return committedNormalize;
    }

    private static void expireKilledLease(PostgreSQLContainer<?> postgres) throws Exception {
        try (var connection = java.sql.DriverManager.getConnection(postgres.getJdbcUrl(),
                postgres.getUsername(), postgres.getPassword());
             var update = connection.prepareStatement("""
                     UPDATE workflow_run SET lease_until = clock_timestamp() - interval '1 second'
                      WHERE job_id = ? AND status = 'RUNNING' AND attempt = 1""")) {
            update.setObject(1, java.util.UUID.fromString(LAUNCH_ID));
            assertThat(update.executeUpdate()).isEqualTo(1);
        }
    }

    private static void awaitCompletedRow(PostgreSQLContainer<?> postgres,
            Process coordinator, Path log) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(90).toNanos();
        while (System.nanoTime() < deadline) {
            if (!coordinator.isAlive()) throw new AssertionError("restarted executor exited:\n" + Files.readString(log));
            try (var connection = java.sql.DriverManager.getConnection(postgres.getJdbcUrl(),
                    postgres.getUsername(), postgres.getPassword());
                 var query = connection.prepareStatement(
                         "SELECT status FROM workflow_run WHERE job_id = ?")) {
                query.setObject(1, java.util.UUID.fromString(LAUNCH_ID));
                try (var result = query.executeQuery()) {
                    if (result.next() && "COMPLETED".equals(result.getString(1))) return;
                }
            }
            Thread.sleep(150);
        }
        throw new AssertionError("restarted executor did not complete:\n" + Files.readString(log));
    }

    private static void assertRecoveredRowAndEvents(PostgreSQLContainer<?> postgres,
            JsonNode acceptedSource, JsonNode committedNormalize) throws Exception {
        try (var connection = java.sql.DriverManager.getConnection(postgres.getJdbcUrl(),
                postgres.getUsername(), postgres.getPassword());
             var query = connection.prepareStatement("""
                     SELECT status, attempt, checkpoints, workflow_definition, result
                       FROM workflow_run WHERE job_id = ?""")) {
            query.setObject(1, java.util.UUID.fromString(LAUNCH_ID));
            try (var result = query.executeQuery()) {
                assertThat(result.next()).isTrue();
                assertThat(result.getString("status")).isEqualTo("COMPLETED");
                assertThat(result.getInt("attempt")).isEqualTo(2);
                assertThat(JSON.readTree(result.getString("workflow_definition")))
                        .isEqualTo(acceptedSource);
                JsonNode checkpoints = JSON.readTree(result.getString("checkpoints"));
                assertThat(checkpoints).hasSize(2);
                assertThat(checkpoints.get(0)).isEqualTo(committedNormalize);
                assertThat(checkpoints.get(1).path("name").asText()).isEqualTo("write");
                assertThat(JSON.readTree(result.getString("result")).path("operationId").asText())
                        .isEqualTo(JOB_OPERATION);
            }
        }
        assertEventTypes(postgres, List.of("ACCEPTED", "STEP_CHECKPOINT",
                "STEP_CHECKPOINT", "COMPLETED"));
    }

    private static void assertEventTypes(PostgreSQLContainer<?> postgres,
            List<String> expected) throws Exception {
        try (var connection = java.sql.DriverManager.getConnection(postgres.getJdbcUrl(),
                postgres.getUsername(), postgres.getPassword());
             var query = connection.prepareStatement("""
                     SELECT event_type, payload FROM workflow_run_events_outbox
                      WHERE kafka_key = ? ORDER BY created_at, event_id""")) {
            query.setString(1, LAUNCH_ID);
            try (var result = query.executeQuery()) {
                var actual = new ArrayList<String>();
                var events = new ArrayList<ai.protomolt.proto.jobs.v1.WorkflowRunEvent>();
                while (result.next()) {
                    actual.add(result.getString(1));
                    events.add(ai.protomolt.proto.jobs.v1.WorkflowRunEvent.parseFrom(
                            result.getBytes(2)));
                }
                assertThat(actual).containsExactlyElementsOf(expected);
                assertThat(events).allSatisfy(event -> assertThat(event.getJobId()).isEqualTo(LAUNCH_ID));
                if (expected.size() == 4) {
                    assertThat(events.get(1).getStep()).isEqualTo("normalize");
                    assertThat(events.get(1).getAttempt()).isEqualTo(1);
                    assertThat(events.get(2).getStep()).isEqualTo("write");
                    assertThat(events.get(2).getAttempt()).isEqualTo(2);
                    assertThat(events.get(3).getAttempt()).isEqualTo(2);
                }
            }
        }
    }

    private static Process startFixture(int port, Path records, Path log) throws Exception {
        Path launcher = Path.of("build/install/authoring-fixture/bin/authoring-fixture").toAbsolutePath();
        assertThat(launcher).exists();
        ProcessBuilder builder = new ProcessBuilder(launcher.toString(), Integer.toString(port), records.toString())
                .redirectErrorStream(true).redirectOutput(log.toFile());
        builder.environment().keySet().removeIf(name -> name.startsWith("PROTOMOLT_"));
        return builder.start();
    }

    private static int awaitFixturePort(Process process, Path log) throws Exception {
        String marker = "AuthoringFixtureService listening on port ";
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (System.nanoTime() < deadline) {
            if (!process.isAlive()) throw new AssertionError("fixture exited:\n" + Files.readString(log));
            String output = Files.exists(log) ? Files.readString(log) : "";
            int found = output.indexOf(marker);
            if (found >= 0) return Integer.parseInt(output.substring(found + marker.length()).trim());
            Thread.sleep(50);
        }
        throw new AssertionError("fixture did not start:\n" + Files.readString(log));
    }

    private static Process startCoordinator(int grpcPort, int httpPort, PostgreSQLContainer<?> postgres,
            Path registry, Path workspace, Path authorization, Path preparations, String policySha,
            Path trustFile, Path signingKey, Path accessPolicy, String repositoryEndpoint, Path log)
            throws Exception {
        Path launcher = Path.of("build/install/authoring-coordinator/bin/authoring-coordinator").toAbsolutePath();
        assertThat(launcher).exists();
        var command = new ArrayList<String>();
        command.add(launcher.toString());
        command.addAll(List.of("--host", "127.0.0.1", "--grpc-port", Integer.toString(grpcPort),
                "--http-port", Integer.toString(httpPort), "--registry-port", "0",
                "--registry-git", registry.toString(), "--workflow-workspace", workspace.toString(),
                "--delegation-repo-endpoint", repositoryEndpoint,
                "--delegation-state-key-ref", "env:PROTOMOLT_TRANSCRIPT_KEY",
                "--jobs-jdbc", postgres.getJdbcUrl(), "--jobs-user", postgres.getUsername(),
                "--jobs-password", postgres.getPassword(), "--api-token", OPERATOR_TOKEN,
                "--access-policy", accessPolicy.toString(),
                "--workflow-authoring-policy-sha256", policySha,
                "--workflow-authoring-authorization-dir", authorization.toString(),
                "--workflow-preparation-intent-dir", preparations.toString(),
                "--workflow-preparation-template-provider", AuthoringStarterTemplateProvider.ID));
        ProcessBuilder builder = new ProcessBuilder(command).redirectErrorStream(true)
                .redirectOutput(log.toFile());
        Map<String, String> env = builder.environment();
        env.keySet().removeIf(name -> name.startsWith("PROTOMOLT_"));
        env.put("PROTOMOLT_TRUST_SNAPSHOT", trustFile.toString());
        env.put("PROTOMOLT_TASK_CONSOLE_TOKEN", CONSOLE_TOKEN);
        env.put("PROTOMOLT_RECEIPT_KEY_FILE", signingKey.toString());
        env.put("PROTOMOLT_RECEIPT_KEY_ID", KEY_ID);
        env.put("PROTOMOLT_RECEIPT_ISSUER", ISSUER);
        env.put("PROTOMOLT_TRANSCRIPT_KEY", "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=");
        return builder.start();
    }

    private Process startWorker(int grpcPort, int fixturePort, Path log) throws Exception {
        Path launcher = Path.of("build/install/authoring-worker/bin/authoring-worker").toAbsolutePath();
        assertThat(launcher).exists();
        ProcessBuilder builder = new ProcessBuilder(launcher.toString(), "127.0.0.1:" + grpcPort,
                "127.0.0.1:" + fixturePort, TASK_ID, "1", PRINCIPAL)
                .redirectErrorStream(true).redirectOutput(log.toFile());
        Map<String, String> env = builder.environment();
        env.keySet().removeIf(name -> name.startsWith("PROTOMOLT_"));
        env.put("PROTOMOLT_AUTHOR_TOKEN", AUTHOR_TOKEN);
        return builder.start();
    }

    private static ArtifactReference writePolicy(FileSystemArtifactRepository artifacts, String target)
            throws Exception {
        FileDescriptorSet.Builder descriptorSet = FileDescriptorSet.newBuilder();
        Map<String, FileDescriptor> files = new LinkedHashMap<>();
        collect(WriteRecordRequest.getDescriptor().getFile(), files);
        files.values().forEach(file -> descriptorSet.addFile(file.toProto()));
        ArtifactReference descriptors = artifacts.save(descriptorSet.build().toByteArray(),
                "application/x-protobuf", false);
        String expectedContent = "fixture\n record";
        WriteRecordRequest input = WriteRecordRequest.newBuilder().setOperationId(POLICY_OPERATION)
                .setContent(" \tfixture\r\n record\t ").build();
        WriteRecordResponse output = WriteRecordResponse.newBuilder().setOperationId(POLICY_OPERATION)
                .setContentSha256(sha256(expectedContent.getBytes(StandardCharsets.UTF_8))).build();
        ArtifactReference inputRef = artifacts.save(input.toByteArray(), "application/x-protobuf", false);
        ArtifactReference outputRef = artifacts.save(output.toByteArray(), "application/x-protobuf", false);
        var policy = WorkflowAuthoringPolicy.newBuilder().setDescriptors(descriptors)
                .addFixtures(WorkflowAcceptanceFixture.newBuilder().setName("remote-smoke")
                        .setInput(inputRef).setExpectedOutput(outputRef))
                .addPermittedCalls(WorkflowPermittedCall.newBuilder().setTarget(target)
                        .setMethod("ai.protomolt.proto.samples.authoring.v1.AuthoringFixtureService/NormalizeText"))
                .addPermittedCalls(WorkflowPermittedCall.newBuilder().setTarget(target)
                        .setMethod("ai.protomolt.proto.samples.authoring.v1.AuthoringFixtureService/WriteRecord"))
                .build();
        return artifacts.save(policy.toByteArray(), "application/x-protobuf", false);
    }

    private static void collect(FileDescriptor file, Map<String, FileDescriptor> files) {
        if (files.containsKey(file.getName())) return;
        file.getDependencies().forEach(dependency -> collect(dependency, files));
        files.put(file.getName(), file);
    }

    private JsonNode offerTask(McpSession session, ArtifactReference policy) throws Exception {
        var configured = session.call("get-workflow-authoring-template", JSON.createObjectNode());
        var template = ai.protomolt.proto.workflow.authoring.v1.GetWorkflowAuthoringTemplateResponse.newBuilder();
        JsonFormat.parser().merge(configured.toString(), template);
        assertThat(template.getTemplate().getSpec().getContextList()).contains(policy);
        assertThat(template.getTemplate().getSpec().getContract().getTypeName())
                .isEqualTo(WorkflowAuthoringDeliverable.getDescriptor().getFullName());
        var request = ai.protomolt.proto.workflow.authoring.v1.StartWorkflowAuthoringRequest.newBuilder()
                .setTaskId(TASK_ID).setWorkerId(PRINCIPAL).setTemplateSha256(template.getTemplateSha256())
                .setObjective("Author and independently verify the pinned normalize-record workflow").build();
        return session.call("start-workflow-authoring",
                JSON.readTree(JsonFormat.printer().omittingInsignificantWhitespace().print(request)));
    }

    private static McpSession initializeMcp(int port, String token) throws Exception {
        McpSession session = new McpSession(port, token);
        session = session.initialize();
        if (AUTHOR_TOKEN.equals(token)) {
            assertThat(session.tools()).contains("read-workflow-author-assignments")
                    .doesNotContain("delegation-offer", "get-job", "start-workflow-authoring");
        } else {
            assertThat(session.tools()).contains("delegation-offer", "get-job");
        }
        return session;
    }

    private record McpSession(int port, String token, String sessionId, String version) {
        McpSession(int port, String token) { this(port, token, null, null); }

        McpSession initialize() throws Exception {
            var response = post(port, "/mcp", """
                    {"jsonrpc":"2.0","id":1,"method":"initialize","params":{
                      "protocolVersion":"2025-06-18","capabilities":{},
                      "clientInfo":{"name":"authoring-remote-process-test","version":"1"}}}
                    """, token, null, null);
            assertThat(response.statusCode()).isEqualTo(200);
            String id = response.headers().firstValue("Mcp-Session-Id").orElseThrow();
            String protocol = JSON.readTree(response.body()).path("result").path("protocolVersion").asText();
            assertThat(post(port, "/mcp", "{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}",
                    token, id, protocol).statusCode()).isEqualTo(202);
            return new McpSession(port, token, id, protocol);
        }

        List<String> tools() throws Exception {
            var response = post(port, "/mcp",
                    "{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"tools/list\",\"params\":{}}",
                    token, sessionId, version);
            assertThat(response.statusCode()).as(response.body()).isEqualTo(200);
            return JSON.readTree(response.body()).path("result").path("tools").findValuesAsText("name");
        }

        JsonNode call(String name, JsonNode arguments) throws Exception {
            var params = JSON.createObjectNode().put("name", name).set("arguments", arguments);
            var response = post(port, "/mcp", JSON.createObjectNode().put("jsonrpc", "2.0")
                .put("id", MCP_IDS.getAndIncrement()).put("method", "tools/call").set("params", params).toString(),
                    token, sessionId, version);
            assertThat(response.statusCode()).isEqualTo(200);
            JsonNode result = JSON.readTree(response.body()).path("result");
            assertThat(result.path("isError").asBoolean())
                    .as(() -> result.path("structuredContent").toString()).isFalse();
            return result.path("structuredContent");
        }
    }

    private static HttpResponse<String> post(int port, String path, String body, String token,
            String session, String version) throws Exception {
        HttpRequest.Builder request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path))
                .header("content-type", "application/json")
                .header("accept", "application/json, text/event-stream")
                .timeout(Duration.ofSeconds(10)).POST(HttpRequest.BodyPublishers.ofString(body));
        if (token != null) request.header("api_token", token);
        if (session != null) request.header("Mcp-Session-Id", session);
        if (version != null) request.header("MCP-Protocol-Version", version);
        return HTTP.send(request.build(), HttpResponse.BodyHandlers.ofString());
    }

    private static void awaitHealth(Process process, int port, Path log) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        while (System.nanoTime() < deadline) {
            if (!process.isAlive()) throw new AssertionError("coordinator exited:\n" + Files.readString(log));
            try {
                var response = HTTP.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/health"))
                                .timeout(Duration.ofSeconds(3)).GET().build(), HttpResponse.BodyHandlers.ofString());
                if (response.statusCode() == 200) return;
            } catch (Exception ignored) { }
            Thread.sleep(150);
        }
        throw new AssertionError("coordinator health timeout:\n" + Files.readString(log));
    }

    private static void awaitOutput(Process process, Path log, String marker, Duration timeout)
            throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            String output = Files.exists(log) ? Files.readString(log) : "";
            if (output.contains(marker)) return;
            if (!process.isAlive()) throw new AssertionError("process exited before '" + marker + "':\n" + output);
            Thread.sleep(100);
        }
        throw new AssertionError("process did not reach '" + marker + "':\n" + Files.readString(log));
    }

    private static TrustSnapshot writeSigningAndTrust(Path keyFile, Path trustFile) throws Exception {
        KeyPair pair = RecordKeys.generate();
        byte[] encoded = pair.getPrivate().getEncoded();
        Files.write(keyFile, java.util.Arrays.copyOfRange(encoded, encoded.length - 32, encoded.length));
        TrustSnapshot trust = TrustSnapshot.newBuilder().addIssuers(TrustedIssuer.newBuilder()
                .setIssuer(ISSUER).addSubjectKinds(WorkRecords.SUBJECT_KIND_WORKFLOW_RUN)
                .addKeys(TrustedKey.newBuilder().setKeyId(KEY_ID)
                        .setAlgorithm(SignatureAlgorithm.SIGNATURE_ALGORITHM_ED25519)
                        .setState(KeyState.KEY_STATE_ACTIVE)
                        .setPublicKey(ByteString.copyFrom(RecordKeys.rawPublicKey(pair.getPublic())))))
                .build();
        Files.write(trustFile, trust.toByteArray());
        return trust;
    }

    private static String accessPolicyJson() throws Exception {
        return """
                {"principals":[
                  {"name":"%s","credentialSha256":["%s"],"scopes":["workflow-author"]},
                  {"name":"browser-launcher","credentialSha256":["%s"],"scopes":["worker-coordinate","workflow-launch"]}
                ]}
                """.formatted(PRINCIPAL, sha256(AUTHOR_TOKEN.getBytes(StandardCharsets.UTF_8)),
                        sha256(BROWSER_TOKEN.getBytes(StandardCharsets.UTF_8)));
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

    private static String sha256(byte[] bytes) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
    }

    private static void stop(Process process) throws Exception {
        if (process == null) return;
        if (process.isAlive()) {
            process.destroy();
            if (!process.waitFor(5, TimeUnit.SECONDS)) {
                process.destroyForcibly();
                process.waitFor(5, TimeUnit.SECONDS);
            }
        }
    }

    private static final class FakeDocumentService extends DocumentServiceGrpc.DocumentServiceImplBase {
        private final Map<String, StoredObject> objects = new java.util.concurrent.ConcurrentHashMap<>();
        @Override public void getBlob(GetBlobRequest request, StreamObserver<GetBlobResponse> observer) {
            StoredObject stored = objects.get(key(request.getStorageRef().getDriveName(),
                    request.getStorageRef().getObjectKey()));
            if (stored == null) { observer.onError(Status.NOT_FOUND.asRuntimeException()); return; }
            observer.onNext(GetBlobResponse.newBuilder().setData(stored.bytes()).setSizeBytes(stored.bytes().size())
                    .setMimeType(stored.mimeType()).build());
            observer.onCompleted();
        }
        @Override public synchronized void putBlob(PutBlobRequest request, StreamObserver<PutBlobResponse> observer) {
            ByteString bytes = request.getData();
            objects.put(key(request.getDriveName(), request.getObjectKey()),
                    new StoredObject(bytes, request.getMimeType()));
            observer.onNext(PutBlobResponse.newBuilder().setStorageRef(
                    FileStorageReference.newBuilder().setDriveName(request.getDriveName())
                            .setObjectKey(request.getObjectKey()))
                    .setSizeBytes(bytes.size()).setSha256(sha256Unchecked(bytes.toByteArray())).build());
            observer.onCompleted();
        }
        @Override public synchronized void getBlobForUpdate(GetBlobForUpdateRequest request,
                StreamObserver<GetBlobForUpdateResponse> observer) {
            StoredObject stored = objects.get(key(request.getKey().getDriveName(), request.getKey().getObjectKey()));
            if (stored == null) { observer.onError(Status.NOT_FOUND.asRuntimeException()); return; }
            observer.onNext(GetBlobForUpdateResponse.newBuilder().setData(stored.bytes())
                    .setVersion(version(request.getKey(), stored)).setMimeType(stored.mimeType()).build());
            observer.onCompleted();
        }
        @Override public synchronized void compareAndPutBlob(CompareAndPutBlobRequest request,
                StreamObserver<CompareAndPutBlobResponse> observer) {
            String objectKey = key(request.getKey().getDriveName(), request.getKey().getObjectKey());
            StoredObject current = objects.get(objectKey);
            boolean matches = switch (request.getPreconditionCase()) {
                case IF_ABSENT -> request.getIfAbsent() && current == null;
                case EXPECTED_ETAG -> current != null && current.etag().equals(request.getExpectedEtag());
                default -> false;
            };
            if (!matches) { observer.onError(Status.ABORTED.asRuntimeException()); return; }
            StoredObject stored = new StoredObject(request.getData(), request.getMimeType());
            objects.put(objectKey, stored);
            observer.onNext(CompareAndPutBlobResponse.newBuilder().setVersion(version(request.getKey(), stored)).build());
            observer.onCompleted();
        }
        private static ConditionalBlobVersion version(ConditionalBlobKey key, StoredObject stored) {
            return ConditionalBlobVersion.newBuilder().setKey(key).setEtag(stored.etag())
                    .setSizeBytes(stored.bytes().size()).setSha256(sha256Unchecked(stored.bytes().toByteArray())).build();
        }
        private record StoredObject(ByteString bytes, String mimeType) {
            String etag() { return "\"" + sha256Unchecked(bytes.toByteArray()) + "\""; }
        }
        private static String key(String drive, String object) { return drive + "/" + object; }
        private static String sha256Unchecked(byte[] bytes) {
            try { return sha256(bytes); } catch (Exception impossible) { throw new AssertionError(impossible); }
        }
    }
}
