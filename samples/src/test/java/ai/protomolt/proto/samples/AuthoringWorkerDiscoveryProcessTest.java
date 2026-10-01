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
import ai.protomolt.proto.samples.authoring.v1.AuthoringWorkerState;
import ai.protomolt.proto.samples.authoring.v1.AuthoringFixtureServiceGrpc;
import ai.protomolt.proto.samples.authoring.v1.FixtureStoredRecord;
import ai.protomolt.proto.samples.authoring.v1.NormalizeTextRequest;
import ai.protomolt.proto.samples.authoring.v1.NormalizeTextResponse;
import ai.protomolt.proto.samples.authoring.v1.WriteRecordRequest;
import ai.protomolt.proto.samples.authoring.v1.WriteRecordResponse;
import ai.protomolt.proto.samples.starter.v1.WorkflowAcceptanceFixture;
import ai.protomolt.proto.samples.starter.v1.WorkflowAuthoringPolicy;
import ai.protomolt.proto.samples.starter.v1.WorkflowAuthoringDeliverable;
import ai.protomolt.proto.samples.starter.v1.WorkflowPermittedCall;
import ai.protomolt.proto.workflow.authoring.FileSystemWorkflowPreparationRepository;
import ai.protomolt.proto.workflow.authoring.v1.ReadWorkflowAuthorAssignmentsRequest;
import ai.protomolt.proto.workflow.authoring.v1.ReadWorkflowAuthorEventsRequest;
import ai.protomolt.proto.workflow.authoring.v1.WorkflowAuthorAssignment;
import ai.protomolt.proto.workflow.authoring.v1.WorkflowAuthorTaskServiceGrpc;
import ai.protomolt.proto.workflow.authoring.v1.WorkflowPreparationIntent;
import ai.protomolt.proto.repo.v1.ConditionalBlobKey;
import ai.protomolt.proto.repo.v1.ConditionalBlobVersion;
import ai.protomolt.proto.repo.v1.CompareAndPutBlobRequest;
import ai.protomolt.proto.repo.v1.CompareAndPutBlobResponse;
import ai.protomolt.proto.repo.v1.DocumentServiceGrpc;
import ai.protomolt.proto.repo.v1.FileStorageReference;
import ai.protomolt.proto.repo.v1.GetBlobForUpdateRequest;
import ai.protomolt.proto.repo.v1.GetBlobForUpdateResponse;
import ai.protomolt.proto.repo.v1.GetBlobRequest;
import ai.protomolt.proto.repo.v1.GetBlobResponse;
import ai.protomolt.proto.repo.v1.PutBlobRequest;
import ai.protomolt.proto.repo.v1.PutBlobResponse;
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
import io.grpc.protobuf.services.ProtoReflectionService;
import java.net.InetSocketAddress;
import java.io.IOException;
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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;

/** Installed idle-worker recovery through a committed fixture effect and exact retry. */
@Tag("integration")
@Testcontainers(disabledWithoutDocker = true)
class AuthoringWorkerDiscoveryProcessTest {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final HttpClient HTTP = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    private static final String OPERATOR_TOKEN = "discovery-process-operator-token";
    private static final String AUTHOR_TOKEN = "discovery-process-author-token";
    private static final String TASK_ID = "00000000-0000-4000-8000-000000000551";
    private static final String POLICY_OPERATION = "00000000-0000-4000-8000-000000000552";
    private static final String ISSUER = "discovery-process-issuer";
    private static final String KEY_ID = "discovery-process-key";
    private static final String PRINCIPAL = "discovery-author";
    private static final String PROBE_CONTENT = "Hello\nworld";

    @TempDir Path directory;

    @Test
    void idleWorkerDiscoversThenRecoversExactProbeAndPreparationIntentsAfterProcessKills() throws Exception {
        var postgres = new PostgreSQLContainer<>(DockerImageName.parse("postgres:16-alpine"))
                .withDatabaseName("protomolt").withUsername("protomolt").withPassword("test-password");
        Process coordinator = null;
        Process worker = null;
        Server repository = null;
        Server fixture = null;
        ManagedChannel authorChannel = null;
        try {
            postgres.start();
            repository = ServerBuilder.forPort(0).addService(new FakeDocumentService()).build().start();

            var fixtureRecords = new FileSystemFixtureRecordRepository(directory.resolve("fixture-records"));
            var fixtureService = new BlockingFixtureService(fixtureRecords);
            fixture = ServerBuilder.forPort(0).addService(fixtureService)
                    .addService(ProtoReflectionService.newInstance()).build().start();
            String fixtureTarget = "127.0.0.1:" + fixture.getPort();

            Path workspace = directory.resolve("workflow-workspace");
            Path artifactsPath = workspace.resolve("artifacts");
            Path registry = directory.resolve("registry.git");
            Path authorization = directory.resolve("launch-authorizations");
            Path preparations = directory.resolve("preparations");
            Path trustFile = directory.resolve("trust.binpb");
            Path signingKey = directory.resolve("signing.seed");
            Path accessPolicy = directory.resolve("access-policy.json");
            writeSigningAndTrust(signingKey, trustFile);
            Files.writeString(accessPolicy, accessPolicyJson());
            FileSystemArtifactRepository artifacts = new FileSystemArtifactRepository(artifactsPath);
            ArtifactReference policy = writePolicy(artifacts, fixtureTarget);

            int grpcPort = freePort();
            int httpPort = freePort();
            Path coordinatorLog = directory.resolve("coordinator.log");
            coordinator = startCoordinator(grpcPort, httpPort, postgres, registry, workspace,
                    authorization, preparations, policy.getSha256(), trustFile, signingKey,
                    accessPolicy, "127.0.0.1:" + repository.getPort(), coordinatorLog);
            awaitHealth(coordinator, httpPort, coordinatorLog);

            authorChannel = ManagedChannelBuilder.forAddress("127.0.0.1", grpcPort)
                    .usePlaintext().maxInboundMessageSize(16 * 1024 * 1024).build();
            var authorTasks = WorkflowAuthorTaskServiceGrpc.newBlockingStub(authorChannel)
                    .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(bearer(AUTHOR_TOKEN)));
            Path stateDirectory = directory.resolve("worker-state");
            Path stateFile = stateDirectory.resolve("authoring-worker-state.pb");
            Path workerLog = directory.resolve("worker-first.log");

            Gate probeGate = fixtureService.blockNextWrite();
            worker = startWorker(grpcPort, fixture.getPort(), stateDirectory, workerLog);
            awaitOutput(worker, workerLog, "AuthoringWorker ready", Duration.ofSeconds(30), coordinatorLog);
            AuthoringWorkerState idle = awaitState(stateFile, state -> state.getDiscoveryCursor() > 0
                    && state.getPendingCount() == 0, Duration.ofSeconds(20));
            assertThat(fixtureService.writeRequests()).isEmpty();

            McpSession operator = initializeMcp(httpPort, OPERATOR_TOKEN);
            JsonNode started = offerTask(operator, policy);
            assertThat(started.path("request").path("taskId").asText()).isEqualTo(TASK_ID);
            assertThat(started.path("offer").path("attempt").asInt()).isEqualTo(1);
            WorkflowAuthorAssignment assignment = awaitAssignment(authorTasks, Duration.ofSeconds(20));
            assertThat(assignment.getTaskId()).isEqualTo(TASK_ID);
            assertThat(idle.getDiscoveryCursor()).isLessThan(assignment.getCursor());

            awaitGate(probeGate, Duration.ofSeconds(45), "probe committed before reply", coordinatorLog, workerLog);
            AuthoringWorkerState atProbe = readState(stateFile);
            assertThat(atProbe.getPendingCount()).isEqualTo(1);
            var probePending = atProbe.getPending(0);
            assertThat(probePending.getAssignment()).isEqualTo(assignment);
            assertThat(probePending.getProbe().getOperationId())
                    .isEqualTo(AuthoringWorkerStateStore.probeOperationId(assignment, PRINCIPAL));
            assertThat(probePending.getProbe().getContent()).isEqualTo(PROBE_CONTENT);
            assertThat(probePending.hasPreparation()).isFalse();
            assertThat(fixtureRecords.find(probePending.getProbe().getOperationId())).isPresent();
            kill(worker);
            worker = null;
            probeGate.release().countDown();

            Gate preparationGate = fixtureService.blockOperation(POLICY_OPERATION);
            Path retryLog = directory.resolve("worker-after-probe-kill.log");
            worker = startWorker(grpcPort, fixture.getPort(), stateDirectory, retryLog);
            awaitOutput(worker, retryLog, "AuthoringWorker ready", Duration.ofSeconds(30), coordinatorLog);
            awaitGate(preparationGate, Duration.ofSeconds(60), "preparation reached a committed fixture effect",
                    coordinatorLog, retryLog);
            AuthoringWorkerState atPreparation = readState(stateFile);
            assertThat(atPreparation.getPendingCount()).isEqualTo(1);
            var preparationPending = atPreparation.getPending(0);
            assertThat(preparationPending.getAssignment()).isEqualTo(assignment);
            assertThat(preparationPending.getProbe()).isEqualTo(probePending.getProbe());
            assertThat(preparationPending.hasPreparation()).isTrue();
            var savedPreparation = preparationPending.getPreparation();
            assertThat(savedPreparation.getPreparationId())
                    .isEqualTo(AuthoringWorkerStateStore.preparationId(assignment, PRINCIPAL));
            assertThat(savedPreparation.getExecutableSourceJson()).isNotEmpty();
            kill(worker);
            worker = null;
            preparationGate.release().countDown();

            Path finalLog = directory.resolve("worker-after-preparation-kill.log");
            worker = startWorker(grpcPort, fixture.getPort(), stateDirectory, finalLog);
            awaitOutput(worker, finalLog, "AuthoringWorker ready", Duration.ofSeconds(30), coordinatorLog);
            awaitOutput(worker, finalLog, "AuthoringWorker accepted task=" + TASK_ID,
                    Duration.ofSeconds(120), coordinatorLog);
            WorkflowPreparationIntent committed = awaitCompletedPreparation(preparations, Duration.ofSeconds(30),
                    coordinatorLog, List.of(workerLog, retryLog, finalLog));
            assertThat(committed.getRequest()).isEqualTo(savedPreparation);
            AuthoringWorkerState completed = awaitState(stateFile, state -> state.getPendingCount() == 0
                    && state.getDiscoveryCursor() >= assignment.getCursor(), Duration.ofSeconds(20));
            assertThat(completed.getPendingCount()).isZero();

            var eventPage = authorTasks.withDeadlineAfter(10, TimeUnit.SECONDS)
                    .readWorkflowAuthorEvents(ReadWorkflowAuthorEventsRequest.newBuilder()
                            .setTaskId(TASK_ID).setAttempt(1).setMaxEvents(64).build());
            var candidates = eventPage.getEventsList().stream().filter(event -> event.getEntry().hasWorkerFrame())
                    .filter(event -> event.getEntry().getWorkerFrame().hasCompletion())
                    .toList();
            assertThat(candidates).hasSize(1);
            var candidate = candidates.getFirst().getEntry().getWorkerFrame().getCompletion();
            var prepared = new FileSystemWorkflowPreparationRepository(preparations)
                    .find(TASK_ID, 1, 1).orElseThrow();
            assertThat(prepared.hasCompleted()).isTrue();
            assertThat(prepared.getIntent().getRequest()).isEqualTo(savedPreparation);
            assertThat(candidate.getResult().unpack(WorkflowAuthoringDeliverable.class))
                    .isEqualTo(prepared.getCompleted().getAuthored());
            assertThat(candidate.getAttempt()).isEqualTo(1);
            assertThat(candidate.getRevision()).isEqualTo(1);
            assertThat(eventPage.getEventsList().stream().filter(event -> event.getEntry().hasCoordinatorFrame())
                    .anyMatch(event -> event.getEntry().getCoordinatorFrame().hasAccepted())).isTrue();

            String probeId = probePending.getProbe().getOperationId();
            List<WriteRecordRequest> probeCalls = fixtureService.writeRequests().stream()
                    .filter(request -> request.getOperationId().equals(probeId)).toList();
            assertThat(probeCalls).hasSize(3).containsOnly(probePending.getProbe());
            assertThat(fixtureRecords.find(probeId)).isPresent().get()
                    .extracting(FixtureStoredRecord::getRequest).isEqualTo(probePending.getProbe());
            var fixtureCalls = fixtureService.writeRequests().stream()
                    .filter(request -> request.getOperationId().equals(POLICY_OPERATION)).toList();
            // Preparation and independent review can deliver this operation again.
            // The contract requires identical intent and one stored effect, not one delivery.
            var expectedFixture = WriteRecordRequest.newBuilder().setOperationId(POLICY_OPERATION)
                    .setContent("fixture\n record").build();
            assertThat(fixtureCalls).hasSizeGreaterThanOrEqualTo(2).containsOnly(expectedFixture);
            assertThat(fixtureRecords.find(POLICY_OPERATION)).isPresent().get().extracting(FixtureStoredRecord::getRequest)
                    .isEqualTo(expectedFixture);
            try (var entries = Files.list(directory.resolve("fixture-records"))) {
                assertThat(entries.filter(path -> path.getFileName().toString().endsWith(".pb")).count())
                        .isEqualTo(2L);
            }
        } finally {
            if (worker != null) kill(worker);
            if (coordinator != null) stop(coordinator);
            if (authorChannel != null) authorChannel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            if (fixture != null) fixture.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            if (repository != null) repository.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            postgres.stop();
        }
    }

    private static Process startWorker(int grpcPort, int fixturePort, Path stateDirectory, Path log)
            throws Exception {
        Path launcher = Path.of("build/install/authoring-worker/bin/authoring-worker").toAbsolutePath();
        assertThat(launcher).exists();
        ProcessBuilder builder = new ProcessBuilder(launcher.toString(), "--discover",
                "127.0.0.1:" + grpcPort, "127.0.0.1:" + fixturePort, PRINCIPAL,
                stateDirectory.toString()).redirectErrorStream(true).redirectOutput(log.toFile());
        var env = builder.environment();
        env.keySet().removeIf(name -> name.startsWith("PROTOMOLT_"));
        env.put("PROTOMOLT_AUTHOR_TOKEN", AUTHOR_TOKEN);
        return builder.start();
    }

    private static JsonNode offerTask(McpSession session, ArtifactReference policy) throws Exception {
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

    private static WorkflowAuthorAssignment awaitAssignment(
            WorkflowAuthorTaskServiceGrpc.WorkflowAuthorTaskServiceBlockingStub tasks, Duration timeout)
            throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            var response = tasks.withDeadlineAfter(5, TimeUnit.SECONDS)
                    .readWorkflowAuthorAssignments(ReadWorkflowAuthorAssignmentsRequest.newBuilder()
                            .setMaxAssignments(64).build());
            if (!response.getAssignmentsList().isEmpty()) return response.getAssignments(0);
            Thread.sleep(50);
        }
        throw new AssertionError("timed out waiting for worker assignment");
    }

    private static AuthoringWorkerState awaitState(Path file,
            java.util.function.Predicate<AuthoringWorkerState> condition, Duration timeout) throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (Files.isRegularFile(file)) {
                AuthoringWorkerState state = readState(file);
                if (condition.test(state)) return state;
            }
            Thread.sleep(25);
        }
        throw new AssertionError("timed out waiting for worker state " + file);
    }

    private static AuthoringWorkerState readState(Path file) throws Exception {
        return AuthoringWorkerState.parseFrom(Files.readAllBytes(file));
    }

    private static WorkflowPreparationIntent awaitCompletedPreparation(Path directory, Duration timeout,
            Path coordinatorLog, List<Path> workerLogs) throws Exception {
        var repository = new FileSystemWorkflowPreparationRepository(directory);
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            var record = repository.find(TASK_ID, 1, 1);
            if (record.isPresent() && record.get().hasCompleted()) return record.get().getIntent();
            Thread.sleep(25);
        }
        StringBuilder evidence = new StringBuilder("preparation did not commit after worker recovery\nCoordinator log:\n");
        if (Files.exists(coordinatorLog)) evidence.append(Files.readString(coordinatorLog));
        for (Path workerLog : workerLogs) {
            evidence.append("\nWorker log ").append(workerLog).append(":\n");
            if (Files.exists(workerLog)) evidence.append(Files.readString(workerLog));
        }
        var last = repository.find(TASK_ID, 1, 1);
        if (last.isPresent()) evidence.append("\nPreparation record state: ").append(last.get().getStateCase());
        throw new AssertionError(evidence.toString());
    }

    private static void awaitOutput(Process process, Path log, String marker, Duration timeout,
            Path coordinatorLog) throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (!process.isAlive()) throw new AssertionError("worker exited:\n" + Files.readString(log)
                    + "\nCoordinator log:\n" + (Files.exists(coordinatorLog) ? Files.readString(coordinatorLog) : ""));
            if (Files.exists(log) && Files.readString(log).contains(marker)) return;
            Thread.sleep(25);
        }
        throw new AssertionError("worker missed " + marker + ":\n" + Files.readString(log)
                + "\nCoordinator log:\n" + (Files.exists(coordinatorLog) ? Files.readString(coordinatorLog) : ""));
    }

    private static void awaitGate(Gate gate, Duration timeout, String description,
            Path coordinatorLog, Path workerLog) throws Exception {
        if (gate.entered().await(timeout.toNanos(), TimeUnit.NANOSECONDS)) return;
        throw new AssertionError(description + " was not reached\nCoordinator log:\n"
                + (Files.exists(coordinatorLog) ? Files.readString(coordinatorLog) : "")
                + "\nWorker log:\n" + (Files.exists(workerLog) ? Files.readString(workerLog) : ""));
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
            Thread.sleep(100);
        }
        throw new AssertionError("coordinator health timeout:\n" + Files.readString(log));
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
        ProcessBuilder builder = new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(log.toFile());
        Map<String, String> env = builder.environment();
        env.keySet().removeIf(name -> name.startsWith("PROTOMOLT_"));
        env.put("PROTOMOLT_TRUST_SNAPSHOT", trustFile.toString());
        env.put("PROTOMOLT_RECEIPT_KEY_FILE", signingKey.toString());
        env.put("PROTOMOLT_RECEIPT_KEY_ID", "discovery-process-key");
        env.put("PROTOMOLT_RECEIPT_ISSUER", "discovery-process-issuer");
        env.put("PROTOMOLT_TRANSCRIPT_KEY", "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=");
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
        String expected = "fixture\n record";
        WriteRecordRequest input = WriteRecordRequest.newBuilder().setOperationId(POLICY_OPERATION)
                .setContent(" \tfixture\r\n record\t ").build();
        WriteRecordResponse output = WriteRecordResponse.newBuilder().setOperationId(POLICY_OPERATION)
                .setContentSha256(sha256(expected.getBytes(StandardCharsets.UTF_8))).build();
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

    private static void writeSigningAndTrust(Path keyFile, Path trustFile) throws Exception {
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
    }

    private static String accessPolicyJson() throws Exception {
        return """
                {"principals":[{"name":"%s","credentialSha256":["%s"],"scopes":["workflow-author"]}]}
                """.formatted(PRINCIPAL, sha256(AUTHOR_TOKEN.getBytes(StandardCharsets.UTF_8)));
    }

    private static McpSession initializeMcp(int port, String token) throws Exception {
        var response = post(port, """
                {"jsonrpc":"2.0","id":1,"method":"initialize","params":{
                  "protocolVersion":"2025-06-18","capabilities":{},
                  "clientInfo":{"name":"authoring-worker-discovery-test","version":"1"}}}
                """, token, null, null);
        assertThat(response.statusCode()).isEqualTo(200);
        String session = response.headers().firstValue("Mcp-Session-Id").orElseThrow();
        String protocol = JSON.readTree(response.body()).path("result").path("protocolVersion").asText();
        assertThat(post(port, "{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}",
                token, session, protocol).statusCode()).isEqualTo(202);
        return new McpSession(port, token, session, protocol);
    }

    private record McpSession(int port, String token, String session, String protocol) {
        JsonNode call(String name, JsonNode args) throws Exception {
            var params = JSON.createObjectNode().put("name", name).set("arguments", args);
            var response = post(port, JSON.createObjectNode().put("jsonrpc", "2.0").put("id", 2)
                    .put("method", "tools/call").set("params", params).toString(), token, session, protocol);
            assertThat(response.statusCode()).as(response.body()).isEqualTo(200);
            JsonNode result = JSON.readTree(response.body()).path("result");
            assertThat(result.path("isError").asBoolean()).as(result.toString()).isFalse();
            return result.path("structuredContent");
        }
    }

    private static HttpResponse<String> post(int port, String body, String token, String session, String protocol)
            throws Exception {
        var request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/mcp"))
                .header("Content-Type", "application/json").header("Accept", "application/json, text/event-stream")
                .timeout(Duration.ofSeconds(15)).POST(HttpRequest.BodyPublishers.ofString(body));
        if (token != null) request.header("api_token", token);
        if (session != null) request.header("Mcp-Session-Id", session);
        if (protocol != null) request.header("MCP-Protocol-Version", protocol);
        return HTTP.send(request.build(), HttpResponse.BodyHandlers.ofString());
    }

    private static Metadata bearer(String token) {
        Metadata metadata = new Metadata();
        metadata.put(Metadata.Key.of("api_token", Metadata.ASCII_STRING_MARSHALLER), token);
        return metadata;
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

    private static void kill(Process process) throws Exception {
        if (process.isAlive()) {
            process.destroyForcibly();
            assertThat(process.waitFor(10, TimeUnit.SECONDS)).isTrue();
        }
    }

    private static void stop(Process process) throws Exception {
        if (process.isAlive()) {
            process.destroy();
            if (!process.waitFor(5, TimeUnit.SECONDS)) kill(process);
        }
    }

    private record Gate(CountDownLatch entered, CountDownLatch release) {}

    private static final class BlockingFixtureService extends AuthoringFixtureServiceGrpc.AuthoringFixtureServiceImplBase {
        private final FileSystemFixtureRecordRepository records;
        private final CopyOnWriteArrayList<WriteRecordRequest> requests = new CopyOnWriteArrayList<>();
        private final AtomicReference<Gate> nextWriteGate = new AtomicReference<>();
        private final java.util.concurrent.ConcurrentMap<String, Gate> operationGates =
                new java.util.concurrent.ConcurrentHashMap<>();

        BlockingFixtureService(FileSystemFixtureRecordRepository records) { this.records = records; }

        Gate blockNextWrite() {
            Gate gate = new Gate(new CountDownLatch(1), new CountDownLatch(1));
            if (!nextWriteGate.compareAndSet(null, gate)) throw new IllegalStateException("write gate already armed");
            return gate;
        }

        Gate blockOperation(String operationId) {
            Gate gate = new Gate(new CountDownLatch(1), new CountDownLatch(1));
            if (operationGates.putIfAbsent(operationId, gate) != null) {
                throw new IllegalStateException("operation gate already armed");
            }
            return gate;
        }

        List<WriteRecordRequest> writeRequests() { return List.copyOf(requests); }

        @Override public void normalizeText(NormalizeTextRequest request,
                StreamObserver<NormalizeTextResponse> observer) {
            try {
                FixtureValidation.validate(request);
                var response = NormalizeTextResponse.newBuilder()
                        .setText(AuthoringFixtureService.normalize(request.getText())).build();
                FixtureValidation.validate(response);
                observer.onNext(response);
                observer.onCompleted();
            } catch (RuntimeException invalid) {
                observer.onError(Status.INVALID_ARGUMENT.withDescription("invalid normalization").asRuntimeException());
            }
        }

        @Override public void writeRecord(WriteRecordRequest request, StreamObserver<WriteRecordResponse> observer) {
            try {
                WriteRecordResponse response = records.writeOrMatch(request);
                requests.add(request);
                Gate gate = nextWriteGate.getAndSet(null);
                if (gate == null) gate = operationGates.remove(request.getOperationId());
                if (gate != null) {
                    gate.entered().countDown();
                    if (!gate.release().await(60, TimeUnit.SECONDS)) {
                        throw new IOException("test write response gate timed out");
                    }
                }
                observer.onNext(response);
                observer.onCompleted();
            } catch (FixtureRecordConflictException conflict) {
                observer.onError(Status.ALREADY_EXISTS.asRuntimeException());
            } catch (IOException | RuntimeException failure) {
                observer.onError(Status.INTERNAL.withDescription("fixture write failed").asRuntimeException());
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                observer.onError(Status.CANCELLED.asRuntimeException());
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
            objects.put(key(request.getDriveName(), request.getObjectKey()), new StoredObject(bytes, request.getMimeType()));
            observer.onNext(PutBlobResponse.newBuilder().setStorageRef(FileStorageReference.newBuilder()
                    .setDriveName(request.getDriveName()).setObjectKey(request.getObjectKey()))
                    .setSizeBytes(bytes.size()).setSha256(shaUnchecked(bytes.toByteArray())).build());
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
                    .setSizeBytes(stored.bytes().size()).setSha256(shaUnchecked(stored.bytes().toByteArray())).build();
        }
        private record StoredObject(ByteString bytes, String mimeType) {
            String etag() { return "\"" + shaUnchecked(bytes.toByteArray()) + "\""; }
        }
        private static String key(String drive, String object) { return drive + "/" + object; }
        private static String shaUnchecked(byte[] bytes) {
            try { return sha256(bytes); } catch (Exception impossible) { throw new AssertionError(impossible); }
        }
    }
}
