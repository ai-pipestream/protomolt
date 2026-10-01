package ai.protomolt.proto.samples;

import ai.protomolt.proto.delegation.v1.AcceptanceCheck;
import ai.protomolt.proto.delegation.v1.DeliverableContract;
import ai.protomolt.proto.delegation.v1.OfferTaskRequest;
import ai.protomolt.proto.delegation.v1.TaskSpec;
import ai.protomolt.proto.grpc.workflow.FileSystemArtifactRepository;
import ai.protomolt.proto.grpc.workflow.v1.ArtifactReference;
import ai.protomolt.proto.receipt.KeyState;
import ai.protomolt.proto.receipt.RecordKeys;
import ai.protomolt.proto.receipt.RecordVerifier;
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
import ai.protomolt.proto.workflow.authoring.WorkflowAuthoringReviewer;
import ai.protomolt.proto.workflow.authoring.v1.WorkflowPreparationIntent;
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

/** Installed-process proof of remote authoring, independent review, signed preparation and launch. */
@Tag("integration")
@Testcontainers(disabledWithoutDocker = true)
class AuthoringRemoteProcessTest {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final HttpClient HTTP = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5)).build();
    private static final String OPERATOR_TOKEN = "remote-process-operator-token";
    private static final String AUTHOR_TOKEN = "remote-process-author-token";
    private static final String BROWSER_TOKEN = "remote-process-browser-launch-token";
    private static final String CONSOLE_TOKEN = "remote-process-default-console-token";
    private static final String TASK_ID = "00000000-0000-4000-8000-000000000441";
    private static final String POLICY_OPERATION = "00000000-0000-4000-8000-000000000442";
    private static final String JOB_OPERATION = "00000000-0000-4000-8000-000000000443";
    private static final String LAUNCH_ID = "00000000-0000-4000-8000-000000000444";
    private static final String ISSUER = "remote-authoring-coordinator";
    private static final String KEY_ID = "remote-authoring-signing-key";
    private static final String PRINCIPAL = "scripted-author";
    private static final AtomicInteger MCP_IDS = new AtomicInteger(10);

    @TempDir Path directory;

    @Test
    void installedCoordinatorFixtureAndWorkerPrepareReviewLaunchAndPersistDistinctOperations()
            throws Exception {
        var postgres = new PostgreSQLContainer<>(DockerImageName.parse("postgres:16-alpine"))
                .withDatabaseName("protomolt").withUsername("protomolt").withPassword("test-password");
        Process coordinator = null;
        Process fixture = null;
        Process worker = null;
        Server repository = null;
        ManagedChannel channel = null;
        try {
            postgres.start();
            repository = ServerBuilder.forPort(0).addService(new FakeDocumentService()).build().start();

            Path fixtureRecords = directory.resolve("fixture-records");
            Path fixtureLog = directory.resolve("fixture.log");
            fixture = startFixture(fixtureRecords, fixtureLog);
            int fixturePort = awaitFixturePort(fixture, fixtureLog);
            String fixtureTarget = "127.0.0.1:" + fixturePort;

            Path workspace = directory.resolve("workflow-workspace");
            Path artifactsPath = workspace.resolve("artifacts");
            Path registry = directory.resolve("registry.git");
            Path authorization = directory.resolve("launch-authorizations");
            Path preparations = directory.resolve("preparations");
            Path trustFile = directory.resolve("trust.binpb");
            Path signingKey = directory.resolve("signing.seed");
            Path accessPolicy = directory.resolve("access-policy.json");
            TrustSnapshot trust = writeSigningAndTrust(signingKey, trustFile);
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

            var mcp = initializeMcp(httpPort, OPERATOR_TOKEN);
            Path workerLog = directory.resolve("worker.log");
            worker = startWorker(grpcPort, fixturePort, workerLog);
            awaitOutput(worker, workerLog, "AuthoringWorker ready", Duration.ofSeconds(30));
            var offered = offerTask(mcp, policy);
            assertThat(offered.path("taskId").asText()).isEqualTo(TASK_ID);
            try {
                awaitOutput(worker, workerLog, "AuthoringWorker accepted task=" + TASK_ID,
                        Duration.ofSeconds(120));
            } catch (AssertionError failed) {
                throw new AssertionError(failed.getMessage() + "\nCoordinator log:\n"
                        + Files.readString(coordinatorLog), failed);
            }
            assertThat(worker.waitFor(10, TimeUnit.SECONDS)).isTrue();
            assertThat(worker.exitValue()).as(Files.readString(workerLog)).isZero();

            channel = ManagedChannelBuilder.forAddress("127.0.0.1", grpcPort).usePlaintext()
                    .maxInboundMessageSize(16 * 1024 * 1024).build();
            Metadata operatorHeaders = bearer(OPERATOR_TOKEN);
            var authoring = WorkflowAuthoringServiceGrpc.newBlockingStub(channel)
                    .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(operatorHeaders));
            WorkflowAcceptedCandidate accepted = authoring.withDeadlineAfter(10, TimeUnit.SECONDS)
                    .getAcceptedWorkflow(GetAcceptedWorkflowRequest.newBuilder().setTaskId(TASK_ID).build());
            assertThat(accepted.getTaskId()).isEqualTo(TASK_ID);

            var prepared = new FileSystemWorkflowPreparationRepository(preparations)
                    .find(TASK_ID, 1, 1).orElseThrow();
            assertThat(prepared.hasCompleted()).isTrue();
            WorkflowPreparationIntent intent = prepared.getIntent();
            assertThat(intent.getBinding().getPreparationId()).isNotEqualTo(POLICY_OPERATION)
                    .isNotEqualTo(JOB_OPERATION).isNotEqualTo(LAUNCH_ID);
            var receiptRef = prepared.getCompleted().getAuthored().getDeliverable().getReceipt();
            byte[] receipt = artifacts.find(receiptRef.getSha256()).orElseThrow().content();
            assertThat(RecordVerifier.verify(receipt, trust).verified()).isTrue();

            WriteRecordRequest jobInput = WriteRecordRequest.newBuilder().setOperationId(JOB_OPERATION)
                    .setContent(" \tprocess-input\r\n value\t ").build();
            var inputService = ai.protomolt.proto.workflow.authoring.v1.WorkflowLaunchInputServiceGrpc
                    .newBlockingStub(channel)
                    .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(operatorHeaders))
                    .withDeadlineAfter(10, TimeUnit.SECONDS);
            var inputContract = inputService.getWorkflowLaunchInputContract(
                    ai.protomolt.proto.workflow.authoring.v1.GetWorkflowLaunchInputContractRequest
                            .newBuilder().setAcceptance(accepted).build());
            assertThat(inputContract.getAcceptance()).isEqualTo(accepted);
            assertThat(inputContract.getInputType()).isEqualTo(WriteRecordRequest.getDescriptor().getFullName());
            assertThat(inputContract.getDescriptors().getSha256())
                    .isEqualTo(sha256(inputContract.getDescriptorSet().toByteArray()));
            var inputRequest = ai.protomolt.proto.workflow.authoring.v1.PrepareWorkflowLaunchInputRequest
                    .newBuilder().setAcceptance(accepted)
                    .setInputJson(ByteString.copyFromUtf8(JsonFormat.printer().print(jobInput))).build();
            var preparedInput = inputService.prepareWorkflowLaunchInput(inputRequest);
            assertThat(preparedInput.getAcceptance()).isEqualTo(accepted);
            assertThat(inputService.prepareWorkflowLaunchInput(inputRequest)).isEqualTo(preparedInput);
            ArtifactReference input = preparedInput.getInput();
            assertThat(input.getSha256()).isEqualTo(sha256(jobInput.toByteArray()));
            WorkflowAuthoringLaunchRequest launchRequest = WorkflowAuthoringLaunchRequest.newBuilder()
                    .setLaunchId(LAUNCH_ID).setAcceptance(accepted).setInput(input).build();
            String consoleCookie = loginBrowser(httpPort, CONSOLE_TOKEN);
            assertThat(browserCall(httpPort, "launch", launchRequest, consoleCookie).statusCode()).isEqualTo(403);
            String launchCookie = loginBrowser(httpPort, BROWSER_TOKEN);
            var browserContract = browserCall(httpPort, "contract",
                    ai.protomolt.proto.workflow.authoring.v1.GetWorkflowLaunchInputContractRequest
                            .newBuilder().setAcceptance(accepted).build(), launchCookie);
            assertThat(browserContract.statusCode()).isEqualTo(200);
            var contractBuilder = inputContract.toBuilder().clear();
            JsonFormat.parser().merge(browserContract.body(), contractBuilder);
            assertThat(contractBuilder.build()).isEqualTo(inputContract);
            var browserInput = browserCall(httpPort, "prepare", inputRequest, launchCookie);
            assertThat(browserInput.statusCode()).isEqualTo(200);
            var inputBuilder = preparedInput.toBuilder().clear();
            JsonFormat.parser().merge(browserInput.body(), inputBuilder);
            assertThat(inputBuilder.build()).isEqualTo(preparedInput);
            var browserLaunch = browserCall(httpPort, "launch", launchRequest, launchCookie);
            assertThat(browserLaunch.statusCode()).isEqualTo(200);
            var launched = authoring.withDeadlineAfter(30, TimeUnit.SECONDS)
                    .launchAcceptedWorkflow(launchRequest);
            assertThat(launched.getJobId()).isEqualTo(LAUNCH_ID);
            var launchBuilder = launched.toBuilder().clear();
            JsonFormat.parser().merge(browserLaunch.body(), launchBuilder);
            assertThat(launchBuilder.build()).isEqualTo(launched);
            assertThat(authoring.withDeadlineAfter(30, TimeUnit.SECONDS)
                    .launchAcceptedWorkflow(launchRequest)).isEqualTo(launched);

            JsonNode job = awaitCompletedJob(mcp, LAUNCH_ID, Duration.ofSeconds(90));
            assertThat(job.path("status").asText()).isEqualTo("COMPLETED");
            assertThat(job.path("result").path("operationId").asText()).isEqualTo(JOB_OPERATION);
            assertThat(job.path("result").path("contentSha256").asText())
                    .isEqualTo(sha256("process-input\n value".getBytes(StandardCharsets.UTF_8)));

            FileSystemFixtureRecordRepository records = new FileSystemFixtureRecordRepository(fixtureRecords);
            FixtureStoredRecord fixtureRecord = records.find(JOB_OPERATION).orElseThrow();
            assertThat(fixtureRecord.getRequest().getOperationId()).isEqualTo(JOB_OPERATION);
            assertThat(fixtureRecord.getRequest().getContent()).isEqualTo("process-input\n value");
            assertThat(fixtureRecord.getResponse().getContentSha256())
                    .isEqualTo(sha256(fixtureRecord.getRequest().getContentBytes().toByteArray()));
            FixtureStoredRecord policyRecord = records.find(POLICY_OPERATION).orElseThrow();
            assertThat(policyRecord.getRequest().getOperationId()).isEqualTo(POLICY_OPERATION);
            assertThat(POLICY_OPERATION).isNotEqualTo(JOB_OPERATION).isNotEqualTo(LAUNCH_ID);
        } finally {
            if (channel != null) channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            stop(worker);
            stop(coordinator);
            stop(fixture);
            if (repository != null) repository.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            postgres.stop();
        }
    }

    private static Process startFixture(Path records, Path log) throws Exception {
        Path launcher = Path.of("build/install/authoring-fixture/bin/authoring-fixture").toAbsolutePath();
        assertThat(launcher).exists();
        ProcessBuilder builder = new ProcessBuilder(launcher.toString(), "0", records.toString())
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

    private static TaskSpec taskSpec(ArtifactReference policy) throws Exception {
        FileDescriptorSet.Builder descriptorSet = FileDescriptorSet.newBuilder();
        Map<String, FileDescriptor> files = new LinkedHashMap<>();
        collect(WorkflowAuthoringDeliverable.getDescriptor().getFile(), files);
        files.values().forEach(file -> descriptorSet.addFile(file.toProto()));
        var spec = TaskSpec.newBuilder().setObjective("Author and independently verify the pinned normalize-record workflow")
                .addContext(policy)
                .setContract(DeliverableContract.newBuilder()
                        .setTypeName(WorkflowAuthoringDeliverable.getDescriptor().getFullName())
                        .setDescriptorSet(ByteString.copyFrom(descriptorSet.build().toByteArray())));
        WorkflowAuthoringReviewer.REQUIRED_CHECKS.forEach(check -> spec.addRequiredChecks(
                AcceptanceCheck.newBuilder().setName(check).setDescription("Run required check: " + check)));
        return spec.build();
    }

    private static void collect(FileDescriptor file, Map<String, FileDescriptor> files) {
        if (files.containsKey(file.getName())) return;
        file.getDependencies().forEach(dependency -> collect(dependency, files));
        files.put(file.getName(), file);
    }

    private JsonNode offerTask(McpSession session, ArtifactReference policy) throws Exception {
        OfferTaskRequest offer = OfferTaskRequest.newBuilder().setTaskId(TASK_ID)
                .setWorkerId(PRINCIPAL).setLeaseSeconds(240).setSpec(taskSpec(policy)).build();
        JsonNode args = JSON.readTree(JsonFormat.printer().omittingInsignificantWhitespace().print(offer));
        return session.call("delegation-offer", args);
    }

    private static JsonNode awaitCompletedJob(McpSession session, String jobId,
            Duration timeout) throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        JsonNode last = null;
        while (System.nanoTime() < deadline) {
            last = session.call("get-job", JSON.createObjectNode().put("jobId", jobId))
                    .path("job");
            if (last.path("status").asText().equals("COMPLETED")) return last;
            if (last.path("status").asText().equals("FAILED") || last.path("status").asText().equals("DEAD")) {
                throw new AssertionError("workflow job failed: " + last);
            }
            Thread.sleep(150);
        }
        throw new AssertionError("workflow job did not complete; last row: " + last);
    }

    private static McpSession initializeMcp(int port, String token) throws Exception {
        McpSession session = new McpSession(port, token);
        session = session.initialize();
        assertThat(session.tools()).contains("delegation-offer", "get-job");
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

    private static String loginBrowser(int port, String token) throws Exception {
        var response = HTTP.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/api/task-session"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(JSON.writeValueAsString(Map.of("token", token))))
                .build(), HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(200);
        return response.headers().firstValue("Set-Cookie").orElseThrow().split(";", 2)[0];
    }

    private static HttpResponse<String> browserCall(int port, String operation,
            com.google.protobuf.Message request, String cookie) throws Exception {
        String origin = "http://127.0.0.1:" + port;
        return HTTP.send(HttpRequest.newBuilder(URI.create(origin + "/api/workflow-launch/" + operation))
                .timeout(Duration.ofSeconds(30)).header("Origin", origin).header("Cookie", cookie)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(JsonFormat.printer().print(request))).build(),
                HttpResponse.BodyHandlers.ofString());
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
