package ai.protomolt.proto.serve;

import ai.protomolt.proto.grpc.workflow.FileSystemArtifactRepository;
import ai.protomolt.proto.grpc.workflow.v1.ArtifactReference;
import ai.protomolt.proto.receipt.*;
import ai.protomolt.proto.repo.v1.DocumentServiceGrpc;
import ai.protomolt.proto.repo.v1.ConditionalBlobVersion;
import ai.protomolt.proto.repo.v1.GetBlobForUpdateRequest;
import ai.protomolt.proto.repo.v1.GetBlobForUpdateResponse;
import ai.protomolt.proto.repo.v1.CompareAndPutBlobRequest;
import ai.protomolt.proto.repo.v1.CompareAndPutBlobResponse;
import ai.protomolt.proto.repo.v1.FileStorageReference;
import ai.protomolt.proto.repo.v1.GetBlobRequest;
import ai.protomolt.proto.repo.v1.GetBlobResponse;
import ai.protomolt.proto.repo.v1.PutBlobRequest;
import ai.protomolt.proto.repo.v1.PutBlobResponse;
import ai.protomolt.proto.samples.starter.v1.WorkflowAcceptanceFixture;
import ai.protomolt.proto.samples.starter.v1.WorkflowAuthoringPolicy;
import ai.protomolt.proto.samples.starter.v1.WorkflowPermittedCall;
import ai.protomolt.proto.workflow.authoring.v1.GetAcceptedWorkflowRequest;
import ai.protomolt.proto.workflow.authoring.v1.WorkflowAuthoringServiceGrpc;
import ai.protomolt.proto.workflow.authoring.v1.WorkflowPreparationServiceGrpc;
import ai.protomolt.proto.workflow.authoring.v1.PrepareWorkflowCandidateRequest;
import ai.protomolt.proto.samples.starter.v1.WorkflowAuthoringLaunchRequest;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
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
import java.security.MessageDigest;
import java.time.Duration;
import java.util.Base64;
import java.util.HexFormat;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Exercises the opt-in authoring mount in the installed serve process and on each host surface. */
@Tag("integration")
@Testcontainers(disabledWithoutDocker = true)
class WorkflowAuthoringServeIntegrationTest {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String TOKEN = "mount-integration-operator-token";
    private static final String TASK_ID = "00000000-0000-4000-8000-000000000001";
    private static final byte[] TRANSCRIPT_KEY = "0123456789abcdef0123456789abcdef"
            .getBytes(StandardCharsets.US_ASCII);
    private static final HttpClient HTTP = HttpClient.newHttpClient();

    @TempDir Path directory;

    @Test
    void enabledProcessMountsAuthenticatedGrpcMcpAndRestThenOptOutRemovesIt() throws Exception {
        var postgres = new GenericContainer<>(DockerImageName.parse("postgres:16-alpine"))
                .withEnv("POSTGRES_DB", "protomolt")
                .withEnv("POSTGRES_USER", "protomolt")
                .withEnv("POSTGRES_PASSWORD", "test-password")
                .withExposedPorts(5432)
                .withStartupAttempts(3);
        Server repository = null;
        Process enabled = null;
        Process disabled = null;
        ManagedChannel channel = null;
        try {
            postgres.start();
            FakeDocumentService documents = new FakeDocumentService();
            repository = ServerBuilder.forPort(0).addService(documents).build().start();
            Path workflowWorkspace = directory.resolve("workflows");
            Path registry = directory.resolve("registry.git");
            Path authorizations = directory.resolve("authorizations");
            Path trustFile = directory.resolve("trust.binpb");
            Files.write(trustFile, trust().toByteArray());
            ArtifactReference policy = writePolicy(workflowWorkspace);

            int grpcPort = freePort();
            int httpPort = freePort();
            enabled = startInstalledServer(true, grpcPort, httpPort, registry,
                    workflowWorkspace, authorizations, policy.getSha256(), trustFile,
                    "127.0.0.1:" + repository.getPort(), postgres.getHost(),
                    postgres.getMappedPort(5432), directory.resolve("enabled.log"));
            awaitHealth(enabled, httpPort, directory.resolve("enabled.log"));

            // The browser route uses scoped sessions, never the API operator token.
            String acceptedJson = "{\"taskId\":\"" + TASK_ID + "\"}";
            assertThat(browserPost(httpPort, acceptedJson, null).statusCode()).isEqualTo(401);
            String defaultSession = loginSession(httpPort, "console-test-token-for-installed-browser-test");
            assertThat(browserPost(httpPort, acceptedJson, defaultSession).statusCode()).isEqualTo(403);
            String launchSession = loginSession(httpPort, "browser-launch-test-token");
            var noAcceptance = browserPost(httpPort, acceptedJson, launchSession);
            assertThat(noAcceptance.statusCode()).isEqualTo(412);
            assertThat(noAcceptance.body()).contains("workflow-authoring-rejected");

            channel = ManagedChannelBuilder.forAddress("127.0.0.1", grpcPort)
                    .usePlaintext().build();
            var raw = WorkflowAuthoringServiceGrpc.newBlockingStub(channel);
            assertThatThrownBy(() -> raw.withDeadlineAfter(5, TimeUnit.SECONDS).getAcceptedWorkflow(
                    GetAcceptedWorkflowRequest.newBuilder().setTaskId(TASK_ID).build()))
                    .isInstanceOf(StatusRuntimeException.class)
                    .extracting(error -> ((StatusRuntimeException) error).getStatus().getCode())
                    .isEqualTo(Status.Code.UNAUTHENTICATED);

            Metadata credentials = new Metadata();
            credentials.put(Metadata.Key.of("api_token", Metadata.ASCII_STRING_MARSHALLER), TOKEN);
            var operator = raw.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(credentials));
            assertThatThrownBy(() -> operator.withDeadlineAfter(5, TimeUnit.SECONDS).getAcceptedWorkflow(
                    GetAcceptedWorkflowRequest.newBuilder().setTaskId(TASK_ID).build()))
                    .isInstanceOf(StatusRuntimeException.class)
                    .extracting(error -> ((StatusRuntimeException) error).getStatus().getCode())
                    .isEqualTo(Status.Code.FAILED_PRECONDITION);
            assertThatThrownBy(() -> operator.withDeadlineAfter(5, TimeUnit.SECONDS).launchAcceptedWorkflow(
                    WorkflowAuthoringLaunchRequest.getDefaultInstance()))
                    .isInstanceOf(StatusRuntimeException.class)
                    .extracting(error -> ((StatusRuntimeException) error).getStatus().getCode())
                    .isEqualTo(Status.Code.INVALID_ARGUMENT);

            var openApi = get(httpPort, "/openapi.json", null);
            assertThat(openApi.statusCode()).isEqualTo(200);
            JsonNode paths = JSON.readTree(openApi.body()).path("paths");
            assertThat(paths.has("/grpc-json/WorkflowAuthoringService/GetAcceptedWorkflow")).isTrue();
            assertThat(paths.has("/grpc-json/WorkflowAuthoringService/LaunchAcceptedWorkflow")).isTrue();
            assertThat(paths.has("/grpc-json/WorkflowLaunchInputService/GetWorkflowLaunchInputContract")).isTrue();
            assertThat(paths.has("/grpc-json/WorkflowLaunchInputService/PrepareWorkflowLaunchInput")).isTrue();
            var inputStub = ai.protomolt.proto.workflow.authoring.v1.WorkflowLaunchInputServiceGrpc
                    .newBlockingStub(channel).withInterceptors(MetadataUtils.newAttachHeadersInterceptor(credentials));
            var selector = ai.protomolt.proto.samples.starter.v1.WorkflowAcceptedCandidate.newBuilder()
                    .setTaskId(TASK_ID).setAttempt(1).setRevision(1)
                    .setTaskSpecSha256("a".repeat(64)).setCandidateSha256("b".repeat(64))
                    .setAcceptedEntrySha256("c".repeat(64)).build();
            assertThatThrownBy(() -> inputStub.withDeadlineAfter(5, TimeUnit.SECONDS)
                    .prepareWorkflowLaunchInput(ai.protomolt.proto.workflow.authoring.v1.PrepareWorkflowLaunchInputRequest
                            .newBuilder().setAcceptance(selector)
                            .setInputJson(com.google.protobuf.ByteString.copyFrom(new byte[4 * 1024 * 1024])).build()))
                    .isInstanceOf(StatusRuntimeException.class)
                    .extracting(error -> ((StatusRuntimeException) error).getStatus().getCode())
                    .isEqualTo(Status.Code.FAILED_PRECONDITION);
            assertThat(paths.has(
                    "/grpc-json/WorkflowPreparationService/PrepareWorkflowCandidate")).isFalse();
            var preparationStub = WorkflowPreparationServiceGrpc.newBlockingStub(channel);
            assertThatThrownBy(() -> preparationStub
                    .withDeadlineAfter(5, TimeUnit.SECONDS)
                    .prepareWorkflowCandidate(PrepareWorkflowCandidateRequest.getDefaultInstance()))
                    .isInstanceOf(StatusRuntimeException.class)
                    .extracting(error -> ((StatusRuntimeException) error).getStatus().getCode())
                    .isEqualTo(Status.Code.UNIMPLEMENTED);
            var rest = post(httpPort, "/grpc-json/WorkflowAuthoringService/GetAcceptedWorkflow",
                    "{\"taskId\":\"not-a-uuid\"}", TOKEN);
            assertThat(rest.statusCode()).isEqualTo(400);
            assertThat(rest.body()).contains("invalid-input");

            JsonNode tools = listMcpTools(httpPort, TOKEN);
            assertThat(tools.findValuesAsText("name")).contains(
                    "get-accepted-workflow", "launch-accepted-workflow",
                    "get-workflow-launch-input-contract", "prepare-workflow-launch-input");

            if (channel != null) {
                channel.shutdownNow();
                channel.awaitTermination(5, TimeUnit.SECONDS);
                channel = null;
            }
            stop(enabled);
            enabled = null;

            // With no authoring options, none of the additional host descriptors are published.
            int disabledGrpc = freePort();
            int disabledHttp = freePort();
            disabled = startInstalledServer(false, disabledGrpc, disabledHttp, null,
                    null, null, null, trustFile, null, null, 0,
                    directory.resolve("disabled.log"));
            awaitHealth(disabled, disabledHttp, directory.resolve("disabled.log"));
            assertThat(browserPost(disabledHttp, "{}", null).statusCode()).isEqualTo(404);
            JsonNode disabledPaths = JSON.readTree(get(disabledHttp, "/openapi.json", null).body())
                    .path("paths");
            assertThat(disabledPaths.has(
                    "/grpc-json/WorkflowAuthoringService/GetAcceptedWorkflow")).isFalse();
            assertThat(disabledPaths.has(
                    "/grpc-json/WorkflowLaunchInputService/GetWorkflowLaunchInputContract")).isFalse();
            assertThat(disabledPaths.has(
                    "/grpc-json/WorkflowPreparationService/PrepareWorkflowCandidate")).isFalse();
            assertThat(listMcpTools(disabledHttp, null).findValuesAsText("name"))
                    .doesNotContain("get-accepted-workflow", "launch-accepted-workflow",
                            "get-workflow-launch-input-contract", "prepare-workflow-launch-input");
            ManagedChannel disabledChannel = ManagedChannelBuilder
                    .forAddress("127.0.0.1", disabledGrpc).usePlaintext().build();
            try {
                assertThatThrownBy(() -> WorkflowAuthoringServiceGrpc.newBlockingStub(disabledChannel)
                        .withDeadlineAfter(5, TimeUnit.SECONDS)
                        .getAcceptedWorkflow(GetAcceptedWorkflowRequest.newBuilder()
                                .setTaskId(TASK_ID).build()))
                        .isInstanceOf(StatusRuntimeException.class)
                        .extracting(error -> ((StatusRuntimeException) error).getStatus().getCode())
                        .isEqualTo(Status.Code.UNIMPLEMENTED);
            } finally {
                disabledChannel.shutdownNow();
                disabledChannel.awaitTermination(5, TimeUnit.SECONDS);
            }
        } finally {
            if (channel != null) {
                channel.shutdownNow();
                channel.awaitTermination(5, TimeUnit.SECONDS);
            }
            stop(enabled);
            stop(disabled);
            if (repository != null) repository.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            postgres.stop();
        }
    }

    private Process startInstalledServer(boolean authoring, int grpcPort, int httpPort,
            Path registry, Path workflowWorkspace, Path authorizationDirectory,
            String policySha, Path trustFile, String repositoryEndpoint,
            String postgresHost, int postgresPort, Path log) throws Exception {
        Path executable = Path.of("build/install/protomolt-serve/bin/protomolt-serve")
                .toAbsolutePath();
        assertThat(executable).exists();
        var command = new java.util.ArrayList<String>();
        command.add(executable.toString());
        command.addAll(java.util.List.of("--host", "127.0.0.1", "--grpc-port",
                Integer.toString(grpcPort), "--http-port", Integer.toString(httpPort),
                "--registry-port", "0"));
        if (authoring) {
            Path accessPolicy = directory.resolve("browser-access.binpb");
            Files.write(accessPolicy, ai.protomolt.proto.authz.AccessPolicy.newBuilder()
                    .addPrincipals(ai.protomolt.proto.authz.Principal.newBuilder()
                            .setName("browser-launcher")
                            .addCredentialSha256(ai.protomolt.proto.authz.AccessPolicyCallers
                                    .sha256Hex("browser-launch-test-token"))
                            .addScopes("worker-coordinate").addScopes("workflow-launch"))
                    .build().toByteArray());
            command.addAll(java.util.List.of("--access-policy", accessPolicy.toString()));
            command.addAll(java.util.List.of("--registry-git", registry.toString(),
                    "--workflow-workspace", workflowWorkspace.toString(),
                    "--delegation-repo-endpoint", repositoryEndpoint,
                    "--delegation-state-key-ref", "env:PROTOMOLT_TRANSCRIPT_KEY",
                    "--jobs-jdbc", "jdbc:postgresql://" + postgresHost + ":" + postgresPort
                            + "/protomolt",
                    "--jobs-user", "protomolt", "--jobs-password", "test-password",
                    "--api-token", TOKEN,
                    "--workflow-authoring-policy-sha256", policySha,
                    "--workflow-authoring-authorization-dir", authorizationDirectory.toString()));
        }
        ProcessBuilder builder = new ProcessBuilder(command).redirectErrorStream(true)
                .redirectOutput(log.toFile());
        Map<String, String> env = builder.environment();
        env.keySet().removeIf(name -> name.startsWith("PROTOMOLT_"));
        if (authoring) env.put("PROTOMOLT_TASK_CONSOLE_TOKEN", "console-test-token-for-installed-browser-test");
        env.put("PROTOMOLT_TRUST_SNAPSHOT", trustFile.toString());
        env.put("PROTOMOLT_TRANSCRIPT_KEY", Base64.getEncoder().encodeToString(TRANSCRIPT_KEY));
        return builder.start();
    }

    private static ArtifactReference writePolicy(Path workspace) throws Exception {
        var artifacts = new FileSystemArtifactRepository(workspace.resolve("artifacts"));
        ArtifactReference descriptors = artifacts.save(new byte[]{1},
                "application/x-protobuf", false);
        ArtifactReference input = artifacts.save(new byte[]{2}, "application/x-protobuf", false);
        ArtifactReference output = artifacts.save(new byte[]{3}, "application/x-protobuf", false);
        var policy = WorkflowAuthoringPolicy.newBuilder().setDescriptors(descriptors)
                .addFixtures(WorkflowAcceptanceFixture.newBuilder().setName("sample")
                        .setInput(input).setExpectedOutput(output))
                .addPermittedCalls(WorkflowPermittedCall.newBuilder()
                        .setTarget("fixture:9090").setMethod("fixture.v1.Echo/Run"))
                .build();
        return artifacts.save(policy.toByteArray(), "application/x-protobuf", false);
    }

    private static TrustSnapshot trust() {
        var keys = RecordKeys.generate();
        return TrustSnapshot.newBuilder().addIssuers(TrustedIssuer.newBuilder()
                .setIssuer("mount-integration").addSubjectKinds(WorkRecords.SUBJECT_KIND_WORKFLOW_RUN)
                .addKeys(TrustedKey.newBuilder().setKeyId("mount-key")
                        .setAlgorithm(SignatureAlgorithm.SIGNATURE_ALGORITHM_ED25519)
                        .setState(KeyState.KEY_STATE_ACTIVE)
                        .setPublicKey(ByteString.copyFrom(RecordKeys.rawPublicKey(keys.getPublic())))))
                .build();
    }

    private static JsonNode listMcpTools(int port, String token) throws Exception {
        HttpResponse<String> init = mcp(port, """
                {"jsonrpc":"2.0","id":1,"method":"initialize","params":{
                  "protocolVersion":"2025-06-18","capabilities":{},
                  "clientInfo":{"name":"authoring-mount-test","version":"1"}}}
                """, token, null, null);
        assertThat(init.statusCode()).isEqualTo(200);
        String session = init.headers().firstValue("Mcp-Session-Id").orElseThrow();
        String version = JSON.readTree(init.body()).path("result").path("protocolVersion").asText();
        assertThat(mcp(port, "{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}",
                token, session, version).statusCode()).isEqualTo(202);
        HttpResponse<String> listed = mcp(port,
                "{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"tools/list\"}",
                token, session, version);
        assertThat(listed.statusCode()).isEqualTo(200);
        return JSON.readTree(listed.body()).path("result").path("tools");
    }

    private static HttpResponse<String> mcp(int port, String body, String token,
            String session, String version) throws Exception {
        HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(
                        "http://127.0.0.1:" + port + "/mcp"))
                .header("content-type", "application/json")
                .header("accept", "application/json, text/event-stream")
                .POST(HttpRequest.BodyPublishers.ofString(body));
        if (token != null) request.header("api_token", token);
        if (session != null) request.header("Mcp-Session-Id", session);
        if (version != null) request.header("MCP-Protocol-Version", version);
        return HTTP.send(request.build(), HttpResponse.BodyHandlers.ofString());
    }

    private static HttpResponse<String> get(int port, String path, String token) throws Exception {
        HttpRequest.Builder request = HttpRequest.newBuilder(
                URI.create("http://127.0.0.1:" + port + path)).GET();
        if (token != null) request.header("api_token", token);
        return HTTP.send(request.build(), HttpResponse.BodyHandlers.ofString());
    }

    private static String loginSession(int port, String token) throws Exception {
        var response = post(port, "/api/task-session", "{\"token\":\"" + token + "\"}", TOKEN);
        assertThat(response.statusCode()).isEqualTo(200);
        return response.headers().firstValue("Set-Cookie").orElseThrow().split(";", 2)[0];
    }

    private static HttpResponse<String> browserPost(int port, String body, String cookie) throws Exception {
        String origin = "http://127.0.0.1:" + port;
        var request = HttpRequest.newBuilder(URI.create(origin + "/api/workflow-launch/accepted"))
                .header("Content-Type", "application/json").header("Origin", origin)
                .POST(HttpRequest.BodyPublishers.ofString(body));
        if (cookie != null) request.header("Cookie", cookie);
        return HTTP.send(request.build(), HttpResponse.BodyHandlers.ofString());
    }

    private static HttpResponse<String> post(int port, String path, String body, String token)
            throws Exception {
        return HTTP.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path))
                        .header("content-type", "application/json").header("api_token", token)
                        .POST(HttpRequest.BodyPublishers.ofString(body)).build(),
                HttpResponse.BodyHandlers.ofString());
    }

    private static int freePort() throws Exception {
        try (ServerSocket socket = new ServerSocket()) {
            socket.bind(new InetSocketAddress("127.0.0.1", 0));
            return socket.getLocalPort();
        }
    }

    private static void awaitHealth(Process process, int port, Path log) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        while (System.nanoTime() < deadline) {
            if (!process.isAlive()) throw new AssertionError("serve exited during startup:\n" + log(log));
            try {
                HttpResponse<String> response = get(port, "/health", null);
                if (response.statusCode() == 200) return;
            } catch (Exception ignored) { }
            Thread.sleep(200);
        }
        throw new AssertionError("serve did not become healthy:\n" + log(log));
    }

    private static String log(Path file) throws Exception {
        return Files.exists(file) ? Files.readString(file) : "<no process output>";
    }

    private static void stop(Process process) throws InterruptedException {
        if (process == null) return;
        process.destroy();
        if (!process.waitFor(5, TimeUnit.SECONDS)) {
            process.destroyForcibly();
            process.waitFor(5, TimeUnit.SECONDS);
        }
    }

    private static final class FakeDocumentService extends DocumentServiceGrpc.DocumentServiceImplBase {
        private final Map<String, ByteString> objects = new ConcurrentHashMap<>();
        private final Map<String, String> etags = new ConcurrentHashMap<>();
        private final Map<String, String> mimeTypes = new ConcurrentHashMap<>();
        private long generation;

        @Override public void getBlob(GetBlobRequest request, StreamObserver<GetBlobResponse> observer) {
            var ref = request.getStorageRef();
            ByteString bytes = objects.get(key(ref.getDriveName(), ref.getObjectKey()));
            if (bytes == null) {
                observer.onError(Status.NOT_FOUND.asRuntimeException());
                return;
            }
            observer.onNext(GetBlobResponse.newBuilder().setData(bytes).setSizeBytes(bytes.size())
                    .setMimeType("application/octet-stream").build());
            observer.onCompleted();
        }

        @Override public void putBlob(PutBlobRequest request, StreamObserver<PutBlobResponse> observer) {
            ByteString bytes = request.getData();
            String objectKey = key(request.getDriveName(), request.getObjectKey());
            objects.put(objectKey, bytes);
            mimeTypes.put(objectKey, request.getMimeType());
            var ref = FileStorageReference.newBuilder().setDriveName(request.getDriveName())
                    .setObjectKey(request.getObjectKey()).build();
            observer.onNext(PutBlobResponse.newBuilder().setStorageRef(ref)
                    .setSizeBytes(bytes.size()).setSha256(sha256(bytes.toByteArray())).build());
            observer.onCompleted();
        }

        @Override public void getBlobForUpdate(GetBlobForUpdateRequest request,
                                               StreamObserver<GetBlobForUpdateResponse> observer) {
            String objectKey = key(request.getKey().getDriveName(), request.getKey().getObjectKey());
            ByteString bytes = objects.get(objectKey);
            if (bytes == null) {
                observer.onError(Status.NOT_FOUND.asRuntimeException());
                return;
            }
            var response = GetBlobForUpdateResponse.newBuilder()
                    .setVersion(ConditionalBlobVersion.newBuilder().setKey(request.getKey())
                            .setEtag(etags.get(objectKey)).setSizeBytes(bytes.size())
                            .setSha256(sha256(bytes.toByteArray())))
                    .setData(bytes);
            String mime = mimeTypes.get(objectKey);
            if (mime != null && !mime.isEmpty()) response.setMimeType(mime);
            observer.onNext(response.build());
            observer.onCompleted();
        }

        @Override public synchronized void compareAndPutBlob(CompareAndPutBlobRequest request,
                                                              StreamObserver<CompareAndPutBlobResponse> observer) {
            String objectKey = key(request.getKey().getDriveName(), request.getKey().getObjectKey());
            ByteString previous = objects.get(objectKey);
            String previousEtag = etags.get(objectKey);
            boolean allowed = request.hasIfAbsent() ? previous == null
                    : previous != null && request.getExpectedEtag().equals(previousEtag);
            if (!allowed) {
                observer.onError(Status.ABORTED.withDescription("conditional write conflict").asRuntimeException());
                return;
            }
            ByteString bytes = request.getData();
            String etag = "\"conditional-" + (++generation) + "\"";
            objects.put(objectKey, bytes);
            etags.put(objectKey, etag);
            if (request.hasMimeType()) mimeTypes.put(objectKey, request.getMimeType());
            else mimeTypes.remove(objectKey);
            observer.onNext(CompareAndPutBlobResponse.newBuilder().setVersion(
                    ConditionalBlobVersion.newBuilder().setKey(request.getKey()).setEtag(etag)
                            .setSizeBytes(bytes.size()).setSha256(sha256(bytes.toByteArray()))).build());
            observer.onCompleted();
        }

        private static String key(String drive, String object) { return drive + "/" + object; }

        private static String sha256(byte[] bytes) {
            try { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes)); }
            catch (java.security.NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); }
        }
    }
}
