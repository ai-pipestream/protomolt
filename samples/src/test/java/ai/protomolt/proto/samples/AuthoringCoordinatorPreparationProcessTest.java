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
import ai.protomolt.proto.samples.starter.v1.WorkflowAcceptanceFixture;
import ai.protomolt.proto.samples.starter.v1.WorkflowAuthoringPolicy;
import ai.protomolt.proto.samples.starter.v1.WorkflowPermittedCall;
import ai.protomolt.proto.workflow.authoring.v1.PrepareWorkflowCandidateRequest;
import ai.protomolt.proto.workflow.authoring.v1.WorkflowPreparationServiceGrpc;
import ai.protomolt.proto.repo.v1.DocumentServiceGrpc;
import ai.protomolt.proto.repo.v1.FileStorageReference;
import ai.protomolt.proto.repo.v1.GetBlobRequest;
import ai.protomolt.proto.repo.v1.GetBlobResponse;
import ai.protomolt.proto.repo.v1.PutBlobRequest;
import ai.protomolt.proto.repo.v1.PutBlobResponse;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
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
import java.security.KeyPair;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Installed coordinator proof for the opt-in, signed, authenticated preparation mount. */
@Tag("integration")
@Testcontainers(disabledWithoutDocker = true)
class AuthoringCoordinatorPreparationProcessTest {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final HttpClient HTTP = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5)).build();
    private static final Metadata.Key<String> ERROR_CODE = Metadata.Key.of(
            "protomolt-error", Metadata.ASCII_STRING_MARSHALLER);
    private static final String OPERATOR_TOKEN = "starter-coordinator-operator";
    private static final String AUTHOR_TOKEN = "starter-workflow-author-token";
    private static final String TASK_ID = "00000000-0000-4000-8000-000000000111";
    private static final String PREPARATION_ID = "00000000-0000-4000-8000-000000000222";

    @TempDir Path directory;

    @Test
    void installedCoordinatorLoadsStarterProviderAndMountsPreparationBehindAuthAndSigning()
            throws Exception {
        var postgres = new PostgreSQLContainer<>(DockerImageName.parse("postgres:16-alpine"))
                .withDatabaseName("protomolt")
                .withUsername("protomolt")
                .withPassword("test-password");
        Process process = null;
        ManagedChannel channel = null;
        io.grpc.Server repository = null;
        try {
            postgres.start();
            repository = ServerBuilder.forPort(0).addService(new FakeDocumentService()).build().start();
            Path workspace = directory.resolve("workflows");
            Path registry = directory.resolve("registry.git");
            Path authorization = directory.resolve("authorizations");
            Path preparations = directory.resolve("preparations");
            Path trustFile = directory.resolve("trust.binpb");
            Path signingKey = directory.resolve("signing.seed");
            Path accessPolicy = directory.resolve("access-policy.json");
            String policySha = writePolicy(workspace);
            writeSigningAndTrust(signingKey, trustFile);
            Files.writeString(accessPolicy, accessPolicyJson());

            int grpcPort = freePort();
            int httpPort = freePort();
            Path log = directory.resolve("coordinator.log");
            process = startInstalledCoordinator(grpcPort, httpPort, postgres, registry, workspace,
                    authorization, preparations, policySha, trustFile, signingKey, accessPolicy,
                    "127.0.0.1:" + repository.getPort(), log);
            awaitHealth(process, httpPort, log);

            JsonNode paths = JSON.readTree(get(httpPort, "/openapi.json", null).body()).path("paths");
            assertThat(paths.has(
                    "/grpc-json/WorkflowPreparationService/PrepareWorkflowCandidate")).isTrue();
            JsonNode tools = listMcpTools(httpPort, OPERATOR_TOKEN);
            assertThat(tools.findValuesAsText("name"))
                    .contains("prepare-workflow-candidate");

            channel = ManagedChannelBuilder.forAddress("127.0.0.1", grpcPort)
                    .usePlaintext().build();
            var stub = WorkflowPreparationServiceGrpc.newBlockingStub(channel);
            PrepareWorkflowCandidateRequest request = PrepareWorkflowCandidateRequest.newBuilder()
                    .setTaskId(TASK_ID).setAttempt(1).setRevision(1)
                    .setPreparationId(PREPARATION_ID)
                    // Structurally valid bytes; this test stops at the absent transcript boundary.
                    .setExecutableSourceJson(ByteString.copyFrom("{}", StandardCharsets.UTF_8))
                    .build();
            assertThatThrownBy(() -> stub.withDeadlineAfter(5, TimeUnit.SECONDS)
                    .prepareWorkflowCandidate(request))
                    .isInstanceOf(StatusRuntimeException.class)
                    .extracting(error -> ((StatusRuntimeException) error).getStatus().getCode())
                    .isEqualTo(Status.Code.UNAUTHENTICATED);

            var workflowAuthor = stub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(
                    bearer(AUTHOR_TOKEN)));
            assertThatThrownBy(() -> workflowAuthor.withDeadlineAfter(5, TimeUnit.SECONDS)
                    .prepareWorkflowCandidate(request))
                    .isInstanceOfSatisfying(StatusRuntimeException.class, error -> {
                        assertThat(error.getStatus().getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);
                        assertThat(error.getTrailers().get(ERROR_CODE))
                                .isEqualTo("workflow-authoring-rejected");
                        assertThat(error.getStatus().getDescription())
                                .contains("workflow-authoring-rejected");
                    });

            var wrongScope = stub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(
                    bearer("worker-only-token")));
            assertThatThrownBy(() -> wrongScope.withDeadlineAfter(5, TimeUnit.SECONDS)
                    .prepareWorkflowCandidate(request))
                    .isInstanceOf(StatusRuntimeException.class)
                    .extracting(error -> ((StatusRuntimeException) error).getStatus().getCode())
                    .isEqualTo(Status.Code.PERMISSION_DENIED);

            // The process reached the handler with workflow-author credentials and failed only
            // because this startup-focused test did not create a delegation transcript.
            assertThat(Files.isDirectory(preparations)).isTrue();
        } finally {
            if (channel != null) channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            stop(process);
            if (repository != null) repository.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            postgres.stop();
        }
    }

    private Process startInstalledCoordinator(int grpcPort, int httpPort,
            PostgreSQLContainer<?> postgres, Path registry, Path workspace, Path authorization,
            Path preparations, String policySha, Path trustFile, Path signingKey,
            Path accessPolicy, String repositoryEndpoint, Path log) throws Exception {
        Path launcher = Path.of("build/install/authoring-coordinator/bin/authoring-coordinator")
                .toAbsolutePath();
        assertThat(launcher).exists();
        var command = new java.util.ArrayList<String>();
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
                "--workflow-preparation-template-provider", "normalize-record-v1"));
        ProcessBuilder builder = new ProcessBuilder(command).redirectErrorStream(true)
                .redirectOutput(log.toFile());
        Map<String, String> env = builder.environment();
        env.keySet().removeIf(name -> name.startsWith("PROTOMOLT_"));
        env.put("PROTOMOLT_TRUST_SNAPSHOT", trustFile.toString());
        env.put("PROTOMOLT_RECEIPT_KEY_FILE", signingKey.toString());
        env.put("PROTOMOLT_RECEIPT_KEY_ID", "starter-signing-key");
        env.put("PROTOMOLT_RECEIPT_ISSUER", "starter-coordinator");
        env.put("PROTOMOLT_TRANSCRIPT_KEY", "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=");
        return builder.start();
    }

    private String writePolicy(Path workspace) throws Exception {
        var artifacts = new FileSystemArtifactRepository(workspace.resolve("artifacts"));
        ArtifactReference descriptors = artifacts.save(new byte[]{1}, "application/x-protobuf", false);
        ArtifactReference input = artifacts.save(new byte[]{2}, "application/x-protobuf", false);
        ArtifactReference output = artifacts.save(new byte[]{3}, "application/x-protobuf", false);
        WorkflowAuthoringPolicy policy = WorkflowAuthoringPolicy.newBuilder()
                .setDescriptors(descriptors)
                .addFixtures(WorkflowAcceptanceFixture.newBuilder().setName("starter-smoke")
                        .setInput(input).setExpectedOutput(output))
                .addPermittedCalls(WorkflowPermittedCall.newBuilder()
                        .setTarget("fixture:9090").setMethod("fixture.v1.Echo/Run"))
                .build();
        ArtifactReference reference = artifacts.save(policy.toByteArray(),
                "application/x-protobuf", false);
        return reference.getSha256();
    }

    private static void writeSigningAndTrust(Path keyFile, Path trustFile) throws Exception {
        KeyPair pair = RecordKeys.generate();
        byte[] encoded = pair.getPrivate().getEncoded();
        byte[] seed = java.util.Arrays.copyOfRange(encoded, encoded.length - 32, encoded.length);
        Files.write(keyFile, seed);
        TrustSnapshot trust = TrustSnapshot.newBuilder().addIssuers(TrustedIssuer.newBuilder()
                .setIssuer("starter-coordinator")
                .addSubjectKinds(WorkRecords.SUBJECT_KIND_WORKFLOW_RUN)
                .addKeys(TrustedKey.newBuilder().setKeyId("starter-signing-key")
                        .setAlgorithm(SignatureAlgorithm.SIGNATURE_ALGORITHM_ED25519)
                        .setState(KeyState.KEY_STATE_ACTIVE)
                        .setPublicKey(ByteString.copyFrom(RecordKeys.rawPublicKey(pair.getPublic())))))
                .build();
        Files.write(trustFile, trust.toByteArray());
    }

    private static String accessPolicyJson() throws Exception {
        return """
                {"principals":[
                  {"name":"starter-author","credentialSha256":["%s"],"scopes":["workflow-author"]},
                  {"name":"worker-only","credentialSha256":["%s"],"scopes":["worker-coordinate"]}
                ]}
                """.formatted(sha256(AUTHOR_TOKEN), sha256("worker-only-token"));
    }

    private static Metadata bearer(String token) {
        Metadata headers = new Metadata();
        headers.put(Metadata.Key.of("api_token", Metadata.ASCII_STRING_MARSHALLER), token);
        return headers;
    }

    private static JsonNode listMcpTools(int port, String token) throws Exception {
        var initialized = post(port, "/mcp", """
                {"jsonrpc":"2.0","id":1,"method":"initialize","params":{
                  "protocolVersion":"2025-06-18","capabilities":{},
                  "clientInfo":{"name":"preparation-process-test","version":"1"}}}
                """, token, null, null);
        assertThat(initialized.statusCode()).isEqualTo(200);
        String session = initialized.headers().firstValue("Mcp-Session-Id").orElseThrow();
        String version = JSON.readTree(initialized.body()).path("result")
                .path("protocolVersion").asText();
        assertThat(post(port, "/mcp", "{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}",
                token, session, version).statusCode()).isEqualTo(202);
        var response = post(port, "/mcp", "{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"tools/list\"}",
                token, session, version);
        assertThat(response.statusCode()).isEqualTo(200);
        return JSON.readTree(response.body()).path("result").path("tools");
    }

    private static HttpResponse<String> post(int port, String path, String body, String token,
            String session, String version) throws Exception {
        HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(
                        "http://127.0.0.1:" + port + path))
                .header("content-type", "application/json")
                .header("accept", "application/json, text/event-stream")
                .timeout(Duration.ofSeconds(5))
                .POST(HttpRequest.BodyPublishers.ofString(body));
        if (token != null) request.header("api_token", token);
        if (session != null) request.header("Mcp-Session-Id", session);
        if (version != null) request.header("MCP-Protocol-Version", version);
        return HTTP.send(request.build(), HttpResponse.BodyHandlers.ofString());
    }

    private static HttpResponse<String> get(int port, String path, String token) throws Exception {
        HttpRequest.Builder request = HttpRequest.newBuilder(
                URI.create("http://127.0.0.1:" + port + path))
                .timeout(Duration.ofSeconds(5)).GET();
        if (token != null) request.header("api_token", token);
        return HTTP.send(request.build(), HttpResponse.BodyHandlers.ofString());
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
            if (!process.isAlive()) throw new AssertionError("coordinator exited during startup:\n"
                    + Files.readString(log));
            try {
                if (get(port, "/health", null).statusCode() == 200) return;
            } catch (Exception ignored) { }
            Thread.sleep(200);
        }
        throw new AssertionError("coordinator did not become healthy:\n" + Files.readString(log));
    }

    private static String sha256(String text) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                .digest(text.getBytes(StandardCharsets.UTF_8)));
    }

    private static String sha256(byte[] bytes) {
        try { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes)); }
        catch (java.security.NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); }
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
        private final Map<String, ByteString> objects = new java.util.concurrent.ConcurrentHashMap<>();

        @Override public void getBlob(GetBlobRequest request, StreamObserver<GetBlobResponse> observer) {
            ByteString bytes = objects.get(key(request.getStorageRef().getDriveName(),
                    request.getStorageRef().getObjectKey()));
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
            objects.put(key(request.getDriveName(), request.getObjectKey()), bytes);
            observer.onNext(PutBlobResponse.newBuilder().setStorageRef(
                    FileStorageReference.newBuilder().setDriveName(request.getDriveName())
                            .setObjectKey(request.getObjectKey()))
                    .setSizeBytes(bytes.size()).setSha256(sha256(bytes.toByteArray())).build());
            observer.onCompleted();
        }

        private static String key(String drive, String object) { return drive + "/" + object; }
    }
}
