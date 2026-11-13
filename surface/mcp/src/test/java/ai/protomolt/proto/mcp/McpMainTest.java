package ai.protomolt.proto.mcp;

import static org.assertj.core.api.Assertions.assertThat;

import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.grpc.workspace.v1.ApprovedTlsEndpoint;
import ai.protomolt.proto.grpc.workspace.v1.ClientCertificateMount;
import ai.protomolt.proto.grpc.workspace.v1.ProfileTlsConfig;
import ai.protomolt.proto.grpc.workspace.v1.TrustBundleMount;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.util.JsonFormat;
import io.grpc.Grpc;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.TlsServerCredentials;
import io.grpc.protobuf.services.HealthStatusManager;
import io.grpc.protobuf.services.ProtoReflectionServiceV1;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.net.ssl.SSLSession;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Verifies {@link McpMain} CLI flags, standalone tool inventory, and end-to-end stdio mutual TLS
 * wiring via {@code --profile-tls-config} and {@code PROTOMOLT_PROFILE_TLS_CONFIG}.
 */
class McpMainTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final List<String> STANDALONE_TOOLS = List.of(
            "compile", "validate-message", "diff-schemas", "check-compat", "render-json-schema",
            "render-prompt", "render-index-mappings", "eval-cel", "map-message",
            "synthesize-shape", "join-messages", "merge-schemas", "check-rules", "infer-schema",
            "mask-message", "extract-metadata", "list-types", "grpc-invoke", "reflect",
            "generate-stubs", "gather-git", "service-register", "service-list",
            "service-inspect", "service-refresh", "service-invoke", "compile-workflow", "suggest-mappings",
            "record-workflow-run", "replay-workflow", "promote-workflow",
            "export-work-record", "verify-work-record", "evaluate-work-record");

    @TempDir
    static Path pki;

    private static Path caCert;
    private static Path serverCert;
    private static Path serverKey;
    private static Path clientCert;
    private static Path clientKey;

    @TempDir
    Path workspace;

    @BeforeAll
    static void issueCertificates() throws Exception {
        String password = "test-only-password";
        Path caStore = pki.resolve("ca.p12");
        keytool("-genkeypair", "-alias", "ca", "-keyalg", "EC", "-groupname", "secp256r1",
                "-dname", "CN=mcp-ca", "-ext", "bc:c", "-validity", "2",
                "-keystore", caStore.toString(), "-storetype", "PKCS12", "-storepass", password);
        caCert = pki.resolve("ca.pem");
        keytool("-exportcert", "-rfc", "-alias", "ca", "-file", caCert.toString(),
                "-keystore", caStore.toString(), "-storepass", password);

        Path serverStore = pki.resolve("server.p12");
        keytool("-genkeypair", "-alias", "server", "-keyalg", "EC", "-groupname", "secp256r1",
                "-dname", "CN=mcp-server", "-validity", "2",
                "-keystore", serverStore.toString(), "-storetype", "PKCS12", "-storepass", password);
        Path serverCsr = pki.resolve("server.csr");
        keytool("-certreq", "-alias", "server", "-file", serverCsr.toString(),
                "-keystore", serverStore.toString(), "-storepass", password);
        serverCert = pki.resolve("server.pem");
        keytool("-gencert", "-rfc", "-alias", "ca", "-infile", serverCsr.toString(),
                "-outfile", serverCert.toString(), "-ext", "SAN=dns:localhost,ip:127.0.0.1",
                "-validity", "2", "-keystore", caStore.toString(), "-storepass", password);
        serverKey = exportKey(serverStore, "server", password, pki.resolve("server.key"));

        Path clientStore = pki.resolve("client.p12");
        keytool("-genkeypair", "-alias", "client", "-keyalg", "EC", "-groupname", "secp256r1",
                "-dname", "CN=mcp-client", "-validity", "2",
                "-keystore", clientStore.toString(), "-storetype", "PKCS12", "-storepass", password);
        Path clientCsr = pki.resolve("client.csr");
        keytool("-certreq", "-alias", "client", "-file", clientCsr.toString(),
                "-keystore", clientStore.toString(), "-storepass", password);
        clientCert = pki.resolve("client.pem");
        keytool("-gencert", "-rfc", "-alias", "ca", "-infile", clientCsr.toString(),
                "-outfile", clientCert.toString(), "-validity", "2",
                "-keystore", caStore.toString(), "-storepass", password);
        clientKey = exportKey(clientStore, "client", password, pki.resolve("client.key"));
    }

    @Test
    void helpPrintsUsageToStderrAndReturns() throws Exception {
        PrintStream original = System.err;
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        try {
            System.setErr(new PrintStream(captured, true, StandardCharsets.UTF_8));
            McpMain.main(new String[]{"--help"});
        } finally {
            System.setErr(original);
        }
        assertThat(captured.toString(StandardCharsets.UTF_8))
                .contains("usage: protomolt-mcp")
                .contains("--registry-git", "--service-workspace", "--profile-tls-config",
                        "PROTOMOLT_PROFILE_TLS_CONFIG", "--workflow-workspace");
    }

    @Test
    void standaloneCatalogMatchesDocumentedInventory() {
        assertThat(McpMain.catalog(ActionContext.create()).names())
                .containsExactlyInAnyOrderElementsOf(STANDALONE_TOOLS);
    }

    @Test
    void stdioLauncherDialsMutualTlsWhenConfiguredAndRefusesUnsupportedTransportWhenUnconfigured()
            throws Exception {
        AtomicReference<String> presentedClient = new AtomicReference<>();
        ServerInterceptor peerRecorder = new ServerInterceptor() {
            @Override
            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                    ServerCall<ReqT, RespT> call, Metadata headers,
                    ServerCallHandler<ReqT, RespT> next) {
                SSLSession session = call.getAttributes().get(Grpc.TRANSPORT_ATTR_SSL_SESSION);
                try {
                    X509Certificate peer = (X509Certificate) session.getPeerCertificates()[0];
                    presentedClient.set(peer.getSubjectX500Principal().getName());
                } catch (Exception ignored) {
                    presentedClient.set(null);
                }
                return next.startCall(call, headers);
            }
        };
        Server server = Grpc.newServerBuilderForPort(0, TlsServerCredentials.newBuilder()
                        .keyManager(serverCert.toFile(), serverKey.toFile())
                        .trustManager(caCert.toFile())
                        .clientAuth(TlsServerCredentials.ClientAuth.REQUIRE)
                        .build())
                .addService(ServerInterceptors.intercept(
                        new HealthStatusManager().getHealthService(), peerRecorder))
                .addService(ServerInterceptors.intercept(
                        ProtoReflectionServiceV1.newInstance(), peerRecorder))
                .build()
                .start();
        try {
            int port = server.getPort();
            ObjectNode registerArgs = registerArguments(port);

            // 1. Unconfigured standalone MCP refuses trust_ref + client_certificate_ref.
            List<JsonNode> unconfiguredReplies = runStdioSession(
                    new String[]{"--service-workspace", workspace.resolve("unconfigured").toString()},
                    null,
                    List.of(toolCall(1, "service-register", registerArgs)));
            JsonNode unconfiguredCall = responseForId(unconfiguredReplies, 1);
            assertThat(unconfiguredCall.path("result").path("isError").asBoolean()).isTrue();
            assertThat(unconfiguredCall.path("result").toString())
                    .contains("unsupported-transport");
            assertThat(presentedClient.get()).isNull();

            // 2. Configured via --profile-tls-config dials mTLS across register, invoke, and
            // stored reflected verbs on subsequent startup.
            Path tlsConfigPath = workspace.resolve("profile-tls.json");
            ProfileTlsConfig tlsConfig = ProfileTlsConfig.newBuilder()
                    .setRootDirectory(pki.toString())
                    .addTrustBundles(TrustBundleMount.newBuilder()
                            .setRef("tls:mcp-ca")
                            .setBundlePath("ca.pem"))
                    .addClientCertificates(ClientCertificateMount.newBuilder()
                            .setRef("tls:mcp-client")
                            .setCertificateChainPath("client.pem")
                            .setPrivateKeyPath("client.key"))
                    .addBindings(ApprovedTlsEndpoint.newBuilder()
                            .setProfile("mtls-health")
                            .setEndpoint("local")
                            .setHost("localhost")
                            .setPort(port)
                            .setTrustRef("tls:mcp-ca")
                            .setClientCertificateRef("tls:mcp-client"))
                    .build();
            Files.writeString(tlsConfigPath, JsonFormat.printer().print(tlsConfig),
                    StandardCharsets.UTF_8);

            Path configuredWorkspace = workspace.resolve("configured");
            ObjectNode invokeArgs = MAPPER.createObjectNode()
                    .put("name", "mtls-health")
                    .put("method", "grpc.health.v1.Health/Check");
            invokeArgs.putObject("request").put("service", "");

            List<JsonNode> registerReplies = runStdioSession(
                    new String[]{
                            "--service-workspace", configuredWorkspace.toString(),
                            "--profile-tls-config", tlsConfigPath.toString()
                    },
                    null,
                    List.of(toolCall(10, "service-register", registerArgs)));
            JsonNode registered = responseForId(registerReplies, 10);
            assertThat(registered.path("result").path("structuredContent").path("ok").asBoolean())
                    .as("%s", registered)
                    .isTrue();
            assertThat(presentedClient.get()).isEqualTo("CN=mcp-client");

            // 3. Restarting McpMain with PROTOMOLT_PROFILE_TLS_CONFIG env fallback mounts the
            // stored reflected verb (`mtls-health-check`) and dials both `service-invoke` and
            // `mtls-health-check` over mTLS cleanly.
            presentedClient.set(null);
            List<JsonNode> invokeReplies = runStdioSession(
                    new String[]{"--service-workspace", configuredWorkspace.toString()},
                    tlsConfigPath.toString(),
                    List.of(
                            toolCall(11, "service-invoke", invokeArgs),
                            toolCall(20, "mtls-health-check", MAPPER.createObjectNode())));
            JsonNode invoked = responseForId(invokeReplies, 11);
            assertThat(invoked.path("result").path("structuredContent").path("ok").asBoolean())
                    .as("%s", invoked)
                    .isTrue();
            JsonNode reflected = responseForId(invokeReplies, 20);
            assertThat(reflected.path("result").path("structuredContent").path("status").asText())
                    .as("%s", reflected)
                    .isEqualTo("SERVING");
            assertThat(presentedClient.get()).isEqualTo("CN=mcp-client");
        } finally {
            server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    private static List<JsonNode> runStdioSession(
            String[] args, String envTlsConfig, List<ObjectNode> requests) throws Exception {
        StringBuilder stdin = new StringBuilder();
        stdin.append("{\"jsonrpc\":\"2.0\",\"id\":0,\"method\":\"initialize\",\"params\":")
                .append("{\"protocolVersion\":\"2025-06-18\",\"capabilities\":{},")
                .append("\"clientInfo\":{\"name\":\"mcp-main-test\",\"version\":\"1\"}}}\n");
        stdin.append("{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}\n");
        for (ObjectNode req : requests) {
            stdin.append(MAPPER.writeValueAsString(req)).append('\n');
        }
        ByteArrayOutputStream stdout = new ByteArrayOutputStream();
        McpMain.run(args, envTlsConfig,
                new ByteArrayInputStream(stdin.toString().getBytes(StandardCharsets.UTF_8)),
                stdout);
        List<JsonNode> replies = new ArrayList<>();
        for (String line : stdout.toString(StandardCharsets.UTF_8).lines().toList()) {
            if (!line.isBlank()) {
                replies.add(MAPPER.readTree(line));
            }
        }
        return replies;
    }

    private static JsonNode responseForId(List<JsonNode> replies, int id) {
        return replies.stream()
                .filter(node -> node.path("id").asInt(-1) == id)
                .findFirst()
                .orElseThrow(() -> new AssertionError("missing reply id " + id + " in " + replies));
    }

    private static ObjectNode toolCall(int id, String name, ObjectNode arguments) {
        ObjectNode rpc = MAPPER.createObjectNode();
        rpc.put("jsonrpc", "2.0");
        rpc.put("id", id);
        rpc.put("method", "tools/call");
        ObjectNode params = rpc.putObject("params");
        params.put("name", name);
        params.set("arguments", arguments);
        return rpc;
    }

    private static ObjectNode registerArguments(int port) {
        ObjectNode args = MAPPER.createObjectNode();
        ObjectNode profile = args.putObject("profile");
        profile.put("name", "mtls-health").put("description", "Mutual TLS health service");
        profile.putArray("endpoints").addObject()
                .put("name", "local")
                .put("host", "localhost")
                .put("port", port)
                .put("transport", "TRANSPORT_TLS")
                .put("trustRef", "tls:mcp-ca")
                .put("clientCertificateRef", "tls:mcp-client");
        args.put("endpoint", "local");
        args.put("deadlineMs", 5_000);
        return args;
    }

    private static Path exportKey(Path store, String alias, String password, Path outFile)
            throws Exception {
        KeyStore ks = KeyStore.getInstance("PKCS12");
        try (var in = Files.newInputStream(store)) {
            ks.load(in, password.toCharArray());
        }
        PrivateKey key = (PrivateKey) ks.getKey(alias, password.toCharArray());
        Files.writeString(outFile, "-----BEGIN PRIVATE KEY-----\n"
                + Base64.getMimeEncoder(64, "\n".getBytes(StandardCharsets.US_ASCII))
                        .encodeToString(key.getEncoded())
                + "\n-----END PRIVATE KEY-----\n", StandardCharsets.US_ASCII);
        return outFile;
    }

    private static void keytool(String... args) throws Exception {
        List<String> command = new ArrayList<>();
        command.add(Path.of(System.getProperty("java.home"), "bin", "keytool").toString());
        command.add("-noprompt");
        command.addAll(List.of(args));
        Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
        String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        if (!process.waitFor(60, TimeUnit.SECONDS) || process.exitValue() != 0) {
            throw new IllegalStateException("keytool failed: " + output);
        }
    }
}
