package ai.protomolt.proto.serve;

import static org.assertj.core.api.Assertions.assertThat;

import ai.protomolt.proto.grpc.workspace.v1.ApprovedTlsEndpoint;
import ai.protomolt.proto.grpc.workspace.v1.ClientCertificateMount;
import ai.protomolt.proto.grpc.workspace.v1.ProfileTlsConfig;
import ai.protomolt.proto.grpc.workspace.v1.TrustBundleMount;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.util.JsonFormat;
import io.grpc.Attributes;
import io.grpc.Grpc;
import io.grpc.InsecureServerCredentials;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.ServerTransportFilter;
import io.grpc.Status;
import io.grpc.TlsServerCredentials;
import io.grpc.protobuf.services.HealthStatusManager;
import io.grpc.protobuf.services.ProtoReflectionServiceV1;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.net.ssl.SSLSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end verification for {@code protomolt-serve --profile-tls-config} (#881, #875):
 * stock {@link ProtoMoltServe} dials an external mTLS service across REST and MCP, composes
 * cleanly with {@link CorrectionStarter}'s bearer {@code EnvProfileCredentialResolver}, closes
 * pooled outbound TLS channels on shutdown, and preserves {@code unsupported-transport} refusal
 * whenever {@code --profile-tls-config} is omitted.
 */
class ServeProfileTlsTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String CONSOLE_TOKEN = "console-login-token-with-at-least-32-chars";
    private static final Metadata.Key<String> AUTHORIZATION_HEADER =
            Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);

    @TempDir
    static Path pki;

    private static Path caCert;
    private static Path serverCert;
    private static Path serverKey;
    private static Path clientCert;
    private static Path clientKey;

    @TempDir
    Path dir;

    private final AtomicReference<String> presentedMtlsClient = new AtomicReference<>();
    private final AtomicInteger mtlsHandshakes = new AtomicInteger();
    private final AtomicInteger mtlsTerminatedTransports = new AtomicInteger();
    private Server mtlsServer;
    private int mtlsPort;

    @BeforeAll
    static void issueCertificates() throws Exception {
        String password = "test-only-password";
        Path caStore = pki.resolve("ca.p12");
        keytool("-genkeypair", "-alias", "ca", "-keyalg", "EC", "-groupname", "secp256r1",
                "-dname", "CN=serve-ca", "-ext", "bc:c", "-validity", "2",
                "-keystore", caStore.toString(), "-storetype", "PKCS12", "-storepass", password);
        caCert = pki.resolve("ca.pem");
        keytool("-exportcert", "-rfc", "-alias", "ca", "-file", caCert.toString(),
                "-keystore", caStore.toString(), "-storepass", password);

        Path serverStore = pki.resolve("server.p12");
        keytool("-genkeypair", "-alias", "server", "-keyalg", "EC", "-groupname", "secp256r1",
                "-dname", "CN=serve-mtls-target", "-validity", "2",
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
                "-dname", "CN=serve-client-one", "-validity", "2",
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

    @BeforeEach
    void startMtlsTarget() throws Exception {
        presentedMtlsClient.set(null);
        mtlsHandshakes.set(0);
        mtlsTerminatedTransports.set(0);
        ServerInterceptor peerRecorder = new ServerInterceptor() {
            @Override
            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                    ServerCall<ReqT, RespT> call, Metadata headers,
                    ServerCallHandler<ReqT, RespT> next) {
                SSLSession session = call.getAttributes().get(Grpc.TRANSPORT_ATTR_SSL_SESSION);
                try {
                    X509Certificate peer = (X509Certificate) session.getPeerCertificates()[0];
                    presentedMtlsClient.set(peer.getSubjectX500Principal().getName());
                } catch (Exception ignored) {
                    presentedMtlsClient.set(null);
                }
                return next.startCall(call, headers);
            }
        };
        mtlsServer = Grpc.newServerBuilderForPort(0, TlsServerCredentials.newBuilder()
                        .keyManager(serverCert.toFile(), serverKey.toFile())
                        .trustManager(caCert.toFile())
                        .clientAuth(TlsServerCredentials.ClientAuth.REQUIRE)
                        .build())
                .addTransportFilter(new ServerTransportFilter() {
                    @Override
                    public Attributes transportReady(Attributes transportAttrs) {
                        mtlsHandshakes.incrementAndGet();
                        return transportAttrs;
                    }

                    @Override
                    public void transportTerminated(Attributes transportAttrs) {
                        mtlsTerminatedTransports.incrementAndGet();
                    }
                })
                .addService(ServerInterceptors.intercept(
                        new HealthStatusManager().getHealthService(), peerRecorder))
                .addService(ServerInterceptors.intercept(
                        ProtoReflectionServiceV1.newInstance(), peerRecorder))
                .build()
                .start();
        mtlsPort = mtlsServer.getPort();
    }

    @AfterEach
    void stopMtlsTarget() throws InterruptedException {
        mtlsServer.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
    }

    @Test
    void stockServeWithProfileTlsConfigDialsExternalMtlsServiceAndClosesPooledChannelsOnStop()
            throws Exception {
        Path tlsConfigFile = writeTlsConfig(dir.resolve("profile-tls.json"));
        Path serviceWorkspace = dir.resolve("workspace");

        ProtoMoltServe.Options parsed = ProtoMoltServe.Options.parse(new String[]{
                "--host", "127.0.0.1",
                "--grpc-port", "0",
                "--http-port", "0",
                "--service-workspace", serviceWorkspace.toString(),
                "--profile-tls-config", tlsConfigFile.toString()
        });
        assertThat(parsed.profileTlsConfig()).isEqualTo(tlsConfigFile);

        try (HttpClient http = HttpClient.newHttpClient()) {
            try (ProtoMoltServe serve = ProtoMoltServe.start(parsed)) {
                HttpResponse<String> registerResp = postRest(
                        http, serve.httpPort(), "ServiceRegister", null, registerBody());
                assertThat(registerResp.statusCode()).as("%s", registerResp.body()).isEqualTo(200);
                JsonNode registered = MAPPER.readTree(registerResp.body());
                assertThat(registered.path("ok").asBoolean()).isTrue();
                assertThat(presentedMtlsClient.get()).isEqualTo("CN=serve-client-one");

                HttpResponse<String> inspectResp = postRest(
                        http, serve.httpPort(), "ServiceInspect", null,
                        MAPPER.createObjectNode().put("name", "mtls-health"));
                assertThat(inspectResp.statusCode()).isEqualTo(200);

                for (int i = 0; i < 2; i++) {
                    presentedMtlsClient.set(null);
                    HttpResponse<String> invokeResp = postRest(
                            http, serve.httpPort(), "ServiceInvoke", null, invokeHealthBody());
                    assertThat(invokeResp.statusCode()).isEqualTo(200);
                    assertThat(MAPPER.readTree(invokeResp.body()).path("ok").asBoolean()).isTrue();
                    assertThat(presentedMtlsClient.get()).isEqualTo("CN=serve-client-one");
                }

                // Reflected verb over HTTP /mcp also reuses the same pooled TLS channel.
                presentedMtlsClient.set(null);
                JsonNode mcpCall = callMcpTool(
                        http, serve.httpPort(), null, "mtls-health-check",
                        MAPPER.createObjectNode());
                assertThat(mcpCall.path("result").path("structuredContent")
                        .path("status").asText()).isEqualTo("SERVING");
                assertThat(presentedMtlsClient.get()).isEqualTo("CN=serve-client-one");
                assertThat(mtlsHandshakes.get()).isEqualTo(1);
                assertThat(mtlsTerminatedTransports.get()).isZero();
            }
            awaitTerminatedTransports(1);
        }
    }

    @Test
    void composesProfileTlsResolverCleanlyWithCorrectionStarterBearerCredentials()
            throws Exception {
        String correctionBearer = "correction-secret-bearer-token";
        AtomicReference<String> observedBearer = new AtomicReference<>();
        ServerInterceptor bearerCheck = new ServerInterceptor() {
            @Override
            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                    ServerCall<ReqT, RespT> call, Metadata headers,
                    ServerCallHandler<ReqT, RespT> next) {
                String auth = headers.get(AUTHORIZATION_HEADER);
                observedBearer.set(auth);
                if (!("Bearer " + correctionBearer).equals(auth)) {
                    call.close(Status.UNAUTHENTICATED, new Metadata());
                    return new ServerCall.Listener<>() {};
                }
                return next.startCall(call, headers);
            }
        };
        Server correctionBackend = Grpc.newServerBuilderForPort(0,
                        InsecureServerCredentials.create())
                .addService(ServerInterceptors.intercept(
                        new HealthStatusManager().getHealthService(), bearerCheck))
                .addService(ServerInterceptors.intercept(
                        ProtoReflectionServiceV1.newInstance(), bearerCheck))
                .build()
                .start();
        try {
            int correctionPort = correctionBackend.getPort();
            CorrectionStarter starter = CorrectionStarter.fromEnvironment(Map.of(
                    "PROTOMOLT_CORRECTION_TARGET", "127.0.0.1:" + correctionPort,
                    "PROTOMOLT_CORRECTION_API_TOKEN", correctionBearer));
            Path tlsConfigFile = writeTlsConfig(dir.resolve("composed-tls.json"));
            String operatorToken = "operator-api-token";
            ProtoMoltServe.Options options = new ProtoMoltServe.Options(
                    "127.0.0.1", 0, 0, null, 0, operatorToken, false, null, null,
                    List.of(), dir.resolve("composed-workspace"), null, null, null,
                    new ProtoMoltServe.TaskConsoleOptions(CONSOLE_TOKEN, Duration.ofMinutes(30)),
                    null, null, null, null, null, null, tlsConfigFile);

            try (HttpClient http = HttpClient.newHttpClient();
                 ProtoMoltServe serve = ProtoMoltServe.start(options, starter)) {
                // CorrectionStarter already registered `correction` on startup with bearer auth.
                assertThat(observedBearer.get()).isEqualTo("Bearer " + correctionBearer);

                observedBearer.set(null);
                ObjectNode invokeCorrection = MAPPER.createObjectNode()
                        .put("name", "correction")
                        .put("method", "grpc.health.v1.Health/Check");
                invokeCorrection.putObject("request").put("service", "");
                HttpResponse<String> correctionResp = postRest(
                        http, serve.httpPort(), "ServiceInvoke", operatorToken, invokeCorrection);
                assertThat(correctionResp.statusCode()).isEqualTo(200);
                assertThat(MAPPER.readTree(correctionResp.body()).path("ok").asBoolean()).isTrue();
                assertThat(observedBearer.get()).isEqualTo("Bearer " + correctionBearer);

                // External mTLS profile registers and invokes on the same ProtoMoltServe instance.
                HttpResponse<String> mtlsRegResp = postRest(
                        http, serve.httpPort(), "ServiceRegister", operatorToken, registerBody());
                assertThat(mtlsRegResp.statusCode()).isEqualTo(200);
                assertThat(MAPPER.readTree(mtlsRegResp.body()).path("ok").asBoolean()).isTrue();
                assertThat(presentedMtlsClient.get()).isEqualTo("CN=serve-client-one");

                presentedMtlsClient.set(null);
                HttpResponse<String> mtlsInvokeResp = postRest(
                        http, serve.httpPort(), "ServiceInvoke", operatorToken, invokeHealthBody());
                assertThat(mtlsInvokeResp.statusCode()).isEqualTo(200);
                assertThat(MAPPER.readTree(mtlsInvokeResp.body()).path("ok").asBoolean()).isTrue();
                assertThat(presentedMtlsClient.get()).isEqualTo("CN=serve-client-one");
            }
        } finally {
            correctionBackend.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void unconfiguredServePreservesUnsupportedTransportRefusalWithAndWithoutCorrectionStarter()
            throws Exception {
        try (HttpClient http = HttpClient.newHttpClient()) {
            // 1. Stock ProtoMoltServe without --profile-tls-config.
            ProtoMoltServe.Options unconfigured = ProtoMoltServe.Options.parse(new String[]{
                    "--host", "127.0.0.1",
                    "--grpc-port", "0",
                    "--http-port", "0",
                    "--service-workspace", dir.resolve("unconfigured-ws").toString()
            });
            try (ProtoMoltServe serve = ProtoMoltServe.start(unconfigured)) {
                HttpResponse<String> restRefused = postRest(
                        http, serve.httpPort(), "ServiceRegister", null, registerBody());
                assertThat(restRefused.statusCode()).isEqualTo(400);
                assertThat(restRefused.body()).contains("configured TLS resolver");

                JsonNode mcpRefused = callMcpTool(
                        http, serve.httpPort(), null, "service-register", registerBody());
                assertThat(mcpRefused.path("result").path("isError").asBoolean()).isTrue();
                assertThat(mcpRefused.path("result").toString())
                        .contains("unsupported-transport", "configured TLS resolver");
                assertThat(mtlsHandshakes.get()).isZero();
            }

            // 2. ProtoMoltServe with CorrectionStarter bearer resolver only (no TLS config).
            Server plainBackend = Grpc.newServerBuilderForPort(0, InsecureServerCredentials.create())
                    .addService(new HealthStatusManager().getHealthService())
                    .addService(ProtoReflectionServiceV1.newInstance())
                    .build()
                    .start();
            try {
                CorrectionStarter starter = CorrectionStarter.fromEnvironment(Map.of(
                        "PROTOMOLT_CORRECTION_TARGET", "127.0.0.1:" + plainBackend.getPort(),
                        "PROTOMOLT_CORRECTION_API_TOKEN", "starter-bearer"));
                String operatorToken = "operator-api-token";
                ProtoMoltServe.Options bearerOnly = new ProtoMoltServe.Options(
                        "127.0.0.1", 0, 0, null, 0, operatorToken, false, null, null,
                        List.of(), dir.resolve("bearer-only-ws"), null, null, null,
                        new ProtoMoltServe.TaskConsoleOptions(CONSOLE_TOKEN,
                                Duration.ofMinutes(30)));
                try (ProtoMoltServe serve = ProtoMoltServe.start(bearerOnly, starter)) {
                    JsonNode mcpRefused = callMcpTool(
                            http, serve.httpPort(), operatorToken, "service-register",
                            registerBody());
                    assertThat(mcpRefused.path("result").path("isError").asBoolean()).isTrue();
                    assertThat(mcpRefused.path("result").toString())
                            .contains("unsupported-transport", "configured TLS resolver");
                    assertThat(mtlsHandshakes.get()).isZero();
                }
            } finally {
                plainBackend.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            }
        }
    }

    private Path writeTlsConfig(Path target) throws Exception {
        ProfileTlsConfig config = ProfileTlsConfig.newBuilder()
                .setRootDirectory(pki.toString())
                .addTrustBundles(TrustBundleMount.newBuilder()
                        .setRef("tls:serve-ca")
                        .setBundlePath("ca.pem"))
                .addClientCertificates(ClientCertificateMount.newBuilder()
                        .setRef("tls:serve-client")
                        .setCertificateChainPath("client.pem")
                        .setPrivateKeyPath("client.key"))
                .addBindings(ApprovedTlsEndpoint.newBuilder()
                        .setProfile("mtls-health")
                        .setEndpoint("local")
                        .setHost("localhost")
                        .setPort(mtlsPort)
                        .setTrustRef("tls:serve-ca")
                        .setClientCertificateRef("tls:serve-client"))
                .build();
        Files.writeString(target, JsonFormat.printer().print(config), StandardCharsets.UTF_8);
        return target;
    }

    private ObjectNode registerBody() {
        ObjectNode body = MAPPER.createObjectNode();
        ObjectNode profile = body.putObject("profile");
        profile.put("name", "mtls-health").put("description", "External mTLS health target");
        profile.putArray("endpoints").addObject()
                .put("name", "local")
                .put("host", "localhost")
                .put("port", mtlsPort)
                .put("transport", "TRANSPORT_TLS")
                .put("trustRef", "tls:serve-ca")
                .put("clientCertificateRef", "tls:serve-client");
        body.put("endpoint", "local");
        body.put("deadlineMs", 5_000);
        return body;
    }

    private static ObjectNode invokeHealthBody() {
        ObjectNode body = MAPPER.createObjectNode()
                .put("name", "mtls-health")
                .put("method", "grpc.health.v1.Health/Check");
        body.putObject("request").put("service", "");
        return body;
    }

    private static HttpResponse<String> postRest(
            HttpClient http, int port, String method, String apiToken, ObjectNode body)
            throws Exception {
        HttpRequest.Builder req = HttpRequest.newBuilder(
                        URI.create("http://127.0.0.1:" + port + "/grpc-json/ProtoMoltService/" + method))
                .header("content-type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body.toString()));
        if (apiToken != null) {
            req.header("api_token", apiToken);
        }
        return http.send(req.build(), HttpResponse.BodyHandlers.ofString());
    }

    private static JsonNode callMcpTool(
            HttpClient http, int port, String bearerToken, String toolName, ObjectNode arguments)
            throws Exception {
        String endpoint = "http://127.0.0.1:" + port + "/mcp";
        HttpRequest.Builder initReq = HttpRequest.newBuilder(URI.create(endpoint))
                .header("content-type", "application/json")
                .header("accept", "application/json, text/event-stream")
                .POST(HttpRequest.BodyPublishers.ofString("""
                        {"jsonrpc":"2.0","id":1,"method":"initialize","params":
                         {"protocolVersion":"2025-06-18","capabilities":{},
                          "clientInfo":{"name":"serve-tls-test","version":"1"}}}
                        """));
        if (bearerToken != null) {
            initReq.header("authorization", "Bearer " + bearerToken);
        }
        HttpResponse<String> initialized = http.send(initReq.build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(initialized.statusCode()).isEqualTo(200);
        String sessionId = initialized.headers().firstValue("Mcp-Session-Id").orElseThrow();
        String protocolVersion = MAPPER.readTree(initialized.body())
                .path("result").path("protocolVersion").asText();

        HttpRequest.Builder readyReq = HttpRequest.newBuilder(URI.create(endpoint))
                .header("content-type", "application/json")
                .header("accept", "application/json, text/event-stream")
                .header("Mcp-Session-Id", sessionId)
                .header("MCP-Protocol-Version", protocolVersion)
                .POST(HttpRequest.BodyPublishers.ofString(
                        "{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}"));
        if (bearerToken != null) {
            readyReq.header("authorization", "Bearer " + bearerToken);
        }
        assertThat(http.send(readyReq.build(), HttpResponse.BodyHandlers.ofString()).statusCode())
                .isEqualTo(202);

        ObjectNode call = MAPPER.createObjectNode();
        call.put("jsonrpc", "2.0");
        call.put("id", 2);
        call.put("method", "tools/call");
        ObjectNode params = call.putObject("params");
        params.put("name", toolName);
        params.set("arguments", arguments);

        HttpRequest.Builder toolReq = HttpRequest.newBuilder(URI.create(endpoint))
                .header("content-type", "application/json")
                .header("accept", "application/json, text/event-stream")
                .header("Mcp-Session-Id", sessionId)
                .header("MCP-Protocol-Version", protocolVersion)
                .POST(HttpRequest.BodyPublishers.ofString(call.toString()));
        if (bearerToken != null) {
            toolReq.header("authorization", "Bearer " + bearerToken);
        }
        HttpResponse<String> response = http.send(toolReq.build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(200);
        return MAPPER.readTree(response.body());
    }

    private void awaitTerminatedTransports(int expected) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (mtlsTerminatedTransports.get() < expected && System.nanoTime() < deadline) {
            Thread.sleep(20);
        }
        assertThat(mtlsTerminatedTransports.get()).isGreaterThanOrEqualTo(expected);
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
