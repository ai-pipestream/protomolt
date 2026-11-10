package ai.protomolt.proto.grpc.workspace;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.protomolt.proto.actions.ActionCatalog;
import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.actions.ActionException;
import ai.protomolt.proto.grpc.invoke.ChannelFactory;
import ai.protomolt.proto.grpc.profile.FileSystemServiceProfileRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.grpc.ChannelCredentials;
import io.grpc.Grpc;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.TlsServerCredentials;
import io.grpc.protobuf.services.HealthStatusManager;
import io.grpc.protobuf.services.ProtoReflectionServiceV1;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Service profiles whose TLS endpoint names a {@code trust_ref} and a
 * {@code client_certificate_ref} (#823): register, refresh, inspection, invocation and the
 * reflected verb all reach a real gRPC server that requires a client certificate from a
 * private CA. The server records the presented client identity, so the tests show the
 * certificate files are read again for each channel and a rotated certificate is the one
 * presented next. Missing, unapproved or malformed material fails before any channel opens,
 * and no reply or error carries key material or file paths.
 */
class ProfileMutualTlsTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String PROFILE = "mtls-health";
    private static final String TRUST_REF = "tls:private-ca";
    private static final String CLIENT_REF = "tls:health-client";

    @TempDir
    static Path pki;

    private static Pem trustedCa;
    private static Pem server;
    private static Pem clientOne;
    private static Pem clientTwo;
    private static Pem untrustedClient;
    private static Pem otherCa;
    private static final List<String> keyLines = new ArrayList<>();

    @TempDir
    Path directory;

    private final AtomicReference<String> presentedClient = new AtomicReference<>();
    private Server tlsServer;
    private int port;
    private Path trustFile;
    private Path clientCertificateFile;
    private Path clientKeyFile;
    private ChannelFactory channels;

    @BeforeAll
    static void issueCertificates() throws Exception {
        Keytool keytool = new Keytool(pki);
        trustedCa = keytool.authority("trusted-ca");
        otherCa = keytool.authority("other-ca");
        server = keytool.leaf("server", "trusted-ca", "SAN=dns:localhost,ip:127.0.0.1");
        clientOne = keytool.leaf("client-one", "trusted-ca", null);
        clientTwo = keytool.leaf("client-two", "trusted-ca", null);
        untrustedClient = keytool.leaf("untrusted-client", "other-ca", null);
        for (Pem pem : List.of(clientOne, clientTwo, untrustedClient)) {
            keyLines.add(Files.readString(pem.key()).lines()
                    .filter(line -> !line.startsWith("-----")).findFirst().orElseThrow());
        }
    }

    @BeforeEach
    void start() throws Exception {
        ServerInterceptor identity = new ServerInterceptor() {
            @Override
            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                    ServerCall<ReqT, RespT> call, Metadata headers,
                    ServerCallHandler<ReqT, RespT> next) {
                SSLSession session = call.getAttributes().get(Grpc.TRANSPORT_ATTR_SSL_SESSION);
                try {
                    X509Certificate peer = (X509Certificate) session.getPeerCertificates()[0];
                    presentedClient.set(peer.getSubjectX500Principal().getName());
                } catch (SSLPeerUnverifiedException e) {
                    presentedClient.set(null);
                }
                return next.startCall(call, headers);
            }
        };
        tlsServer = Grpc.newServerBuilderForPort(0, TlsServerCredentials.newBuilder()
                        .keyManager(server.certificate().toFile(), server.key().toFile())
                        .trustManager(trustedCa.certificate().toFile())
                        .clientAuth(TlsServerCredentials.ClientAuth.REQUIRE)
                        .build())
                .addService(ServerInterceptors.intercept(
                        new HealthStatusManager().getHealthService(), identity))
                .addService(ServerInterceptors.intercept(
                        ProtoReflectionServiceV1.newInstance(), identity))
                .build()
                .start();
        port = tlsServer.getPort();

        trustFile = directory.resolve("trust.pem");
        clientCertificateFile = directory.resolve("client.pem");
        clientKeyFile = directory.resolve("client.key");
        Files.copy(trustedCa.certificate(), trustFile);
        install(clientOne);

        channels = new ChannelFactory() {
            @Override
            public ManagedChannel open(String target, boolean tls) {
                throw new AssertionError("an endpoint with TLS references opened a default "
                        + (tls ? "TLS" : "plaintext") + " channel");
            }

            @Override
            public ManagedChannel open(String target, ChannelCredentials credentials) {
                return Grpc.newChannelBuilder(target, credentials).build();
            }
        };
    }

    @AfterEach
    void stop() throws InterruptedException {
        tlsServer.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
    }

    @Test
    void everyPathPresentsTheClientCertificateAndTrustsOnlyThePrivateCa() throws Exception {
        ActionCatalog catalog = catalog(resolver(binding(TRUST_REF, CLIENT_REF)));

        ObjectNode registered = catalog.execute("service-register", registerInput(CLIENT_REF));
        assertThat(registered.path("ok").asBoolean()).as("%s", registered).isTrue();
        assertThat(presentedClient.get()).isEqualTo("CN=client-one");

        ObjectNode inspected = catalog.execute("service-inspect",
                MAPPER.createObjectNode().put("name", PROFILE));
        ObjectNode endpoint = (ObjectNode) inspected.path("profile").path("endpoints").get(0);
        assertThat(endpoint.path("trustRef").asText()).isEqualTo(TRUST_REF);
        assertThat(endpoint.path("clientCertificateRef").asText()).isEqualTo(CLIENT_REF);

        presentedClient.set(null);
        ObjectNode result = catalog.execute("service-invoke", checkInput());
        assertThat(result.path("ok").asBoolean()).as("%s", result).isTrue();
        assertThat(result.path("status").asText()).isEqualTo("OK");
        assertThat(presentedClient.get()).isEqualTo("CN=client-one");

        presentedClient.set(null);
        ObjectNode verb = catalog.execute("mtls-health-check", MAPPER.createObjectNode());
        assertThat(verb.path("status").asText()).isEqualTo("SERVING");
        assertThat(presentedClient.get()).isEqualTo("CN=client-one");

        for (ObjectNode reply : List.of(registered, inspected, result, verb)) {
            assertNoMaterial(reply.toString());
        }
    }

    @Test
    void aRotatedClientCertificateIsPresentedOnTheNextCall() throws Exception {
        ActionCatalog catalog = catalog(resolver(binding(TRUST_REF, CLIENT_REF)));
        catalog.execute("service-register", registerInput(CLIENT_REF));
        assertThat(presentedClient.get()).isEqualTo("CN=client-one");

        install(clientTwo);

        ObjectNode refreshed = catalog.execute("service-refresh",
                MAPPER.createObjectNode().put("name", PROFILE));
        assertThat(refreshed.path("ok").asBoolean()).as("%s", refreshed).isTrue();
        assertThat(presentedClient.get()).isEqualTo("CN=client-two");
        ObjectNode result = catalog.execute("service-invoke", checkInput());
        assertThat(result.path("ok").asBoolean()).isTrue();
        assertThat(presentedClient.get()).isEqualTo("CN=client-two");
    }

    @Test
    void aCertificateFromAnUntrustedCaIsRefusedByTheServer() throws Exception {
        ActionCatalog catalog = catalog(resolver(binding(TRUST_REF, CLIENT_REF)));
        catalog.execute("service-register", registerInput(CLIENT_REF));

        install(untrustedClient);

        ObjectNode result = catalog.execute("service-invoke", checkInput());
        assertThat(result.path("ok").asBoolean()).isFalse();
        assertThat(result.path("status").asText()).isEqualTo("UNAVAILABLE");
        assertNoMaterial(result.toString());
    }

    @Test
    void trustWithoutAClientCertificateFailsTheHandshake() throws Exception {
        ActionCatalog catalog = catalog(resolver(binding(TRUST_REF, "")));

        ObjectNode registered = catalog.execute("service-register", registerInput(""));
        assertThat(registered.path("ok").asBoolean()).isFalse();
        assertThat(presentedClient.get()).isNull();
        assertNoMaterial(registered.toString());
    }

    @Test
    void theServerIsNotTrustedUnderAnotherCaBundle() throws Exception {
        Files.copy(otherCa.certificate(), trustFile, StandardCopyOption.REPLACE_EXISTING);
        ActionCatalog catalog = catalog(resolver(binding(TRUST_REF, CLIENT_REF)));

        ObjectNode registered = catalog.execute("service-register", registerInput(CLIENT_REF));
        assertThat(registered.path("ok").asBoolean()).isFalse();
        assertThat(presentedClient.get()).isNull();
    }

    @Test
    void missingUnapprovedOrMalformedMaterialFailsBeforeAnyChannelOpens() throws Exception {
        ActionCatalog working = catalog(resolver(binding(TRUST_REF, CLIENT_REF)));
        working.execute("service-register", registerInput(CLIENT_REF));
        presentedClient.set(null);

        ActionCatalog unapproved = catalog(resolver(new FileProfileTlsResolver.Binding(
                PROFILE, "local", "localhost", port + 1, TRUST_REF, CLIENT_REF)));
        assertRefused(unapproved, "not approved for TLS material");

        ActionCatalog unconfigured = catalog(ProfileCredentialResolver.withTls(null,
                new FileProfileTlsResolver(Set.of(binding(TRUST_REF, CLIENT_REF)),
                        Map.of(TRUST_REF, trustFile), Map.of())));
        assertRefused(unconfigured, "client-certificate reference is not configured");

        Files.writeString(clientKeyFile, "-----BEGIN PRIVATE KEY-----\nAAAA\n"
                + "-----END PRIVATE KEY-----\n");
        assertRefused(working, "TLS material could not be loaded");

        Files.delete(clientKeyFile);
        assertRefused(working, "TLS material could not be loaded");
        // The cause is kept for host diagnostics, while the wire message stays generic.
        assertThatThrownBy(() -> working.execute("service-invoke", checkInput()))
                .isInstanceOfSatisfying(ActionException.class, refused ->
                        assertThat(refused.getCause()).isInstanceOf(NoSuchFileException.class));

        ActionCatalog withoutTls = catalog(null);
        assertThatThrownBy(() -> withoutTls.execute("service-invoke", checkInput()))
                .isInstanceOfSatisfying(ActionException.class, refused -> {
                    assertThat(refused.code()).isEqualTo("unsupported-transport");
                    assertThat(refused.getMessage()).contains("configured TLS resolver");
                });
        assertThat(presentedClient.get()).isNull();
    }

    @Test
    void tlsReferencesOnAPlaintextEndpointAreRefused() throws Exception {
        ActionCatalog catalog = catalog(resolver(binding(TRUST_REF, CLIENT_REF)));
        ObjectNode input = registerInput(CLIENT_REF);
        ((ObjectNode) input.path("profile").path("endpoints").get(0))
                .put("transport", "TRANSPORT_PLAINTEXT");

        assertThatThrownBy(() -> catalog.execute("service-register", input))
                .isInstanceOfSatisfying(ActionException.class, refused -> {
                    assertThat(refused.code()).isEqualTo("unsupported-transport");
                    assertThat(refused.getMessage()).contains("need TLS transport");
                });
        assertThat(presentedClient.get()).isNull();
    }

    private void assertRefused(ActionCatalog catalog, String message) {
        for (String verb : List.of("service-invoke", "service-refresh", "service-inspect")) {
            ObjectNode input = "service-invoke".equals(verb) ? checkInput()
                    : MAPPER.createObjectNode().put("name", PROFILE);
            assertThatThrownBy(() -> catalog.execute(verb, input))
                    .as(verb)
                    .isInstanceOfSatisfying(ActionException.class, refused -> {
                        assertThat(refused.code()).isEqualTo("credential-unavailable");
                        assertThat(refused.getMessage()).contains(message);
                        assertNoMaterial(refused.getMessage());
                    });
        }
        assertThat(presentedClient.get()).isNull();
    }

    private void assertNoMaterial(String text) {
        assertThat(text)
                .doesNotContain("PRIVATE KEY")
                .doesNotContain("BEGIN CERTIFICATE")
                .doesNotContain(directory.toString())
                .doesNotContain(pki.toString());
        for (String keyLine : keyLines) {
            assertThat(text).doesNotContain(keyLine);
        }
    }

    private ActionCatalog catalog(ProfileCredentialResolver resolver) throws IOException {
        return ServiceWorkspaceActions.register(
                ActionCatalog.defaults(ActionContext.create()),
                new FileSystemServiceProfileRepository(directory.resolve("profiles")), null,
                channels, resolver);
    }

    private ProfileCredentialResolver resolver(FileProfileTlsResolver.Binding binding) {
        Map<String, FileProfileTlsResolver.KeyPairFiles> clients = binding.clientCertificateRef()
                .isBlank() ? Map.of()
                : Map.of(CLIENT_REF, new FileProfileTlsResolver.KeyPairFiles(
                        clientCertificateFile, clientKeyFile));
        return ProfileCredentialResolver.withTls(null, new FileProfileTlsResolver(
                Set.of(binding), Map.of(TRUST_REF, trustFile), clients));
    }

    private FileProfileTlsResolver.Binding binding(String trustRef, String clientRef) {
        return new FileProfileTlsResolver.Binding(PROFILE, "local", "localhost", port,
                trustRef, clientRef);
    }

    private void install(Pem client) throws IOException {
        Files.copy(client.certificate(), clientCertificateFile,
                StandardCopyOption.REPLACE_EXISTING);
        Files.copy(client.key(), clientKeyFile, StandardCopyOption.REPLACE_EXISTING);
    }

    private ObjectNode registerInput(String clientRef) {
        ObjectNode input = MAPPER.createObjectNode();
        ObjectNode profile = input.putObject("profile");
        profile.put("name", PROFILE).put("description", "Health over mutual TLS");
        ObjectNode endpoint = profile.putArray("endpoints").addObject();
        endpoint.put("name", "local").put("host", "localhost").put("port", port)
                .put("transport", "TRANSPORT_TLS").put("trustRef", TRUST_REF);
        if (!clientRef.isBlank()) {
            endpoint.put("clientCertificateRef", clientRef);
        }
        input.put("endpoint", "local");
        input.put("deadlineMs", 5_000);
        return input;
    }

    private static ObjectNode checkInput() {
        ObjectNode invocation = MAPPER.createObjectNode()
                .put("name", PROFILE)
                .put("method", "grpc.health.v1.Health/Check");
        invocation.putObject("request").put("service", "");
        return invocation;
    }

    /** A PEM certificate and, for a leaf or an authority, its PKCS#8 PEM key. */
    private record Pem(Path certificate, Path key) {
    }

    /**
     * Issues a small private PKI with the JDK's own keytool, so the tests need no
     * certificate library and no checked-in key material.
     */
    private static final class Keytool {
        private static final String PASSWORD = "test-only-store";
        private final Path dir;

        Keytool(Path dir) {
            this.dir = dir;
        }

        Pem authority(String name) throws Exception {
            Path store = dir.resolve(name + ".p12");
            run("-genkeypair", "-alias", name, "-keyalg", "EC", "-groupname", "secp256r1",
                    "-dname", "CN=" + name, "-ext", "bc:c", "-validity", "2",
                    "-keystore", store.toString(), "-storetype", "PKCS12",
                    "-storepass", PASSWORD);
            Path certificate = dir.resolve(name + ".pem");
            run("-exportcert", "-rfc", "-alias", name, "-file", certificate.toString(),
                    "-keystore", store.toString(), "-storepass", PASSWORD);
            return new Pem(certificate, exportKey(store, name));
        }

        Pem leaf(String name, String issuer, String san) throws Exception {
            Path store = dir.resolve(name + ".p12");
            run("-genkeypair", "-alias", name, "-keyalg", "EC", "-groupname", "secp256r1",
                    "-dname", "CN=" + name, "-validity", "2",
                    "-keystore", store.toString(), "-storetype", "PKCS12",
                    "-storepass", PASSWORD);
            Path request = dir.resolve(name + ".csr");
            run("-certreq", "-alias", name, "-file", request.toString(),
                    "-keystore", store.toString(), "-storepass", PASSWORD);
            Path certificate = dir.resolve(name + ".pem");
            List<String> sign = new ArrayList<>(List.of("-gencert", "-rfc",
                    "-alias", issuer, "-infile", request.toString(),
                    "-outfile", certificate.toString(), "-validity", "2",
                    "-keystore", dir.resolve(issuer + ".p12").toString(),
                    "-storepass", PASSWORD));
            if (san != null) {
                sign.add("-ext");
                sign.add(san);
            }
            run(sign.toArray(String[]::new));
            return new Pem(certificate, exportKey(store, name));
        }

        private Path exportKey(Path store, String alias) throws Exception {
            KeyStore keys = KeyStore.getInstance("PKCS12");
            try (var in = Files.newInputStream(store)) {
                keys.load(in, PASSWORD.toCharArray());
            }
            PrivateKey key = (PrivateKey) keys.getKey(alias, PASSWORD.toCharArray());
            Path file = dir.resolve(alias + ".key");
            Files.writeString(file, "-----BEGIN PRIVATE KEY-----\n"
                    + Base64.getMimeEncoder(64, "\n".getBytes(StandardCharsets.US_ASCII))
                            .encodeToString(key.getEncoded())
                    + "\n-----END PRIVATE KEY-----\n", StandardCharsets.US_ASCII);
            return file;
        }

        private void run(String... args) throws Exception {
            List<String> command = new ArrayList<>();
            command.add(Path.of(System.getProperty("java.home"), "bin", "keytool").toString());
            command.add("-noprompt");
            command.addAll(List.of(args));
            Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
            String output = new String(process.getInputStream().readAllBytes(),
                    StandardCharsets.UTF_8);
            if (!process.waitFor(60, TimeUnit.SECONDS) || process.exitValue() != 0) {
                throw new IllegalStateException("keytool " + args[0] + " failed: " + output);
            }
        }
    }
}
