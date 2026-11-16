package ai.protomolt.proto.grpc.workspace;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.protomolt.proto.actions.ActionException;
import ai.protomolt.proto.grpc.profile.v1.ServiceEndpoint;
import ai.protomolt.proto.grpc.profile.v1.ServiceProfile;
import ai.protomolt.proto.grpc.profile.v1.Transport;
import ai.protomolt.proto.grpc.workspace.v1.ApprovedTlsEndpoint;
import ai.protomolt.proto.grpc.workspace.v1.ClientCertificateMount;
import ai.protomolt.proto.grpc.workspace.v1.ProfileTlsConfig;
import ai.protomolt.proto.grpc.workspace.v1.TrustBundleMount;
import com.google.protobuf.util.JsonFormat;
import io.grpc.ChannelCredentials;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ProfileTlsConfigsTest {

    @TempDir
    static Path pki;

    private static Path caCert;
    private static Path leafCert;
    private static Path leafKey;

    @TempDir
    Path dir;

    @BeforeAll
    static void generateTestPems() throws Exception {
        String password = "test-only-password";
        Path caStore = pki.resolve("ca.p12");
        keytool("-genkeypair", "-alias", "ca", "-keyalg", "EC", "-groupname", "secp256r1",
                "-dname", "CN=config-ca", "-ext", "bc:c", "-validity", "2",
                "-keystore", caStore.toString(), "-storetype", "PKCS12", "-storepass", password);
        caCert = pki.resolve("ca.pem");
        keytool("-exportcert", "-rfc", "-alias", "ca", "-file", caCert.toString(),
                "-keystore", caStore.toString(), "-storepass", password);

        Path leafStore = pki.resolve("leaf.p12");
        keytool("-genkeypair", "-alias", "leaf", "-keyalg", "EC", "-groupname", "secp256r1",
                "-dname", "CN=config-leaf", "-validity", "2",
                "-keystore", leafStore.toString(), "-storetype", "PKCS12", "-storepass", password);
        Path csr = pki.resolve("leaf.csr");
        keytool("-certreq", "-alias", "leaf", "-file", csr.toString(),
                "-keystore", leafStore.toString(), "-storepass", password);
        leafCert = pki.resolve("leaf.pem");
        keytool("-gencert", "-rfc", "-alias", "ca", "-infile", csr.toString(),
                "-outfile", leafCert.toString(), "-validity", "2",
                "-keystore", caStore.toString(), "-storepass", password);

        KeyStore ks = KeyStore.getInstance("PKCS12");
        try (var in = Files.newInputStream(leafStore)) {
            ks.load(in, password.toCharArray());
        }
        PrivateKey key = (PrivateKey) ks.getKey("leaf", password.toCharArray());
        leafKey = pki.resolve("leaf.key");
        Files.writeString(leafKey, "-----BEGIN PRIVATE KEY-----\n"
                + Base64.getMimeEncoder(64, "\n".getBytes(StandardCharsets.US_ASCII))
                        .encodeToString(key.getEncoded())
                + "\n-----END PRIVATE KEY-----\n", StandardCharsets.US_ASCII);
    }

    @Test
    void loadsJsonAndBinaryConfigsResolvingRelativePathsAgainstParentAndRootDirectory()
            throws Exception {
        Path certsDir = Files.createDirectories(dir.resolve("mounted-certs"));
        Files.copy(caCert, certsDir.resolve("ca.pem"));
        Files.copy(leafCert, certsDir.resolve("client.pem"));
        Files.copy(leafKey, certsDir.resolve("client.key"));

        ProfileTlsConfig protoConfig = ProfileTlsConfig.newBuilder()
                .setRootDirectory(certsDir.toString())
                .addTrustBundles(TrustBundleMount.newBuilder()
                        .setRef("tls:corp-ca")
                        .setBundlePath("ca.pem"))
                .addClientCertificates(ClientCertificateMount.newBuilder()
                        .setRef("tls:orders-client")
                        .setCertificateChainPath("client.pem")
                        .setPrivateKeyPath("client.key"))
                .addBindings(ApprovedTlsEndpoint.newBuilder()
                        .setProfile("orders")
                        .setEndpoint("prod")
                        .setHost("orders.internal")
                        .setPort(8443)
                        .setTrustRef("tls:corp-ca")
                        .setClientCertificateRef("tls:orders-client"))
                .build();

        Path jsonPath = dir.resolve("profile-tls.json");
        Files.writeString(jsonPath, JsonFormat.printer().print(protoConfig), StandardCharsets.UTF_8);

        Path binpbPath = dir.resolve("profile-tls.binpb");
        Files.write(binpbPath, protoConfig.toByteArray());

        ServiceProfile profile = ServiceProfile.newBuilder().setName("orders").build();
        ServiceEndpoint endpoint = ServiceEndpoint.newBuilder()
                .setName("prod")
                .setHost("orders.internal")
                .setPort(8443)
                .setTransport(Transport.TRANSPORT_TLS)
                .setTrustRef("tls:corp-ca")
                .setClientCertificateRef("tls:orders-client")
                .build();

        for (Path configPath : List.of(jsonPath, binpbPath)) {
            FileProfileTlsResolver resolver = ProfileTlsConfigs.loadResolver(configPath);
            ChannelCredentials creds = resolver.channelCredentials(profile, endpoint);
            assertThat(creds).isNotNull();
        }

        // Without root_directory, relative paths resolve against the config file's parent directory.
        ProfileTlsConfig parentRelative = protoConfig.toBuilder().clearRootDirectory().build();
        Path parentJson = certsDir.resolve("local-tls.json");
        Files.writeString(parentJson, JsonFormat.printer().print(parentRelative),
                StandardCharsets.UTF_8);
        FileProfileTlsResolver localResolver = ProfileTlsConfigs.loadResolver(parentJson);
        assertThat(localResolver.channelCredentials(profile, endpoint)).isNotNull();
    }

    @Test
    void rejectsMalformedUnboundedDuplicateOrDanglingConfigsLoudly() throws Exception {
        // Unsupported extension
        Path txt = dir.resolve("tls.yaml");
        Files.writeString(txt, "{}", StandardCharsets.UTF_8);
        assertThatThrownBy(() -> ProfileTlsConfigs.load(txt))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(".json, .binpb, or .pb");

        // Empty bindings (violates min_items = 1)
        Path emptyBindings = dir.resolve("empty.json");
        Files.writeString(emptyBindings, "{}", StandardCharsets.UTF_8);
        assertThatThrownBy(() -> ProfileTlsConfigs.load(emptyBindings))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("profile TLS config is invalid");

        // Endpoint binding with neither trust_ref nor client_certificate_ref (violates CEL rule)
        Path noRefs = dir.resolve("no-refs.json");
        Files.writeString(noRefs, """
                {
                  "bindings": [{
                    "profile": "orders",
                    "endpoint": "prod",
                    "host": "orders.internal",
                    "port": 8443
                  }]
                }
                """, StandardCharsets.UTF_8);
        assertThatThrownBy(() -> ProfileTlsConfigs.load(noRefs))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must name a trust_ref or a client_certificate_ref");

        // Out-of-range port (violates lte = 65535)
        Path badPort = dir.resolve("bad-port.json");
        Files.writeString(badPort, """
                {
                  "trustBundles": [{"ref": "tls:ca", "bundlePath": "ca.pem"}],
                  "bindings": [{
                    "profile": "orders",
                    "endpoint": "prod",
                    "host": "orders.internal",
                    "port": 70000,
                    "trustRef": "tls:ca"
                  }]
                }
                """, StandardCharsets.UTF_8);
        assertThatThrownBy(() -> ProfileTlsConfigs.load(badPort))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("profile TLS config is invalid");

        // Unknown JSON field rejected
        Path unknownField = dir.resolve("unknown-field.json");
        Files.writeString(unknownField, "{\"unexpected\": true}", StandardCharsets.UTF_8);
        assertThatThrownBy(() -> ProfileTlsConfigs.load(unknownField))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("does not parse");

        // Oversized config file rejected before parsing
        Path oversized = dir.resolve("oversized.json");
        Files.writeString(oversized,
                "{\"padding\":\"" + "x".repeat(ProfileTlsConfigs.MAX_CONFIG_BYTES + 16) + "\"}",
                StandardCharsets.US_ASCII);
        assertThatThrownBy(() -> ProfileTlsConfigs.load(oversized))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exceeds");

        // Dangling trust_ref rejected
        ProfileTlsConfig danglingTrust = ProfileTlsConfig.newBuilder()
                .addBindings(ApprovedTlsEndpoint.newBuilder()
                        .setProfile("orders")
                        .setEndpoint("prod")
                        .setHost("orders.internal")
                        .setPort(8443)
                        .setTrustRef("tls:missing-ca"))
                .build();
        assertThatThrownBy(() -> ProfileTlsConfigs.toResolver(danglingTrust, dir))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unconfigured trust_ref");

        // Duplicate trust_bundle ref rejected
        ProfileTlsConfig duplicateTrust = ProfileTlsConfig.newBuilder()
                .addTrustBundles(TrustBundleMount.newBuilder()
                        .setRef("tls:dup")
                        .setBundlePath("a.pem"))
                .addTrustBundles(TrustBundleMount.newBuilder()
                        .setRef("tls:dup")
                        .setBundlePath("b.pem"))
                .addBindings(ApprovedTlsEndpoint.newBuilder()
                        .setProfile("orders")
                        .setEndpoint("prod")
                        .setHost("orders.internal")
                        .setPort(8443)
                        .setTrustRef("tls:dup"))
                .build();
        assertThatThrownBy(() -> ProfileTlsConfigs.toResolver(duplicateTrust, dir))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("duplicates trust_bundles ref");

        // Resolved resolver still enforces unapproved endpoint check cleanly
        Files.copy(caCert, dir.resolve("ca.pem"));
        ProfileTlsConfig validTrustOnly = ProfileTlsConfig.newBuilder()
                .addTrustBundles(TrustBundleMount.newBuilder()
                        .setRef("tls:ca")
                        .setBundlePath("ca.pem"))
                .addBindings(ApprovedTlsEndpoint.newBuilder()
                        .setProfile("orders")
                        .setEndpoint("prod")
                        .setHost("orders.internal")
                        .setPort(8443)
                        .setTrustRef("tls:ca"))
                .build();
        FileProfileTlsResolver resolver = ProfileTlsConfigs.toResolver(validTrustOnly, dir);
        ServiceProfile wrongProfile = ServiceProfile.newBuilder().setName("other").build();
        ServiceEndpoint endpoint = ServiceEndpoint.newBuilder()
                .setName("prod")
                .setHost("orders.internal")
                .setPort(8443)
                .setTransport(Transport.TRANSPORT_TLS)
                .setTrustRef("tls:ca")
                .build();
        assertThatThrownBy(() -> resolver.channelCredentials(wrongProfile, endpoint))
                .isInstanceOfSatisfying(ActionException.class, refused ->
                        assertThat(refused.code()).isEqualTo("credential-unavailable"));
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
