package ai.protomolt.proto.parse.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.protomolt.proto.grpc.profile.FileSystemServiceProfileRepository;
import ai.protomolt.proto.grpc.profile.v1.SchemaSource;
import ai.protomolt.proto.grpc.profile.v1.ServiceEndpoint;
import ai.protomolt.proto.grpc.profile.v1.ServiceProfile;
import ai.protomolt.proto.grpc.profile.v1.SourceKind;
import ai.protomolt.proto.grpc.profile.v1.Transport;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Proves parser registration IS a service profile: a profile saved to the
 * store registers its parser under the profile name, the registry dials the
 * named endpoint over real TCP, and dishonest configurations (missing
 * endpoint, TLS the client cannot honor) are rejected loudly at
 * construction.
 */
class ParserProfileDiscoveryTest {

    @TempDir
    static Path profileRoot;

    static Server parserServer;
    static int parserPort;

    @BeforeAll
    static void boot() throws Exception {
        parserServer = NettyServerBuilder.forAddress(new InetSocketAddress("127.0.0.1", 0))
                .addService(new FakeParserPlugin("profiled", "2.0"))
                .build()
                .start();
        parserPort = parserServer.getPort();
    }

    @AfterAll
    static void shutdown() {
        parserServer.shutdownNow();
    }

    /**
     * The store validates profiles fully, schema source included; the
     * registry does not consume it (the plugin contract is the schema),
     * so the fixture carries a well-formed reflection source.
     */
    static SchemaSource reflectionSource() {
        return SchemaSource.newBuilder()
                .setKind(SourceKind.SOURCE_KIND_REFLECTION)
                .setSourceRef("127.0.0.1:" + parserPort)
                .setDescriptorFingerprint("a".repeat(64))
                .setDescriptorArtifactRef("descriptors/parser-plugin-v1")
                .build();
    }

    @Test
    void aSavedProfileRegistersItsParserAndTheRegistryDialsIt() throws Exception {
        FileSystemServiceProfileRepository profiles =
                new FileSystemServiceProfileRepository(profileRoot.resolve("ok"));
        profiles.save(ServiceProfile.newBuilder()
                .setName("profiled")
                .setDescription("the fleet parser this profile registers")
                .setSchemaSource(reflectionSource())
                .addEndpoints(ServiceEndpoint.newBuilder()
                        .setName("local")
                        .setHost("127.0.0.1")
                        .setPort(parserPort)
                        .setTransport(Transport.TRANSPORT_PLAINTEXT))
                .build());

        try (ParserRegistry registry = ParserRegistry.fromProfiles(profiles, "local")) {
            assertThat(registry.parserNames()).containsExactly("profiled");
            ParserClient client = registry.lookup("profiled").orElseThrow();
            assertThat(client.info().getParserName()).isEqualTo("profiled");
            assertThat(client.info().getParserVersion()).isEqualTo("2.0");
        }
    }

    @Test
    void aProfileWithoutTheNamedEndpointIsRejectedByName() throws Exception {
        FileSystemServiceProfileRepository profiles =
                new FileSystemServiceProfileRepository(profileRoot.resolve("missing-endpoint"));
        profiles.save(ServiceProfile.newBuilder()
                .setName("lonely")
                .setSchemaSource(reflectionSource())
                .addEndpoints(ServiceEndpoint.newBuilder()
                        .setName("production")
                        .setHost("parser.internal")
                        .setPort(9090)
                        .setTransport(Transport.TRANSPORT_PLAINTEXT))
                .build());
        assertThatThrownBy(() -> ParserRegistry.fromProfiles(profiles, "local"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("lonely")
                .hasMessageContaining("local");
    }

    @Test
    void aTlsEndpointIsRejectedInsteadOfSilentlyDowngraded() throws Exception {
        FileSystemServiceProfileRepository profiles =
                new FileSystemServiceProfileRepository(profileRoot.resolve("tls"));
        profiles.save(ServiceProfile.newBuilder()
                .setName("secure-parser")
                .setSchemaSource(reflectionSource())
                .addEndpoints(ServiceEndpoint.newBuilder()
                        .setName("local")
                        .setHost("127.0.0.1")
                        .setPort(9090)
                        .setTransport(Transport.TRANSPORT_TLS))
                .build());
        assertThatThrownBy(() -> ParserRegistry.fromProfiles(profiles, "local"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("secure-parser")
                .hasMessageContaining("plaintext");
    }
}
