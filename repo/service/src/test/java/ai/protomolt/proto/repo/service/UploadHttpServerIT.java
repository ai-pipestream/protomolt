package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.repo.container.ledger.LedgerConfig;
import ai.protomolt.proto.repo.v1.CreateDriveRequest;
import ai.protomolt.proto.repo.v1.DriveServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.MessageDigest;
import java.util.HexFormat;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * End-to-end integration test of the streaming HTTP upload route against
 * REAL infrastructure: testcontainers PostgreSQL + LocalStack S3, the full
 * service stack booted through {@link RepoServices}, and a real JDK
 * {@code HttpServer} on an ephemeral port driven by {@code java.net.http}.
 * The parts-based v2 handler owns the document route in every profile.
 */
@Testcontainers
class UploadHttpServerIT {

    private static final String ACCOUNT = "acct-http";
    private static final String DATASOURCE = "ds-http";
    private static final String DRIVE = "upload";

    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    @Container
    static final LocalStackContainer LOCALSTACK =
            new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.8"))
                    .withServices("s3");

    static RepoServices services;
    static ManagedChannel channel;
    static UploadHttpServer http;
    static RepoServiceConfig config;
    static HttpClient client;

    @BeforeAll
    static void boot() {
        config = new RepoServiceConfig(
                0,
                new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()),
                LOCALSTACK.getEndpoint().toString(),
                LOCALSTACK.getRegion(),
                LOCALSTACK.getAccessKey(),
                LOCALSTACK.getSecretKey(),
                "it-http-docs",
                0, null, null, 0, 0L);
        services = RepoServices.build(config.withManagedStorage(new ManagedStoragePolicy("http-test-v1", "test-realm", true)));
        services.startInProcess("it-http");
        http = services.startHttp(0, "synthetic-http-operator-key"); // ephemeral port
        channel = InProcessChannelBuilder.forName("it-http").build();
        DriveServiceGrpc.DriveServiceBlockingStub drives = DriveServiceGrpc.newBlockingStub(channel);
        drives.createDrive(CreateDriveRequest.newBuilder()
                .setName(DRIVE)
                .setAccountId(ACCOUNT)
                .build());
        client = HttpClient.newHttpClient();
    }

    @AfterAll
    static void tearDown() {
        channel.shutdownNow();
        services.close();
    }

    @Test void sameGenerationCannotBeReboundToAnotherStorageRealm() {
        assertThatThrownBy(() -> RepoServices.build(config.withManagedStorage(
                new ManagedStoragePolicy("http-test-v1", "different-realm", true))))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("another physical profile");
    }

    @Test void defaultProfileServesUploadsThroughTheV2Handler() throws Exception {
        try (var disabled = RepoServices.build(config)) {
            var endpoint = disabled.startHttp(0, "synthetic-http-operator-key");
            var body = new byte[] {1};
            String sha256 = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(body));
            var request = HttpRequest.newBuilder(URI.create("http://localhost:" + endpoint.port()
                    + UploadHttpServer.UPLOAD_PATH + "?account_id=" + ACCOUNT + "&datasource_id=" + DATASOURCE
                    + "&drive=" + DRIVE + "&filename=disabled.bin&doc_id=disabled-upload"
                    + "&operation_id=op-disabled-upload"))
                    .header("api_token", "synthetic-http-operator-key")
                    .header("X-Content-Sha256", sha256)
                    .POST(HttpRequest.BodyPublishers.ofByteArray(body)).build();
            var response = client.send(request, HttpResponse.BodyHandlers.ofString());
            // The parts-based handler owns the route in the default profile.
            assertThat(response.statusCode()).as(response.body()).isEqualTo(200);
            assertThat(response.body()).contains("\"doc_id\":\"disabled-upload\"", "\"revision\":1",
                    "\"replayed\":false", "\"sha256\":\"" + sha256 + "\"");
        }
        // A fresh cold host (empty v2Stacks cache) resolves the persisted stackId by drive primary key and reads back.
        try (var coldHost = RepoServices.build(config)) {
            coldHost.startInProcess("it-http-default-cold");
            var v2Channel = InProcessChannelBuilder.forName("it-http-default-cold").build();
            try {
                var readBack = ai.protomolt.proto.repo.v2.DocumentServiceGrpc.newBlockingStub(v2Channel)
                        .getDocument(ai.protomolt.proto.repo.v2.GetDocumentRequest.newBuilder()
                                .setAddress(ai.protomolt.proto.repo.v1.NodeAddress.newBuilder()
                                        .setAccountId(ACCOUNT).setDocId("disabled-upload")
                                        .setGraphId("intake:" + ACCOUNT).setGraphAddressId(DATASOURCE))
                                .build());
                assertThat(readBack.getManifest().getRevision()).isEqualTo(1);
            } finally {
                v2Channel.shutdownNow();
            }
        }
    }
}
