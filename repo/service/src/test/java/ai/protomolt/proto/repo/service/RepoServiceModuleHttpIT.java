package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.composer.Composer;
import ai.protomolt.proto.repo.container.ledger.LedgerConfig;
import ai.protomolt.proto.repo.v1.CreateDriveRequest;
import ai.protomolt.proto.repo.v1.DriveServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.MetadataUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The {@code repo} role as the platform mounts it: booted through {@link Composer} against
 * PostgreSQL and LocalStack S3, it binds its HTTP port next to gRPC, serving the document
 * upload route and the console bundle named by {@link RepositoryConsole#ENV_CONSOLE_DIR}.
 */
@Testcontainers
class RepoServiceModuleHttpIT {

    private static final String TOKEN = "synthetic-role-operator-key";
    private static final String ACCOUNT = "acct-role-http";
    private static final String DRIVE = "upload";
    private static final String INDEX = "<!doctype html><title>repository console</title>";

    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    @Container
    static final LocalStackContainer LOCALSTACK =
            new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.8"))
                    .withServices("s3");

    @TempDir Path temp;

    private final HttpClient client = HttpClient.newHttpClient();

    private static RepoServiceConfig config(int httpPort) {
        return new RepoServiceConfig(
                0,
                new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()),
                LOCALSTACK.getEndpoint().toString(),
                LOCALSTACK.getRegion(),
                LOCALSTACK.getAccessKey(),
                LOCALSTACK.getSecretKey(),
                "role-http-docs",
                httpPort, null, null, 0, 0L);
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static Composer.Node boot(RepoServiceModule module, Map<String, String> environment) {
        return Composer.emptyBuilder()
                .module(module)
                .environment(environment)
                .build()
                .boot(List.of(RepoServiceModule.ROLE));
    }

    @Test
    void theRoleServesUploadsAndTheConsoleOnItsHttpPort() throws Exception {
        Path bundle = Files.createDirectories(temp.resolve("dist"));
        Files.writeString(bundle.resolve("index.html"), INDEX);
        int httpPort = freePort();
        RepoServiceModule module = new RepoServiceModule(config(httpPort));
        try (Composer.Node node = boot(module, Map.of(
                "PROTOMOLT_API_TOKEN", TOKEN,
                RepositoryConsole.ENV_CONSOLE_DIR, bundle.toString()))) {
            assertThat(module.httpPort()).isEqualTo(httpPort);
            String base = "http://127.0.0.1:" + module.httpPort();

            HttpResponse<String> console = client.send(
                    HttpRequest.newBuilder(URI.create(base + "/console/repository")).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            assertThat(console.statusCode()).isEqualTo(200);
            assertThat(console.body()).isEqualTo(INDEX);

            Metadata operator = new Metadata();
            operator.put(Metadata.Key.of("api_token", Metadata.ASCII_STRING_MARSHALLER), TOKEN);
            ManagedChannel channel = NettyChannelBuilder.forAddress("127.0.0.1", module.grpcPort())
                    .usePlaintext()
                    .intercept(MetadataUtils.newAttachHeadersInterceptor(operator))
                    .build();
            try {
                DriveServiceGrpc.newBlockingStub(channel).createDrive(CreateDriveRequest.newBuilder()
                        .setName(DRIVE).setAccountId(ACCOUNT).build());
            } finally {
                channel.shutdownNow();
            }
            byte[] body = "role upload".getBytes(StandardCharsets.UTF_8);
            String sha256 = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(body));
            HttpResponse<String> upload = client.send(HttpRequest.newBuilder(URI.create(base
                            + UploadHttpServer.UPLOAD_PATH + "?account_id=" + ACCOUNT
                            + "&datasource_id=ds-role-http&drive=" + DRIVE
                            + "&filename=role.txt&doc_id=role-upload&operation_id=op-role-upload"))
                    .header("api_token", TOKEN)
                    .header("X-Content-Sha256", sha256)
                    .POST(HttpRequest.BodyPublishers.ofByteArray(body)).build(),
                    HttpResponse.BodyHandlers.ofString());
            assertThat(upload.statusCode()).as(upload.body()).isEqualTo(200);
            assertThat(upload.body()).contains("\"doc_id\":\"role-upload\"", "\"sha256\":\"" + sha256 + "\"");
        }
    }

    @Test
    void aDisabledHttpPortLeavesEveryHttpRouteUnbound() {
        RepoServiceModule module = new RepoServiceModule(config(0));
        try (Composer.Node node = boot(module, Map.of("PROTOMOLT_API_TOKEN", TOKEN))) {
            assertThat(module.grpcPort()).isPositive();
            assertThatThrownBy(module::httpPort)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining(RepoServiceConfig.ENV_HTTP_PORT);
        }
    }

    @Test
    void aConsoleDirectoryWithoutABundleFailsTheBoot() throws Exception {
        Path empty = Files.createDirectories(temp.resolve("empty"));
        RepoServiceModule module = new RepoServiceModule(config(freePort()));

        assertThatThrownBy(() -> boot(module, Map.of(
                "PROTOMOLT_API_TOKEN", TOKEN,
                RepositoryConsole.ENV_CONSOLE_DIR, empty.toString())))
                .hasStackTraceContaining(RepositoryConsole.ENV_CONSOLE_DIR)
                .hasStackTraceContaining("has no index.html");
    }
}
