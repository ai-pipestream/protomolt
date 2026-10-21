package ai.protomolt.proto.repo.service;

import static org.assertj.core.api.Assertions.assertThat;

import ai.protomolt.proto.authz.AccountBinding;
import ai.protomolt.proto.authz.AccountBindingResolver;
import ai.protomolt.proto.authz.AccountBindings;
import ai.protomolt.proto.authz.FakeOpenIdProvider;
import ai.protomolt.proto.authz.OidcConsoleSessions;
import ai.protomolt.proto.repo.container.ledger.LedgerConfig;
import ai.protomolt.proto.repo.v1.CreateDriveRequest;
import ai.protomolt.proto.repo.v1.DriveServiceGrpc;
import ai.protomolt.proto.repo.v1.DriveType;
import ai.protomolt.proto.repo.v2.ItemError;
import ai.protomolt.proto.repo.v2.RejectionReason;
import ai.protomolt.proto.repo.v2.UploadDocumentResponse;
import com.sun.net.httpserver.HttpServer;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.AbstractStub;
import io.grpc.stub.MetadataUtils;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.UnaryOperator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * The document upload route on a repository host that mounts the Connect gateway: it resolves
 * each request's caller with the host's resolver, so the operator token uploads with process
 * authority, a console session uploads as the scoped caller its principal is bound as, and a
 * request with neither is unauthenticated. Real PostgreSQL and LocalStack S3, the production
 * {@link RepoServices} composition, and a fake OpenID provider for the session.
 */
@Testcontainers
class RepoSessionUploadIT {

    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    @Container
    static final LocalStackContainer LOCALSTACK =
            new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.8"))
                    .withServices("s3");

    private static final String OPERATOR = "synthetic-session-upload-operator-token";
    private static final String BOUND = "session-upload-bound";
    private static final String UNBOUND = "session-upload-unbound";
    private static final String DATASOURCE = "ds-session";
    private static final String DRIVE = "intake";
    private static final byte[] BODY = "bytes a console session uploads".getBytes(StandardCharsets.UTF_8);

    static RepoServices services;
    static ManagedChannel channel;
    static FakeOpenIdProvider provider;
    static HttpServer signInHost;
    static String base;
    static String session;
    static final HttpClient CLIENT = HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NEVER).build();

    @BeforeAll
    static void boot() throws Exception {
        RepoServiceConfig config = new RepoServiceConfig(
                0,
                new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()),
                LOCALSTACK.getEndpoint().toString(),
                LOCALSTACK.getRegion(),
                LOCALSTACK.getAccessKey(),
                LOCALSTACK.getSecretKey(),
                "it-session-upload",
                0,
                null, null, 0, 0L)
                .withManagedStorage(new ManagedStoragePolicy("session-upload-original", "session-upload-realm", true));
        // The fake provider signs "alice" in; she is bound to one account only.
        AccountBindings bindings = AccountBindings.newBuilder()
                .addBindings(AccountBinding.newBuilder().setPrincipal("alice").addAccountIds(BOUND))
                .build();
        services = RepoServices.build(config).withAccountBindings(AccountBindingResolver.of(bindings));

        // The session store lives behind a sign-in host of its own; the repository host
        // resolves the session cookie through the same store, as one process would.
        provider = new FakeOpenIdProvider("protomolt-console", "console-secret", Clock.systemUTC());
        signInHost = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        String signInBase = "http://127.0.0.1:" + signInHost.getAddress().getPort();
        OidcConsoleSessions sessions = new OidcConsoleSessions(
                provider.settings(URI.create(signInBase + ConsoleSignInHandler.PATH + "/callback")),
                HttpClient.newHttpClient(), Clock.systemUTC());
        signInHost.createContext(ConsoleSignInHandler.PATH, ConsoleSignInHandler.over(sessions, caller -> List.of(BOUND)));
        signInHost.start();

        int grpcPort = services.startNetty(0, OPERATOR, null).getPort();
        UploadHttpServer http = services.startHttp(0, OPERATOR, RepositoryConsole.notConfigured(), Map.of(),
                RepoServices.requestCallers(OPERATOR, null, Optional.of(sessions)));
        base = "http://127.0.0.1:" + http.port();
        channel = NettyChannelBuilder.forAddress("localhost", grpcPort).usePlaintext().build();
        for (String account : new String[] {BOUND, UNBOUND}) {
            as(OPERATOR, DriveServiceGrpc.newBlockingStub(channel)).createDrive(CreateDriveRequest.newBuilder()
                    .setName(DRIVE).setAccountId(account).setDriveType(DriveType.DRIVE_TYPE_INTAKE).build());
        }
        session = signIn(signInBase);
    }

    @AfterAll
    static void tearDown() {
        if (channel != null) channel.shutdownNow();
        if (services != null) services.close();
        if (signInHost != null) signInHost.stop(0);
        if (provider != null) provider.close();
    }

    @Test
    void aRequestWithNeitherBearerNorSessionIsUnauthenticated() throws Exception {
        HttpResponse<byte[]> anonymous = upload(BOUND, "anon", builder -> builder);
        assertThat(anonymous.statusCode()).isEqualTo(401);
        assertThat(ItemError.parseFrom(anonymous.body()).getCode()).isEqualTo(16);

        HttpResponse<byte[]> stale = upload(BOUND, "stale",
                builder -> builder.header("Cookie", OidcConsoleSessions.COOKIE + "=not-a-live-session"));
        assertThat(stale.statusCode()).isEqualTo(401);
    }

    @Test
    void theOperatorTokenStillUploadsWithProcessAuthority() throws Exception {
        HttpResponse<byte[]> saved = upload(UNBOUND, "operator", builder -> builder.header("api_token", OPERATOR));
        assertThat(saved.statusCode()).isEqualTo(200);
        assertThat(UploadDocumentResponse.parseFrom(saved.body()).getSaved().getAccession().getRevision()).isEqualTo(1);
    }

    @Test
    void aSessionIsRefusedForAnAccountItsPrincipalIsNotBoundTo() throws Exception {
        HttpResponse<byte[]> refused = upload(UNBOUND, "unbound", this::withSession);
        assertThat(refused.statusCode()).isEqualTo(403);
        ItemError error = ItemError.parseFrom(refused.body());
        assertThat(error.getCode()).isEqualTo(7);
        assertThat(error.getMessage()).contains("not bound to account '" + UNBOUND + "'");
    }

    @Test
    void aSessionOnItsBoundAccountSavesAsItsScopedCaller() throws Exception {
        // The session reaches the save as a scoped caller bound to the account. A scoped
        // caller saves only through a registered credential with a writer grant, and a
        // console session carries none, so the save is refused by the writer check, not by
        // the account check and not as the operator.
        HttpResponse<byte[]> answer = upload(BOUND, "bound", this::withSession);
        assertThat(answer.statusCode()).isEqualTo(403);
        ItemError error = ItemError.parseFrom(answer.body());
        assertThat(error.getRejection().getReason()).isEqualTo(RejectionReason.REJECTION_REASON_WRITE_DENIED);
        assertThat(error.getMessage()).contains("credential-bound caller");
    }

    private HttpRequest.Builder withSession(HttpRequest.Builder builder) {
        return builder.header("Cookie", OidcConsoleSessions.COOKIE + "=" + session);
    }

    private static HttpResponse<byte[]> upload(String account, String name,
            UnaryOperator<HttpRequest.Builder> credential) throws Exception {
        HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(base + UploadHttpServer.UPLOAD_PATH
                        + "?account_id=" + account + "&datasource_id=" + DATASOURCE + "&drive=" + DRIVE
                        + "&filename=" + name + ".txt&doc_id=doc-" + name + "&operation_id=op-" + name))
                .header("Accept", "application/proto")
                .POST(HttpRequest.BodyPublishers.ofByteArray(BODY));
        return CLIENT.send(credential.apply(request).build(), HttpResponse.BodyHandlers.ofByteArray());
    }

    /** Signs alice in through the fake provider and returns the session cookie value. */
    private static String signIn(String signInBase) throws Exception {
        HttpResponse<byte[]> login = CLIENT.send(HttpRequest.newBuilder(
                URI.create(signInBase + ConsoleSignInHandler.PATH + "/login?return=/console/repository")).GET().build(),
                HttpResponse.BodyHandlers.ofByteArray());
        assertThat(login.statusCode()).isEqualTo(302);
        String state = cookie(login, OidcConsoleSessions.LOGIN_COOKIE);
        HttpResponse<byte[]> provided = CLIENT.send(HttpRequest.newBuilder(
                URI.create(login.headers().firstValue("Location").orElseThrow())).GET().build(),
                HttpResponse.BodyHandlers.ofByteArray());
        HttpResponse<byte[]> callback = CLIENT.send(HttpRequest.newBuilder(
                URI.create(provided.headers().firstValue("Location").orElseThrow()))
                .header("Cookie", OidcConsoleSessions.LOGIN_COOKIE + "=" + state).GET().build(),
                HttpResponse.BodyHandlers.ofByteArray());
        assertThat(callback.statusCode()).isEqualTo(302);
        return cookie(callback, OidcConsoleSessions.COOKIE);
    }

    private static String cookie(HttpResponse<?> response, String name) {
        return response.headers().allValues("Set-Cookie").stream()
                .filter(header -> header.startsWith(name + "="))
                .map(header -> header.substring(name.length() + 1, header.indexOf(';')))
                .findFirst().orElseThrow();
    }

    private static <S extends AbstractStub<S>> S as(String credential, S stub) {
        Metadata headers = new Metadata();
        headers.put(Metadata.Key.of("api_token", Metadata.ASCII_STRING_MARSHALLER), credential);
        return stub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers));
    }
}
