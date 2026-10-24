package ai.protomolt.proto.repo.service;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Real HTTP credential boundary checks on the archive route, whose credential
 * enforcement is this server's own. The parts-based document route checks the
 * same credential inside the accession upload handler, covered there.
 */
class UploadHttpServerCredentialTest {

    private static final String TOKEN = "correct-horse-battery-staple";

    private UploadHttpServer server;
    private HttpClient client;
    private String base;

    @BeforeEach
    void start() {
        server = new UploadHttpServer(null, null, TOKEN, unimplementedArchive(),
                RepositoryConsole.notConfigured());
        base = "http://127.0.0.1:" + server.start(0);
        client = HttpClient.newHttpClient();
    }

    @AfterEach
    void stop() {
        client.close();
        server.close();
    }

    private HttpResponse<String> post(String path, String header, String value)
            throws Exception {
        HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(base + path))
                .POST(HttpRequest.BodyPublishers.ofInputStream(
                        () -> new ByteArrayInputStream(new byte[0])));
        if (header != null) {
            request.header(header, value);
        }
        return client.send(request.build(), HttpResponse.BodyHandlers.ofString());
    }

    @Test
    void aRequestWithoutACredentialIsRefused() throws Exception {
        HttpResponse<String> response = post(UploadHttpServer.ARCHIVE_UPLOAD_PATH, null, null);

        assertThat(response.statusCode()).isEqualTo(401);
        assertThat(response.body()).contains("api_token");
    }

    @Test
    void aRequestWithTheWrongCredentialIsRefusedWithoutEchoingIt() throws Exception {
        HttpResponse<String> response =
                post(UploadHttpServer.ARCHIVE_UPLOAD_PATH, "api_token", "not-the-token");

        assertThat(response.statusCode()).isEqualTo(401);
        assertThat(response.body()).doesNotContain("not-the-token");
    }

    @Test
    void theCredentialIsCheckedBeforeTheRouteIsMatched() throws Exception {
        // The handler is reached for any path under the registered prefix. An
        // unauthenticated probe of one must not learn whether it is a real route, so it
        // answers the same 401 as the route itself rather than 404.
        HttpResponse<String> response =
                post(UploadHttpServer.ARCHIVE_UPLOAD_PATH + "/probe", null, null);

        assertThat(response.statusCode()).isEqualTo(401);
    }

    @Test
    void bearerAuthorizationIsAccepted() throws Exception {
        // Authenticated requests reach archive validation before storage.
        HttpResponse<String> response =
                post(UploadHttpServer.ARCHIVE_UPLOAD_PATH, "Authorization", "Bearer " + TOKEN);

        assertThat(response.statusCode()).isEqualTo(400);
        assertThat(response.body()).contains("account_id is required");
    }

    @Test
    void missingOrBlankOperatorCredentialCannotOpenAListener() {
        for (String token : new String[]{null, "", "  "}) {
            assertThatThrownBy(() -> {
                try (var open = new UploadHttpServer(null, null, token, null,
                        RepositoryConsole.notConfigured())) { open.start(0); }
            }).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("API token");
        }
    }

    @Test
    void requestCallerResolverHandlesArchiveRouteUnauthenticatedAndOutageAndBindsCaller() throws Exception {
        var capturedCaller = new java.util.concurrent.atomic.AtomicReference<ai.protomolt.proto.repo.spi.RepositoryCaller>();
        io.grpc.Channel recordingChannel = new io.grpc.Channel() {
            @Override
            public <RequestT, ResponseT> io.grpc.ClientCall<RequestT, ResponseT> newCall(
                    io.grpc.MethodDescriptor<RequestT, ResponseT> methodDescriptor,
                    io.grpc.CallOptions callOptions) {
                capturedCaller.set(ArchiveCallerHandoff.REPOSITORY_CALLER.get());
                return new io.grpc.ClientCall<>() {
                    private Listener<ResponseT> listener;

                    @Override
                    public void start(Listener<ResponseT> responseListener, io.grpc.Metadata headers) {
                        this.listener = responseListener;
                    }

                    @Override
                    public void request(int numMessages) {}

                    @Override
                    public void cancel(String message, Throwable cause) {}

                    @Override
                    public void halfClose() {
                        listener.onClose(
                                io.grpc.Status.NOT_FOUND.withDescription("synthetic not found"),
                                new io.grpc.Metadata());
                    }

                    @Override
                    public void sendMessage(RequestT message) {}
                };
            }

            @Override
            public String authority() {
                return "test";
            }
        };
        var recording = new ai.protomolt.proto.repo.archive.DocumentArchive(
                recordingChannel,
                (accountId, archive) -> java.util.Optional.empty(),
                java.time.Clock.systemUTC());
        ai.protomolt.proto.authz.RequestCallerResolver resolver =
                ai.protomolt.proto.authz.RequestCallerResolver.bearer(TOKEN, token -> switch (token) {
                    case "session-alice" -> java.util.Optional.of(
                            ai.protomolt.proto.actions.Caller.scoped("alice", java.util.Set.of()));
                    case "store-down" -> throw new IllegalStateException("credential store down");
                    default -> java.util.Optional.empty();
                });
        try (var scopedServer = new UploadHttpServer(null, TOKEN, resolver,
                auth -> new ai.protomolt.proto.repo.spi.RepositoryCaller(
                        auth.caller().name(), false, java.util.Set.of("acct-1"),
                        java.util.Set.of()),
                recording,
                RepositoryConsole.notConfigured())) {
            String scopedBase = "http://127.0.0.1:" + scopedServer.start(0);
            String target = scopedBase + UploadHttpServer.ARCHIVE_UPLOAD_PATH
                    + "?account_id=acct-1&archive=arc&entry_id=e1&operation_id=op-1";

            // Unauthenticated -> 401
            HttpResponse<String> unauth = client.send(
                    HttpRequest.newBuilder(URI.create(target))
                            .POST(HttpRequest.BodyPublishers.ofByteArray(new byte[] {1}))
                            .build(),
                    HttpResponse.BodyHandlers.ofString());
            assertThat(unauth.statusCode()).isEqualTo(401);

            // Outage -> 503
            HttpResponse<String> outage = client.send(
                    HttpRequest.newBuilder(URI.create(target))
                            .header("Authorization", "Bearer store-down")
                            .POST(HttpRequest.BodyPublishers.ofByteArray(new byte[] {1}))
                            .build(),
                    HttpResponse.BodyHandlers.ofString());
            assertThat(outage.statusCode()).isEqualTo(503);

            // Authenticated scoped caller reaches the archive port with the bound RepositoryCaller
            HttpResponse<String> reached = client.send(
                    HttpRequest.newBuilder(URI.create(target))
                            .header("Authorization", "Bearer session-alice")
                            .POST(HttpRequest.BodyPublishers.ofByteArray(new byte[] {1}))
                            .build(),
                    HttpResponse.BodyHandlers.ofString());
            assertThat(reached.statusCode()).as(reached.body()).isEqualTo(404);
            assertThat(capturedCaller.get()).isNotNull();
            assertThat(capturedCaller.get().principalName()).isEqualTo("alice");
            assertThat(capturedCaller.get().processAuthority()).isFalse();
            assertThat(capturedCaller.get().accountIds()).containsExactly("acct-1");
        }
    }

    @Test
    void theOperatorTokenWithoutACallerSupplierUploadsAsTheOperator() throws Exception {
        var captured = new java.util.concurrent.atomic.AtomicReference<ai.protomolt.proto.repo.spi.RepositoryCaller>();
        try (var direct = new UploadHttpServer(null, null, TOKEN, recordingArchive(captured),
                RepositoryConsole.notConfigured())) {
            String target = "http://127.0.0.1:" + direct.start(0) + UploadHttpServer.ARCHIVE_UPLOAD_PATH
                    + "?account_id=acct-1&archive=arc&entry_id=e1";

            HttpResponse<String> ok = send(target, "api_token", TOKEN);

            assertThat(ok.statusCode()).as(ok.body()).isEqualTo(200);
            assertThat(captured.get().principalName()).isEqualTo("operator");
            assertThat(captured.get().processAuthority()).isTrue();
        }
    }

    @Test
    void aResolverWithoutAGatewayResolvesEachUploadAndExposesNoGatewayResolver() throws Exception {
        // The gateway-less host start: the operator bearer resolver and the account bindings,
        // without a Connect gateway on the port.
        var captured = new java.util.concurrent.atomic.AtomicReference<ai.protomolt.proto.repo.spi.RepositoryCaller>();
        var bound = new java.util.concurrent.atomic.AtomicReference<ai.protomolt.proto.authz.AuthenticatedCaller>();
        try (var direct = new UploadHttpServer(null, null, TOKEN, null, recordingArchive(captured), null,
                RepositoryConsole.notConfigured(), java.util.Map.of(), null,
                ai.protomolt.proto.authz.RequestCallerResolver.bearer(TOKEN),
                auth -> {
                    bound.set(auth);
                    return new ai.protomolt.proto.repo.spi.RepositoryCaller(
                            auth.caller().name(), auth.caller().unrestricted());
                })) {
            String target = "http://127.0.0.1:" + direct.start(0) + UploadHttpServer.ARCHIVE_UPLOAD_PATH
                    + "?account_id=acct-1&archive=arc&entry_id=e1";

            HttpResponse<String> other = send(target, "Authorization", "Bearer session-alice");
            assertThat(other.statusCode()).isEqualTo(401);
            assertThat(bound.get()).isNull();
            assertThat(captured.get()).isNull();

            HttpResponse<String> ok = send(target, "Authorization", "Bearer " + TOKEN);
            assertThat(ok.statusCode()).as(ok.body()).isEqualTo(200);
            assertThat(bound.get().caller().name()).isEqualTo("operator");
            assertThat(captured.get().principalName()).isEqualTo("operator");
            assertThat(captured.get().processAuthority()).isTrue();

            assertThatThrownBy(direct::callers).isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("no Connect gateway");
        }
    }

    private HttpResponse<String> send(String target, String header, String value) throws Exception {
        return client.send(HttpRequest.newBuilder(URI.create(target))
                        .header(header, value)
                        .POST(HttpRequest.BodyPublishers.ofByteArray(new byte[] {1}))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
    }

    /** An archive port that records the caller of each upload and accepts it. */
    static ai.protomolt.proto.repo.spi.ArchiveRepository recordingArchive(
            java.util.concurrent.atomic.AtomicReference<ai.protomolt.proto.repo.spi.RepositoryCaller> captured) {
        return (ai.protomolt.proto.repo.spi.ArchiveRepository) java.lang.reflect.Proxy.newProxyInstance(
                UploadHttpServerCredentialTest.class.getClassLoader(),
                new Class<?>[] { ai.protomolt.proto.repo.spi.ArchiveRepository.class },
                (proxy, method, args) -> {
                    captured.set((ai.protomolt.proto.repo.spi.RepositoryCaller) args[0]);
                    ((java.io.InputStream) args[9]).readAllBytes();
                    return new ai.protomolt.proto.repo.spi.ArchiveRepository.UploadResult(
                            "entry-uuid", 1L, "sha", 1L, "obj", "root", false);
                });
    }

    /** Credential and validation failures fire before the archive port is ever called. */
    static ai.protomolt.proto.repo.archive.DocumentArchive unimplementedArchive() {
        io.grpc.Channel unimplemented = new io.grpc.Channel() {
            @Override
            public <RequestT, ResponseT> io.grpc.ClientCall<RequestT, ResponseT> newCall(
                    io.grpc.MethodDescriptor<RequestT, ResponseT> methodDescriptor,
                    io.grpc.CallOptions callOptions) {
                throw new UnsupportedOperationException(methodDescriptor.getFullMethodName());
            }

            @Override
            public String authority() {
                return "unimplemented";
            }
        };
        return new ai.protomolt.proto.repo.archive.DocumentArchive(
                unimplemented,
                (accountId, archive) -> java.util.Optional.empty(),
                java.time.Clock.systemUTC());
    }
}
