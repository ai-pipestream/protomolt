package ai.protomolt.proto.repo.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.protomolt.proto.authz.AccountBinding;
import ai.protomolt.proto.authz.AccountBindingResolver;
import ai.protomolt.proto.authz.AccountBindings;
import ai.protomolt.proto.authz.AclIdentity;
import ai.protomolt.proto.authz.AuthenticatedCaller;
import ai.protomolt.proto.authz.ConsoleSessionStatus;
import ai.protomolt.proto.authz.MutableClock;
import ai.protomolt.proto.authz.OidcConsoleSessions;
import ai.protomolt.proto.authz.OidcConsoleSettings;
import ai.protomolt.proto.authz.RequestHeaders;
import ai.protomolt.proto.authz.grpc.CallerContexts;
import ai.protomolt.proto.composer.Composer;
import ai.protomolt.proto.repo.container.ledger.LedgerConfig;
import ai.protomolt.proto.repo.v1.Access;
import ai.protomolt.proto.repo.v1.AccessRule;
import ai.protomolt.proto.repo.v1.CreateDriveRequest;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.DocumentSecurity;
import ai.protomolt.proto.repo.v1.DriveServiceGrpc;
import ai.protomolt.proto.repo.v1.DriveType;
import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v1.OwnershipContext;
import ai.protomolt.proto.repo.v1.SearchMetadata;
import ai.protomolt.proto.repo.v2.DocumentPart;
import ai.protomolt.proto.repo.v2.DocumentServiceGrpc;
import ai.protomolt.proto.repo.v2.DocumentTarget;
import ai.protomolt.proto.repo.v2.ExpectedRevision;
import ai.protomolt.proto.repo.v2.IntakeOrigin;
import ai.protomolt.proto.repo.v2.ListDocumentsRequest;
import ai.protomolt.proto.repo.v2.PartSelector;
import ai.protomolt.proto.repo.v2.PartWrite;
import ai.protomolt.proto.repo.v2.SaveDocumentRequest;
import ai.protomolt.proto.repo.v2.SaveOwner;
import ai.protomolt.proto.repo.v2.WriteAction;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.AbstractStub;
import io.grpc.stub.MetadataUtils;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Console sign-in against a real OpenID provider: Keycloak in a container with an imported
 * realm, the repository host on PostgreSQL and LocalStack S3, the production sign-in routes,
 * no mocks. A browser is played by hand: the login redirect, Keycloak's login form, the
 * callback with the login cookie, then the session cookie on the status route. The session's
 * caller reaches the document service the way the Connect gateway will dispatch it, under
 * the gRPC context keys, and lists only its bound account's readable rows; another account
 * refuses it by name, an unbound user reaches nothing, and an expired session is refused
 * everywhere. The role boots the same sign-in from the environment, and a wrong issuer or a
 * partial configuration fails the boot by name. An integration test: it needs Docker.
 */
@Testcontainers
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ConsoleSignInKeycloakIT {

    private static final String OPERATOR = "synthetic-operator-token";
    private static final String ACCOUNT_A = "console-account-a";
    private static final String ACCOUNT_B = "console-account-b";
    private static final String DATASOURCE = "ds-console";
    private static final String ALICE_UPN = "alice@example.com";
    private static final String RETURN = "/console/repository";
    private static final String REALM = "protomolt";
    private static final String CLIENT_ID = "protomolt-console";
    private static final String CLIENT_SECRET = "console-secret";
    private static final Duration TTL = Duration.ofHours(1);
    private static final Metadata.Key<String> COOKIE_HEADER =
            Metadata.Key.of("cookie", Metadata.ASCII_STRING_MARSHALLER);

    /** The repository HTTP port, chosen before Keycloak starts: the realm pins the callback to it. */
    private static final int HTTP_PORT = freePort();

    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    @Container
    static final LocalStackContainer LOCALSTACK =
            new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.8"))
                    .withServices("s3");

    @Container
    static final GenericContainer<?> KEYCLOAK = new GenericContainer<>("quay.io/keycloak/keycloak:26.4")
            .withCommand("start-dev", "--import-realm")
            .withEnv("KC_BOOTSTRAP_ADMIN_USERNAME", "admin")
            .withEnv("KC_BOOTSTRAP_ADMIN_PASSWORD", "admin-password")
            .withCopyToContainer(Transferable.of(realm(callback(HTTP_PORT))),
                    "/opt/keycloak/data/import/" + REALM + "-realm.json")
            .withExposedPorts(8080)
            .waitingFor(Wait.forHttp("/realms/" + REALM + "/.well-known/openid-configuration")
                    .forPort(8080).withStartupTimeout(Duration.ofMinutes(4)));

    static final MutableClock CLOCK = new MutableClock(Instant.now());
    static RepoServices services;
    static OidcConsoleSessions sessions;
    static ManagedChannel operatorChannel;
    static Server gateway;
    static ManagedChannel gatewayChannel;
    static String base;

    private static int freePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private static String callback(int httpPort) {
        return "http://127.0.0.1:" + httpPort + ConsoleSignInHandler.PATH + "/callback";
    }

    private static String realm(String callbackUri) {
        try (var in = ConsoleSignInKeycloakIT.class.getResourceAsStream("keycloak-realm.json")) {
            return new String(Objects.requireNonNull(in, "keycloak-realm.json").readAllBytes(),
                    StandardCharsets.UTF_8).replace("${CALLBACK}", callbackUri);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Keycloak's address for the played browser: the loopback IP rather than the container
     * host name, because the JDK cookie store drops host-only cookies for a dotless host
     * such as localhost, and Keycloak's login form needs those cookies on the post.
     */
    private static String issuer() {
        return "http://127.0.0.1:" + KEYCLOAK.getMappedPort(8080) + "/realms/" + REALM;
    }

    private static RepoServiceConfig config(String bucketBase, int httpPort) {
        return new RepoServiceConfig(
                0,
                new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()),
                LOCALSTACK.getEndpoint().toString(),
                LOCALSTACK.getRegion(),
                LOCALSTACK.getAccessKey(),
                LOCALSTACK.getSecretKey(),
                bucketBase,
                httpPort, null, null, 0, 0L);
    }

    @BeforeAll
    static void boot() throws IOException {
        var bindings = AccountBindings.newBuilder()
                .addBindings(AccountBinding.newBuilder().setPrincipal("alice").addAccountIds(ACCOUNT_A)
                        .addAclIdentities(AclIdentity.newBuilder()
                                .setIdentityType("user-principal-name").setIdentity(ALICE_UPN)))
                .build();
        services = RepoServices.build(config("console-it", HTTP_PORT))
                .withAccountBindings(AccountBindingResolver.of(bindings));
        var settings = new OidcConsoleSettings(URI.create(issuer()), CLIENT_ID, CLIENT_SECRET,
                URI.create(callback(HTTP_PORT)), TTL);
        sessions = new OidcConsoleSessions(settings, HttpClient.newHttpClient(), CLOCK);
        int grpcPort = services.startNetty(0, OPERATOR, null).getPort();
        services.startHttp(HTTP_PORT, OPERATOR, RepositoryConsole.notConfigured(),
                services.consoleSignInRoutes(Optional.of(sessions)));
        base = "http://127.0.0.1:" + HTTP_PORT;

        // The gateway's dispatch, as #524 will do it: the session resolver answers the
        // request's cookie, and the call runs under the caller context keys.
        var gatewayBuilder = InProcessServerBuilder.forName("console-sign-in-gateway")
                .intercept(new SessionDispatch());
        services.services().forEach(gatewayBuilder::addService);
        gateway = gatewayBuilder.build().start();
        gatewayChannel = InProcessChannelBuilder.forName("console-sign-in-gateway").build();

        operatorChannel = NettyChannelBuilder.forAddress("localhost", grpcPort).usePlaintext().build();
        var drives = as(OPERATOR, DriveServiceGrpc.newBlockingStub(operatorChannel));
        var documents = as(OPERATOR, DocumentServiceGrpc.newBlockingStub(operatorChannel));
        for (String account : List.of(ACCOUNT_A, ACCOUNT_B)) {
            drives.createDrive(CreateDriveRequest.newBuilder().setName("intake")
                    .setAccountId(account).setDriveType(DriveType.DRIVE_TYPE_INTAKE).build());
        }
        documents.saveDocument(save(ACCOUNT_A, "doc-for-alice", rule("user-principal-name", ALICE_UPN)));
        documents.saveDocument(save(ACCOUNT_A, "doc-for-someone-else", rule("user-principal-name", "dave@example.com")));
        documents.saveDocument(save(ACCOUNT_B, "doc-public-in-b", rule("public", "public")));
    }

    @AfterAll
    static void tearDown() {
        if (gatewayChannel != null) gatewayChannel.shutdownNow();
        if (gateway != null) gateway.shutdownNow();
        if (operatorChannel != null) operatorChannel.shutdownNow();
        if (services != null) services.close();
    }

    /** The gateway stand-in: the request's cookie resolves through the session store, or 401. */
    private static final class SessionDispatch implements ServerInterceptor {
        @Override
        public <Q, R> ServerCall.Listener<Q> interceptCall(ServerCall<Q, R> call, Metadata headers,
                                                           ServerCallHandler<Q, R> next) {
            String cookie = headers.get(COOKIE_HEADER);
            Optional<AuthenticatedCaller> caller = sessions.resolve(RequestHeaders.of(
                    cookie == null ? Map.of() : Map.of("Cookie", List.of(cookie))));
            if (caller.isEmpty()) {
                call.close(Status.UNAUTHENTICATED.withDescription("no live console session"), new Metadata());
                return new ServerCall.Listener<>() { };
            }
            return Contexts.interceptCall(Context.current()
                    .withValues(CallerContexts.CALLER, caller.get().caller(),
                            CallerContexts.AUTHENTICATED_CALLER, caller.get()), call, headers, next);
        }
    }

    // ------------------------------------------------------------------ the browser

    /** Signs {@code username} in through Keycloak's login page and returns the session cookie value. */
    private static String signIn(String username, String password) throws Exception {
        HttpClient host = HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NEVER).build();
        HttpResponse<String> login = host.send(HttpRequest.newBuilder(
                URI.create(base + ConsoleSignInHandler.PATH + "/login?return=" + RETURN)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(login.statusCode()).as(login.body()).isEqualTo(302);
        String loginCookie = cookieValue(login, OidcConsoleSessions.LOGIN_COOKIE);
        String authorize = login.headers().firstValue("Location").orElseThrow();
        assertThat(authorize).startsWith(issuer() + "/protocol/openid-connect/auth?");

        // Keycloak's cookies travel between the login page and the form post. A map stands in
        // for the browser's store: Keycloak marks them Secure even over plain HTTP, which a
        // browser accepts on loopback and the JDK cookie store does not.
        Map<String, String> jar = new LinkedHashMap<>();
        HttpResponse<String> page = host.send(withCookies(HttpRequest.newBuilder(URI.create(authorize)).GET(), jar),
                HttpResponse.BodyHandlers.ofString());
        for (int redirect = 0; page.statusCode() == 302 && redirect < 5; redirect++) {
            remember(page, jar);
            page = host.send(withCookies(HttpRequest.newBuilder(
                    URI.create(page.headers().firstValue("Location").orElseThrow())).GET(), jar),
                    HttpResponse.BodyHandlers.ofString());
        }
        remember(page, jar);
        assertThat(page.statusCode()).isEqualTo(200);
        String action = formAction(page.body());
        String form = "username=" + URLEncoder.encode(username, StandardCharsets.UTF_8)
                + "&password=" + URLEncoder.encode(password, StandardCharsets.UTF_8) + "&credentialId=";
        HttpResponse<String> authenticated = host.send(withCookies(HttpRequest.newBuilder(URI.create(action))
                .header("Content-Type", "application/x-www-form-urlencoded")
                .POST(HttpRequest.BodyPublishers.ofString(form)), jar), HttpResponse.BodyHandlers.ofString());
        assertThat(authenticated.statusCode()).as(authenticated.body()).isEqualTo(302);
        String callbackUrl = authenticated.headers().firstValue("Location").orElseThrow();
        assertThat(callbackUrl).startsWith(callback(HTTP_PORT) + "?");

        HttpResponse<String> callback = host.send(HttpRequest.newBuilder(URI.create(callbackUrl))
                .header("Cookie", OidcConsoleSessions.LOGIN_COOKIE + "=" + loginCookie).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(callback.statusCode()).as(callback.body()).isEqualTo(302);
        assertThat(callback.headers().firstValue("Location")).contains(RETURN);
        return cookieValue(callback, OidcConsoleSessions.COOKIE);
    }

    /** Stores the cookies {@code response} set, by name. */
    private static void remember(HttpResponse<?> response, Map<String, String> jar) {
        for (String header : response.headers().allValues("Set-Cookie")) {
            int semicolon = header.indexOf(';');
            String assignment = semicolon < 0 ? header : header.substring(0, semicolon);
            int equals = assignment.indexOf('=');
            if (equals > 0) {
                jar.put(assignment.substring(0, equals).trim(), assignment.substring(equals + 1).trim());
            }
        }
    }

    /** Sends the jar's cookies on {@code request}. */
    private static HttpRequest withCookies(HttpRequest.Builder request, Map<String, String> jar) {
        if (!jar.isEmpty()) {
            StringBuilder cookie = new StringBuilder();
            jar.forEach((name, value) -> cookie.append(cookie.isEmpty() ? "" : "; ").append(name).append('=').append(value));
            request.header("Cookie", cookie.toString());
        }
        return request.build();
    }

    /** The login form's action URL, scanned out of Keycloak's page. */
    private static String formAction(String html) {
        int form = html.indexOf("kc-form-login");
        assertThat(form).as("Keycloak login form").isPositive();
        int action = html.indexOf("action=\"", form);
        assertThat(action).as("login form action").isPositive();
        int end = html.indexOf('"', action + 8);
        return html.substring(action + 8, end).replace("&amp;", "&");
    }

    private static String cookieValue(HttpResponse<?> response, String name) {
        return response.headers().allValues("Set-Cookie").stream()
                .filter(header -> header.startsWith(name + "="))
                .map(header -> header.substring(name.length() + 1, header.indexOf(';')))
                .findFirst().orElseThrow(() -> new AssertionError("no " + name + " cookie: "
                        + response.headers().allValues("Set-Cookie")));
    }

    private static HttpResponse<byte[]> status(String sessionCookie) throws Exception {
        HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(base + ConsoleSignInHandler.PATH)).GET();
        if (sessionCookie != null) {
            request.header("Cookie", OidcConsoleSessions.COOKIE + "=" + sessionCookie);
        }
        return HttpClient.newHttpClient().send(request.build(), HttpResponse.BodyHandlers.ofByteArray());
    }

    private static DocumentServiceGrpc.DocumentServiceBlockingStub throughGateway(String sessionCookie) {
        var stub = DocumentServiceGrpc.newBlockingStub(gatewayChannel);
        if (sessionCookie == null) {
            return stub;
        }
        var headers = new Metadata();
        headers.put(COOKIE_HEADER, OidcConsoleSessions.COOKIE + "=" + sessionCookie);
        return stub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers));
    }

    // ------------------------------------------------------------------ the tests

    @Test
    @Order(1)
    void loginIssuesASessionBoundToTheIntrospectedPrincipal() throws Exception {
        assertThat(status(null).statusCode()).isEqualTo(401);

        String session = signIn("alice", "alice-password");
        HttpResponse<byte[]> status = status(session);
        assertThat(status.statusCode()).isEqualTo(200);
        ConsoleSessionStatus parsed = ConsoleSessionStatus.parseFrom(status.body());
        assertThat(parsed.getAuthenticated()).isTrue();
        assertThat(parsed.getPrincipal()).isEqualTo("alice");
        assertThat(parsed.getAccountIdsList()).containsExactly(ACCOUNT_A);
        assertThat(parsed.getExpiresAt().getSeconds()).isEqualTo(CLOCK.instant().plus(TTL).getEpochSecond());
    }

    @Test
    @Order(2)
    void theSessionCallerListsOnlyItsBoundAccountsReadableRowsAndIsRefusedElsewhere() throws Exception {
        String session = signIn("alice", "alice-password");
        assertThat(throughGateway(session).listDocuments(list(ACCOUNT_A)).getDocumentsList())
                .extracting(summary -> summary.getAddress().getDocId())
                .containsExactly("doc-for-alice");
        assertDenied(() -> throughGateway(session).listDocuments(list(ACCOUNT_B)), ACCOUNT_B);
        assertCode(() -> throughGateway(null).listDocuments(list(ACCOUNT_A)), Status.Code.UNAUTHENTICATED);
        assertCode(() -> throughGateway("never-issued").listDocuments(list(ACCOUNT_A)), Status.Code.UNAUTHENTICATED);
    }

    @Test
    @Order(3)
    void anUnboundUserSignsInButReachesNoAccount() throws Exception {
        String session = signIn("carol", "carol-password");
        ConsoleSessionStatus parsed = ConsoleSessionStatus.parseFrom(status(session).body());
        assertThat(parsed.getPrincipal()).isEqualTo("carol");
        assertThat(parsed.getAccountIdsList()).isEmpty();
        assertDenied(() -> throughGateway(session).listDocuments(list(ACCOUNT_A)), "bound to no account");
    }

    @Test
    @Order(4)
    void aWrongPasswordNeverReachesTheCallback() throws Exception {
        assertThatThrownBy(() -> signIn("alice", "not-her-password"))
                .isInstanceOf(AssertionError.class);
        assertThat(status(null).statusCode()).isEqualTo(401);
    }

    @Test
    @Order(5)
    void theRoleBootsSignInFromTheEnvironmentAndRefusesAWrongOrPartialOne() throws Exception {
        int httpPort = freePort();
        Map<String, String> environment = Map.of(
                "PROTOMOLT_API_TOKEN", OPERATOR,
                OidcConsoleSettings.ENV_ISSUER, issuer(),
                OidcConsoleSettings.ENV_CLIENT_ID, CLIENT_ID,
                OidcConsoleSettings.ENV_CLIENT_SECRET, CLIENT_SECRET,
                OidcConsoleSettings.ENV_REDIRECT_URI, callback(httpPort),
                OidcConsoleSettings.ENV_SESSION_TTL, "PT30M");
        RepoServiceModule module = new RepoServiceModule(config("console-role-it", httpPort));
        try (Composer.Node node = Composer.emptyBuilder().module(module).environment(environment).build()
                .boot(List.of(RepoServiceModule.ROLE))) {
            HttpClient client = HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NEVER).build();
            String roleBase = "http://127.0.0.1:" + module.httpPort();
            HttpResponse<byte[]> anonymous = client.send(HttpRequest.newBuilder(
                    URI.create(roleBase + ConsoleSignInHandler.PATH)).GET().build(), HttpResponse.BodyHandlers.ofByteArray());
            assertThat(anonymous.statusCode()).isEqualTo(401);
            assertThat(ConsoleSessionStatus.parseFrom(anonymous.body()).getAuthenticated()).isFalse();
            HttpResponse<String> login = client.send(HttpRequest.newBuilder(
                    URI.create(roleBase + ConsoleSignInHandler.PATH + "/login?return=" + RETURN)).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            assertThat(login.statusCode()).isEqualTo(302);
            assertThat(login.headers().firstValue("Location").orElseThrow())
                    .startsWith(issuer() + "/protocol/openid-connect/auth?")
                    .contains("redirect_uri=" + URLEncoder.encode(callback(httpPort), StandardCharsets.UTF_8));
        }

        Map<String, String> wrongIssuer = new java.util.HashMap<>(environment);
        wrongIssuer.put(OidcConsoleSettings.ENV_ISSUER, issuer() + "-missing");
        assertThatThrownBy(() -> Composer.emptyBuilder().module(new RepoServiceModule(config("console-role-it", freePort())))
                .environment(wrongIssuer).build().boot(List.of(RepoServiceModule.ROLE)))
                .hasStackTraceContaining("OIDC discovery at " + issuer() + "-missing");

        Map<String, String> partial = new java.util.HashMap<>(environment);
        partial.remove(OidcConsoleSettings.ENV_CLIENT_SECRET);
        assertThatThrownBy(() -> Composer.emptyBuilder().module(new RepoServiceModule(config("console-role-it", freePort())))
                .environment(partial).build().boot(List.of(RepoServiceModule.ROLE)))
                .hasStackTraceContaining(OidcConsoleSettings.ENV_CLIENT_SECRET)
                .hasStackTraceContaining("partly configured");
    }

    @Test
    @Order(6)
    void anExpiredSessionIsRefusedEverywhere() throws Exception {
        String session = signIn("alice", "alice-password");
        assertThat(status(session).statusCode()).isEqualTo(200);
        CLOCK.advance(TTL);
        assertThat(status(session).statusCode()).isEqualTo(401);
        assertCode(() -> throughGateway(session).listDocuments(list(ACCOUNT_A)), Status.Code.UNAUTHENTICATED);
    }

    // ------------------------------------------------------------------ fixtures

    private static <S extends AbstractStub<S>> S as(String credential, S stub) {
        var headers = new Metadata();
        headers.put(Metadata.Key.of("api_token", Metadata.ASCII_STRING_MARSHALLER), credential);
        return stub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers));
    }

    private static ListDocumentsRequest list(String account) {
        return ListDocumentsRequest.newBuilder().setAccountId(account).setPageSize(100).build();
    }

    private static NodeAddress address(String account, String doc) {
        return NodeAddress.newBuilder().setAccountId(account).setDocId(doc)
                .setGraphId("intake:" + account).setGraphAddressId(DATASOURCE).build();
    }

    private static AccessRule rule(String identityType, String identity) {
        return AccessRule.newBuilder().setIdentityType(identityType).setIdentity(identity)
                .setAccess(Access.ACCESS_READ).build();
    }

    private static SaveDocumentRequest save(String account, String doc, AccessRule readRule) {
        var document = Document.newBuilder().setDocId(doc)
                .setOwnership(OwnershipContext.newBuilder().setAccountId(account).setDatasourceId(DATASOURCE)
                        .setSecurity(DocumentSecurity.newBuilder().addPermissions(readRule)))
                .setSearchMetadata(SearchMetadata.newBuilder().setTitle(doc).setBody("body of " + doc))
                .build();
        return SaveDocumentRequest.newBuilder().setOperationId("op-" + doc)
                .setTarget(DocumentTarget.newBuilder().setAddress(address(account, doc)))
                .setExpected(ExpectedRevision.newBuilder().setNone(true))
                .setDocument(document)
                .addWrites(PartWrite.newBuilder()
                        .setSelector(PartSelector.newBuilder().setPart(DocumentPart.DOCUMENT_PART_CORE))
                        .setAction(WriteAction.WRITE_ACTION_PUT))
                .setOwner(SaveOwner.newBuilder().setIntake(IntakeOrigin.newBuilder().setDatasourceId(DATASOURCE)))
                .build();
    }

    private static void assertCode(ThrowingCallable call, Status.Code code) {
        assertThatThrownBy(call).isInstanceOfSatisfying(StatusRuntimeException.class,
                failure -> assertThat(failure.getStatus().getCode()).isEqualTo(code));
    }

    private static void assertDenied(ThrowingCallable call, String message) {
        assertThatThrownBy(call).isInstanceOfSatisfying(StatusRuntimeException.class, failure -> {
            assertThat(failure.getStatus().getCode()).isEqualTo(Status.Code.PERMISSION_DENIED);
            assertThat(failure.getStatus().getDescription()).contains(message);
        });
    }
}
