package ai.protomolt.proto.repo.service;

import static org.assertj.core.api.Assertions.assertThat;

import ai.protomolt.proto.authz.AccountBinding;
import ai.protomolt.proto.authz.AccountBindingResolver;
import ai.protomolt.proto.authz.AccountBindings;
import ai.protomolt.proto.authz.AclIdentity;
import ai.protomolt.proto.authz.IntakeWriterScope;
import ai.protomolt.proto.authz.FakeOpenIdProvider;
import ai.protomolt.proto.authz.OidcConsoleSessions;
import ai.protomolt.proto.http.connect.ConnectGateway;
import ai.protomolt.proto.repo.container.ledger.LedgerConfig;
import ai.protomolt.proto.repo.v1.Access;
import ai.protomolt.proto.repo.v1.AccessRule;
import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v1.CreateDriveRequest;
import ai.protomolt.proto.repo.v1.DriveServiceGrpc;
import ai.protomolt.proto.repo.v1.DriveType;
import ai.protomolt.proto.repo.v2.DeleteDocumentRequest;
import ai.protomolt.proto.repo.v2.DeleteDocumentResponse;
import ai.protomolt.proto.repo.v2.ExpectedRevision;
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
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
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
    static String bobSession;
    static String carolSession;
    static final AtomicReference<AccountBindings> BINDINGS = new AtomicReference<>();
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
        // The fake provider signs "alice" in; she is bound to one account only and reads.
        // "bob" and "carol" are bound to the same account and write as the session
        // datasource; carol's binding is swapped during a test, so the resolver reads the
        // current document on every call, as a reloaded binding file would be.
        BINDINGS.set(AccountBindings.newBuilder()
                .addBindings(AccountBinding.newBuilder().setPrincipal("alice").addAccountIds(BOUND))
                .addBindings(writer("bob"))
                .addBindings(writer("carol"))
                .build());
        services = RepoServices.build(config).withAccountBindings(
                caller -> AccountBindingResolver.of(BINDINGS.get()).bindingOf(caller));

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
        session = signIn(signInBase, "alice");
        bobSession = signIn(signInBase, "bob");
        carolSession = signIn(signInBase, "carol");
    }

    /** A binding that writes as {@link #DATASOURCE} in {@link #BOUND} and presents its own name as an ACL identity. */
    private static AccountBinding writer(String principal) {
        return AccountBinding.newBuilder().setPrincipal(principal).addAccountIds(BOUND)
                .addAclIdentities(AclIdentity.newBuilder().setIdentityType("user-principal-name")
                        .setIdentity(principal + "@example.com"))
                .addIntakeWriters(IntakeWriterScope.newBuilder().setAccountId(BOUND).setDatasourceId(DATASOURCE)
                        .setAllowCreate(true))
                .build();
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
        var accession = UploadDocumentResponse.parseFrom(saved.body()).getSaved().getAccession();
        assertThat(accession.getRevision()).isEqualTo(1);
        // Process authority writes no access rules, as before session writers existed.
        assertThat(services.v2Composition().catalog().manifest(accession.getAddress(), 1).orElseThrow()
                .getOwnership().hasSecurity()).isFalse();
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

    @Test
    void aSessionWhoseBindingListsAnIntakeWriterUploadsReplacesAndDeletes() throws Exception {
        UnaryOperator<HttpRequest.Builder> bob = cookie(bobSession);
        HttpResponse<byte[]> created = upload(BOUND, "bob", bob);
        assertThat(created.statusCode()).as(new String(created.body(), StandardCharsets.UTF_8)).isEqualTo(200);
        var accession = UploadDocumentResponse.parseFrom(created.body()).getSaved().getAccession();
        assertThat(accession.getRevision()).isEqualTo(1);
        NodeAddress address = accession.getAddress();

        // The revision records the binding's grant as its write authority, and the upload
        // granted the uploader's ACL identity READ and WRITE, so the session can delete it.
        var catalog = services.v2Composition().catalog();
        var first = catalog.manifest(address, 1).orElseThrow();
        UUID credential = RepoBindingWriters.credentialId("bob");
        var grants = catalog.grants(BOUND, RepoBindingWriters.ISSUER, credential);
        assertThat(grants).singleElement().satisfies(grant -> {
            assertThat(grant.revoked()).isFalse();
            assertThat(grant.grant().getIntake().getDatasourceId()).isEqualTo(DATASOURCE);
            assertThat(grant.grant().getAllowCreate()).isTrue();
        });
        assertThat(first.getValidation().getWriterGrant().getWriterGrantId()).isEqualTo(grants.getFirst().grant().getGrantId());
        assertThat(first.getOwnership().getSecurity().getPermissionsList())
                .extracting(AccessRule::getIdentityType, AccessRule::getIdentity, AccessRule::getAccess)
                .containsExactly(
                        org.assertj.core.groups.Tuple.tuple("user-principal-name", "bob@example.com", Access.ACCESS_READ),
                        org.assertj.core.groups.Tuple.tuple("user-principal-name", "bob@example.com", Access.ACCESS_WRITE));

        // A new revision keeps the rules of the revision it replaces.
        HttpResponse<byte[]> replaced = upload(BOUND, "bob", "op-bob-2", "1", bob);
        assertThat(replaced.statusCode()).as(new String(replaced.body(), StandardCharsets.UTF_8)).isEqualTo(200);
        assertThat(catalog.manifest(address, 2).orElseThrow().getOwnership().getSecurity())
                .isEqualTo(first.getOwnership().getSecurity());

        HttpResponse<byte[]> deleted = connect(bobSession, "DeleteDocument", DeleteDocumentRequest.newBuilder()
                .setOperationId("op-bob-delete").setAddress(address)
                .setExpected(ExpectedRevision.newBuilder().setRevision(2)).build());
        assertThat(deleted.statusCode()).as(new String(deleted.body(), StandardCharsets.UTF_8)).isEqualTo(200);
        var tombstone = DeleteDocumentResponse.parseFrom(deleted.body()).getAccession();
        assertThat(tombstone.getRevision()).isEqualTo(3);
        assertThat(tombstone.getTombstone()).isTrue();

        // alice is bound to the account but reads only: her delete is refused by the writer check.
        HttpResponse<byte[]> other = upload(BOUND, "bob-other", bob);
        assertThat(other.statusCode()).isEqualTo(200);
        HttpResponse<byte[]> refused = connect(session, "DeleteDocument", DeleteDocumentRequest.newBuilder()
                .setOperationId("op-alice-delete")
                .setAddress(UploadDocumentResponse.parseFrom(other.body()).getSaved().getAccession().getAddress())
                .setExpected(ExpectedRevision.newBuilder().setRevision(1)).build());
        assertThat(refused.statusCode()).isEqualTo(403);
    }

    @Test
    void removingAnIntakeWriterFromTheBindingRevokesItsGrantAndListingItAgainIssuesANewOne() throws Exception {
        UnaryOperator<HttpRequest.Builder> carol = cookie(carolSession);
        assertThat(upload(BOUND, "carol-1", carol).statusCode()).isEqualTo(200);
        UUID credential = RepoBindingWriters.credentialId("carol");
        var catalog = services.v2Composition().catalog();
        String firstGrant = catalog.grants(BOUND, RepoBindingWriters.ISSUER, credential).getFirst().grant().getGrantId();

        AccountBindings before = BINDINGS.get();
        BINDINGS.set(replace(before, writer("carol").toBuilder().clearIntakeWriters().build()));
        try {
            HttpResponse<byte[]> refused = upload(BOUND, "carol-2", carol);
            assertThat(refused.statusCode()).isEqualTo(403);
            assertThat(ItemError.parseFrom(refused.body()).getRejection().getReason())
                    .isEqualTo(RejectionReason.REJECTION_REASON_WRITE_DENIED);
            assertThat(catalog.grants(BOUND, RepoBindingWriters.ISSUER, credential))
                    .allSatisfy(grant -> assertThat(grant.revoked()).isTrue());
        } finally {
            BINDINGS.set(before);
        }

        // Listing the writer again issues the next grant of its series; a revoked ID is never reused.
        assertThat(upload(BOUND, "carol-3", carol).statusCode()).isEqualTo(200);
        var grants = catalog.grants(BOUND, RepoBindingWriters.ISSUER, credential);
        assertThat(grants).hasSize(2);
        assertThat(grants).filteredOn(grant -> !grant.revoked()).singleElement()
                .satisfies(grant -> assertThat(grant.grant().getGrantId()).isNotEqualTo(firstGrant)
                        .isEqualTo(firstGrant.substring(0, firstGrant.lastIndexOf(':') + 1) + "2"));
    }

    private static AccountBindings replace(AccountBindings document, AccountBinding binding) {
        AccountBindings.Builder out = AccountBindings.newBuilder();
        for (AccountBinding existing : document.getBindingsList()) {
            out.addBindings(existing.getPrincipal().equals(binding.getPrincipal()) ? binding : existing);
        }
        return out.build();
    }

    private HttpRequest.Builder withSession(HttpRequest.Builder builder) {
        return builder.header("Cookie", OidcConsoleSessions.COOKIE + "=" + session);
    }

    private static UnaryOperator<HttpRequest.Builder> cookie(String value) {
        return builder -> builder.header("Cookie", OidcConsoleSessions.COOKIE + "=" + value);
    }

    /** One unary DocumentService call through the Connect gateway, as the console makes it. */
    private static HttpResponse<byte[]> connect(String sessionCookie, String method, com.google.protobuf.Message request)
            throws Exception {
        return CLIENT.send(HttpRequest.newBuilder(URI.create(base + ConnectGateway.PATH
                        + "/ai.protomolt.proto.repo.v2.DocumentService/" + method))
                .header("Content-Type", ConnectGateway.UNARY_CONTENT_TYPE)
                .header("Cookie", OidcConsoleSessions.COOKIE + "=" + sessionCookie)
                .POST(HttpRequest.BodyPublishers.ofByteArray(request.toByteArray())).build(),
                HttpResponse.BodyHandlers.ofByteArray());
    }

    private static HttpResponse<byte[]> upload(String account, String name,
            UnaryOperator<HttpRequest.Builder> credential) throws Exception {
        return upload(account, name, "op-" + name, "", credential);
    }

    private static HttpResponse<byte[]> upload(String account, String name, String operationId, String expected,
            UnaryOperator<HttpRequest.Builder> credential) throws Exception {
        HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(base + UploadHttpServer.UPLOAD_PATH
                        + "?account_id=" + account + "&datasource_id=" + DATASOURCE + "&drive=" + DRIVE
                        + "&filename=" + name + ".txt&doc_id=doc-" + name + "&operation_id=" + operationId
                        + (expected.isEmpty() ? "" : "&expected_revision=" + expected)))
                .header("Accept", "application/proto")
                .POST(HttpRequest.BodyPublishers.ofByteArray(BODY));
        return CLIENT.send(credential.apply(request).build(), HttpResponse.BodyHandlers.ofByteArray());
    }

    /** Signs {@code principal} in through the fake provider and returns the session cookie value. */
    private static String signIn(String signInBase, String principal) throws Exception {
        provider.nextIntrospection = "{\"active\": true, \"username\": \"" + principal
                + "\", \"protomolt_scopes\": [\"search-query\"]}";
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
