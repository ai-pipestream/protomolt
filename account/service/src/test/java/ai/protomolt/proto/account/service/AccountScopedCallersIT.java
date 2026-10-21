package ai.protomolt.proto.account.service;

import ai.protomolt.proto.account.service.store.AccountStoreConfig;
import ai.protomolt.proto.account.v1.Account;
import ai.protomolt.proto.account.v1.AccountServiceGrpc;
import ai.protomolt.proto.account.v1.AccountStatus;
import ai.protomolt.proto.account.v1.ActivateAccountRequest;
import ai.protomolt.proto.account.v1.CreateAccountRequest;
import ai.protomolt.proto.account.v1.DeactivateAccountRequest;
import ai.protomolt.proto.account.v1.GetAccountRequest;
import ai.protomolt.proto.account.v1.ListAccountsRequest;
import ai.protomolt.proto.account.v1.ListAccountsResponse;
import ai.protomolt.proto.actions.Scopes;
import ai.protomolt.proto.authz.AccessPolicy;
import ai.protomolt.proto.authz.AccessPolicyCallers;
import ai.protomolt.proto.authz.AccountBinding;
import ai.protomolt.proto.authz.AccountBindingResolver;
import ai.protomolt.proto.authz.AccountBindings;
import ai.protomolt.proto.authz.CallerResolver;
import ai.protomolt.proto.authz.Principal;
import ai.protomolt.proto.repo.v1.CreateDriveRequest;
import ai.protomolt.proto.repo.v1.CreateDriveResponse;
import ai.protomolt.proto.repo.v1.Drive;
import ai.protomolt.proto.repo.v1.DriveServiceGrpc;
import com.google.protobuf.Timestamp;
import io.grpc.ClientInterceptor;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.health.v1.HealthCheckRequest;
import io.grpc.health.v1.HealthGrpc;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Scoped callers on AccountService, over the real Netty transport and the real
 * {@code ApiTokenServerInterceptor}, against a testcontainers PostgreSQL: the
 * credential gate (missing and unknown credentials), account-bound reads and lists
 * (including the totals and continuation of a restricted list), the
 * process-authority-only writes, the in-process authenticated overload, and the
 * repo channel presenting the operator token.
 */
@Testcontainers
class AccountScopedCallersIT {

    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    private static final String REPO_INPROCESS = "scoped-it-repo";
    private static final String OPERATOR_TOKEN = "scoped-it-operator-token";
    private static final String ALICE_TOKEN = "scoped-it-alice-token";
    private static final String CAROL_TOKEN = "scoped-it-carol-token";
    private static final String BOB_TOKEN = "scoped-it-bob-token";

    /** A DriveService that records CreateDrive calls and answers idempotently. */
    static final class FakeDriveService extends DriveServiceGrpc.DriveServiceImplBase {
        final List<CreateDriveRequest> createCalls = new CopyOnWriteArrayList<>();

        @Override
        public void createDrive(CreateDriveRequest request,
                StreamObserver<CreateDriveResponse> responseObserver) {
            createCalls.add(request);
            responseObserver.onNext(CreateDriveResponse.newBuilder()
                    .setDrive(Drive.newBuilder()
                            .setDriveId("drive-" + request.getAccountId() + "-" + request.getName())
                            .setName(request.getName())
                            .setAccountId(request.getAccountId())
                            .setDriveType(request.getDriveType())
                            .setCreatedAt(Timestamp.getDefaultInstance()))
                    .build());
            responseObserver.onCompleted();
        }

        long callsFor(String accountId) {
            return createCalls.stream().filter(r -> r.getAccountId().equals(accountId)).count();
        }
    }

    static FakeDriveService fakeDrives;
    static Server fakeRepoServer;
    static AccountServices services;
    static Server netty;
    static final List<ManagedChannel> CHANNELS = new ArrayList<>();

    /** Five accounts; alice is bound to three of them plus an id with no row. */
    static List<String> accountIds;
    static List<String> aliceAccounts;
    /** Carol's two accounts: one active, one deactivated. */
    static String carolActive;
    static String carolDeactivated;

    static CallerResolver resolver;

    @BeforeAll
    static void boot() throws Exception {
        fakeDrives = new FakeDriveService();
        fakeRepoServer = InProcessServerBuilder.forName(REPO_INPROCESS)
                .addService(fakeDrives).directExecutor().build().start();
        services = AccountServices.build(config(AccountServiceConfig.INPROCESS_TARGET_PREFIX
                + REPO_INPROCESS));
        resolver = new AccessPolicyCallers(AccessPolicy.newBuilder()
                .addPrincipals(principal("alice", ALICE_TOKEN))
                .addPrincipals(principal("bob", BOB_TOKEN))
                .addPrincipals(principal("carol", CAROL_TOKEN))
                .build());
        netty = services.startNetty(0, OPERATOR_TOKEN, resolver);

        AccountServiceGrpc.AccountServiceBlockingStub operator = stub(OPERATOR_TOKEN);
        String run = UUID.randomUUID().toString().substring(0, 8);
        accountIds = new ArrayList<>();
        for (int i = 1; i <= 5; i++) {
            String id = "scoped-" + run + "-" + i;
            operator.createAccount(CreateAccountRequest.newBuilder().setAccountId(id).build());
            accountIds.add(id);
        }
        // Bound out of creation order, plus an id with no account row: the
        // restricted list orders by creation and counts only real rows.
        aliceAccounts = List.of(accountIds.get(3), accountIds.get(0), accountIds.get(1));
        carolActive = "scoped-" + run + "-carol-active";
        carolDeactivated = "scoped-" + run + "-carol-off";
        operator.createAccount(CreateAccountRequest.newBuilder().setAccountId(carolActive).build());
        operator.createAccount(CreateAccountRequest.newBuilder()
                .setAccountId(carolDeactivated).build());
        operator.deactivateAccount(DeactivateAccountRequest.newBuilder()
                .setAccountId(carolDeactivated).build());

        services.withAccountBindings(AccountBindingResolver.of(AccountBindings.newBuilder()
                .addBindings(AccountBinding.newBuilder().setPrincipal("alice")
                        .addAllAccountIds(aliceAccounts)
                        .addAccountIds("scoped-" + run + "-no-such-account"))
                .addBindings(AccountBinding.newBuilder().setPrincipal("carol")
                        .addAccountIds(carolActive).addAccountIds(carolDeactivated))
                .build()));
    }

    @AfterAll
    static void tearDown() {
        CHANNELS.forEach(ManagedChannel::shutdownNow);
        services.close();
        fakeRepoServer.shutdownNow();
    }

    // ------------------------------------------------------------- helpers

    private static AccountServiceConfig config(String repoTarget) {
        return new AccountServiceConfig(0, new AccountStoreConfig(POSTGRES.getJdbcUrl(),
                POSTGRES.getUsername(), POSTGRES.getPassword()), repoTarget,
                null, null, null, true, -1L);
    }

    private static Principal principal(String name, String token) {
        return Principal.newBuilder().setName(name)
                .addCredentialSha256(AccessPolicyCallers.sha256Hex(token))
                .addScopes(Scopes.SEARCH_QUERY)
                .build();
    }

    private static ClientInterceptor credential(String token) {
        Metadata headers = new Metadata();
        headers.put(Metadata.Key.of("api_token", Metadata.ASCII_STRING_MARSHALLER), token);
        return MetadataUtils.newAttachHeadersInterceptor(headers);
    }

    private static ManagedChannel channel(String tokenOrNull) {
        var builder = NettyChannelBuilder.forAddress("localhost", netty.getPort()).usePlaintext();
        if (tokenOrNull != null) {
            builder.intercept(credential(tokenOrNull));
        }
        ManagedChannel channel = builder.build();
        CHANNELS.add(channel);
        return channel;
    }

    private static AccountServiceGrpc.AccountServiceBlockingStub stub(String tokenOrNull) {
        return AccountServiceGrpc.newBlockingStub(channel(tokenOrNull));
    }

    private static GetAccountRequest get(String accountId) {
        return GetAccountRequest.newBuilder().setAccountId(accountId).build();
    }

    private static void assertRefused(Runnable call, Status.Code code, String description) {
        assertThatThrownBy(call::run).isInstanceOfSatisfying(StatusRuntimeException.class, e -> {
            assertThat(e.getStatus().getCode()).isEqualTo(code);
            assertThat(e.getStatus().getDescription()).isEqualTo(description);
        });
    }

    private static List<String> idsOf(ListAccountsResponse page) {
        return page.getAccountsList().stream().map(Account::getAccountId).toList();
    }

    // ---------------------------------------------------------------- tests

    @Test
    void aMissingCredentialIsUnauthenticatedOnEveryServiceIncludingHealth() {
        ManagedChannel anonymous = channel(null);
        assertRefused(() -> AccountServiceGrpc.newBlockingStub(anonymous)
                        .getAccount(get(accountIds.get(0))),
                Status.Code.UNAUTHENTICATED, "Missing API token 'api_token'");
        assertRefused(() -> AccountServiceGrpc.newBlockingStub(anonymous)
                        .listAccounts(ListAccountsRequest.getDefaultInstance()),
                Status.Code.UNAUTHENTICATED, "Missing API token 'api_token'");
        assertRefused(() -> HealthGrpc.newBlockingStub(anonymous)
                        .check(HealthCheckRequest.getDefaultInstance()),
                Status.Code.UNAUTHENTICATED, "Missing API token 'api_token'");
    }

    @Test
    void anUnknownCredentialIsUnauthenticated() {
        assertRefused(() -> stub("not-a-known-token").getAccount(get(accountIds.get(0))),
                Status.Code.UNAUTHENTICATED, "Invalid API token");
        assertRefused(() -> stub("not-a-known-token")
                        .listAccounts(ListAccountsRequest.getDefaultInstance()),
                Status.Code.UNAUTHENTICATED, "Invalid API token");
    }

    @Test
    void aScopedCallerReadsItsBoundAccount() {
        Account own = stub(ALICE_TOKEN).getAccount(get(accountIds.get(3))).getAccount();
        assertThat(own.getAccountId()).isEqualTo(accountIds.get(3));
        assertThat(own.getStatus()).isEqualTo(AccountStatus.ACCOUNT_STATUS_ACTIVE);
        // The id is trimmed before the binding check, exactly as before the store read.
        assertThat(stub(ALICE_TOKEN).getAccount(get("  " + accountIds.get(0) + " "))
                .getAccount().getAccountId()).isEqualTo(accountIds.get(0));
    }

    @Test
    void aScopedCallerIsRefusedAnUnboundAccountByName() {
        String other = accountIds.get(2);
        assertRefused(() -> stub(ALICE_TOKEN).getAccount(get(other)),
                Status.Code.PERMISSION_DENIED, "The caller is not bound to account '" + other + "'");
        // An account that does not exist refuses the same way: a scoped caller
        // learns nothing about accounts outside its binding.
        assertRefused(() -> stub(ALICE_TOKEN).getAccount(get("scoped-never-created")),
                Status.Code.PERMISSION_DENIED,
                "The caller is not bound to account 'scoped-never-created'");
        // The account_id shape is still validated first.
        assertRefused(() -> stub(ALICE_TOKEN).getAccount(get("  ")),
                Status.Code.INVALID_ARGUMENT, "account_id is required");
    }

    @Test
    void anUnboundPrincipalIsRefusedOnGetAndList() {
        assertRefused(() -> stub(BOB_TOKEN).getAccount(get(accountIds.get(0))),
                Status.Code.PERMISSION_DENIED, "The caller is bound to no account");
        assertRefused(() -> stub(BOB_TOKEN).listAccounts(ListAccountsRequest.getDefaultInstance()),
                Status.Code.PERMISSION_DENIED, "The caller is bound to no account");
    }

    @Test
    void aScopedListHoldsOnlyBoundAccountsWithRestrictedTotalsAndPaging() {
        AccountServiceGrpc.AccountServiceBlockingStub alice = stub(ALICE_TOKEN);
        ListAccountsResponse first = alice.listAccounts(
                ListAccountsRequest.newBuilder().setLimit(2).build());
        assertThat(first.getTotalCount()).isEqualTo(3);
        // Creation order, not binding order.
        assertThat(idsOf(first)).containsExactly(accountIds.get(0), accountIds.get(1));
        assertThat(first.getNextContinuationToken()).isEqualTo("2");

        ListAccountsResponse second = alice.listAccounts(ListAccountsRequest.newBuilder()
                .setLimit(2).setContinuationToken(first.getNextContinuationToken()).build());
        assertThat(second.getTotalCount()).isEqualTo(3);
        assertThat(idsOf(second)).containsExactly(accountIds.get(3));
        assertThat(second.getNextContinuationToken()).isEmpty();

        // The default page holds all three and nothing else in the store.
        ListAccountsResponse whole = alice.listAccounts(ListAccountsRequest.getDefaultInstance());
        assertThat(idsOf(whole)).containsExactlyInAnyOrderElementsOf(aliceAccounts);
        assertThat(whole.getTotalCount()).isEqualTo(3);
    }

    @Test
    void aScopedListAppliesTheStatusFilterInsideTheRestriction() {
        AccountServiceGrpc.AccountServiceBlockingStub carol = stub(CAROL_TOKEN);
        ListAccountsResponse active = carol.listAccounts(ListAccountsRequest.newBuilder()
                .setStatusFilter(AccountStatus.ACCOUNT_STATUS_ACTIVE).build());
        assertThat(idsOf(active)).containsExactly(carolActive);
        assertThat(active.getTotalCount()).isEqualTo(1);

        ListAccountsResponse off = carol.listAccounts(ListAccountsRequest.newBuilder()
                .setStatusFilter(AccountStatus.ACCOUNT_STATUS_DEACTIVATED).setLimit(1).build());
        assertThat(idsOf(off)).containsExactly(carolDeactivated);
        assertThat(off.getTotalCount()).isEqualTo(1);
        assertThat(off.getNextContinuationToken()).isEmpty();

        ListAccountsResponse every = carol.listAccounts(ListAccountsRequest.getDefaultInstance());
        assertThat(idsOf(every)).containsExactly(carolActive, carolDeactivated);
        assertThat(every.getTotalCount()).isEqualTo(2);
    }

    @Test
    void writesNeedProcessAuthority() {
        AccountServiceGrpc.AccountServiceBlockingStub alice = stub(ALICE_TOKEN);
        String fresh = "scoped-refused-" + UUID.randomUUID().toString().substring(0, 8);
        assertRefused(() -> alice.createAccount(CreateAccountRequest.newBuilder()
                        .setAccountId(fresh).build()),
                Status.Code.PERMISSION_DENIED,
                "CreateAccount needs process authority; caller 'alice' is a scoped caller");
        // Refused before provisioning and before the store: no drive, no row.
        assertThat(fakeDrives.callsFor(fresh)).isZero();
        assertRefused(() -> stub(OPERATOR_TOKEN).getAccount(get(fresh)),
                Status.Code.NOT_FOUND, "account not found: " + fresh);

        // Even on its own bound account.
        String own = accountIds.get(0);
        assertRefused(() -> alice.deactivateAccount(DeactivateAccountRequest.newBuilder()
                        .setAccountId(own).build()),
                Status.Code.PERMISSION_DENIED,
                "DeactivateAccount needs process authority; caller 'alice' is a scoped caller");
        assertRefused(() -> stub(CAROL_TOKEN).activateAccount(ActivateAccountRequest.newBuilder()
                        .setAccountId(carolDeactivated).build()),
                Status.Code.PERMISSION_DENIED,
                "ActivateAccount needs process authority; caller 'carol' is a scoped caller");
        assertThat(stub(OPERATOR_TOKEN).getAccount(get(own)).getAccount().getStatus())
                .isEqualTo(AccountStatus.ACCOUNT_STATUS_ACTIVE);
        assertThat(stub(OPERATOR_TOKEN).getAccount(get(carolDeactivated)).getAccount()
                .getStatus()).isEqualTo(AccountStatus.ACCOUNT_STATUS_DEACTIVATED);
    }

    @Test
    void theOperatorReadsEveryAccountAndWrites() {
        AccountServiceGrpc.AccountServiceBlockingStub operator = stub(OPERATOR_TOKEN);
        // An account no scoped caller is bound to.
        assertThat(operator.getAccount(get(accountIds.get(2))).getAccount().getAccountId())
                .isEqualTo(accountIds.get(2));
        ListAccountsResponse all = operator.listAccounts(ListAccountsRequest.newBuilder()
                .setLimit(1000).build());
        assertThat(idsOf(all)).containsAll(accountIds).contains(carolActive, carolDeactivated);
        assertThat(all.getTotalCount()).isGreaterThanOrEqualTo(7);

        String fresh = "scoped-operator-" + UUID.randomUUID().toString().substring(0, 8);
        operator.createAccount(CreateAccountRequest.newBuilder().setAccountId(fresh).build());
        assertThat(fakeDrives.callsFor(fresh)).isEqualTo(2);
        assertThat(operator.deactivateAccount(DeactivateAccountRequest.newBuilder()
                .setAccountId(fresh).build()).getAccount().getStatus())
                .isEqualTo(AccountStatus.ACCOUNT_STATUS_DEACTIVATED);
        assertThat(operator.activateAccount(ActivateAccountRequest.newBuilder()
                .setAccountId(fresh).build()).getAccount().getStatus())
                .isEqualTo(AccountStatus.ACCOUNT_STATUS_ACTIVE);
    }

    @Test
    void theAuthenticatedInProcessOverloadEnforcesTheSameBoundary() {
        String name = "scoped-it-authenticated-" + UUID.randomUUID();
        services.startInProcess(name, OPERATOR_TOKEN, resolver);
        Function<String, AccountServiceGrpc.AccountServiceBlockingStub> inProcess = token -> {
            var builder = InProcessChannelBuilder.forName(name);
            if (token != null) {
                builder.intercept(credential(token));
            }
            ManagedChannel channel = builder.build();
            CHANNELS.add(channel);
            return AccountServiceGrpc.newBlockingStub(channel);
        };
        assertRefused(() -> inProcess.apply(null).getAccount(get(accountIds.get(0))),
                Status.Code.UNAUTHENTICATED, "Missing API token 'api_token'");
        assertThat(inProcess.apply(ALICE_TOKEN).getAccount(get(accountIds.get(0)))
                .getAccount().getAccountId()).isEqualTo(accountIds.get(0));
        assertRefused(() -> inProcess.apply(ALICE_TOKEN).getAccount(get(accountIds.get(2))),
                Status.Code.PERMISSION_DENIED,
                "The caller is not bound to account '" + accountIds.get(2) + "'");
        assertThat(inProcess.apply(OPERATOR_TOKEN).getAccount(get(accountIds.get(2)))
                .getAccount().getAccountId()).isEqualTo(accountIds.get(2));
    }

    @Test
    void thePlainInProcessTransportRunsWithProcessAuthority() {
        String name = "scoped-it-trusted-" + UUID.randomUUID();
        services.startInProcess(name);
        ManagedChannel channel = InProcessChannelBuilder.forName(name).build();
        CHANNELS.add(channel);
        assertThat(AccountServiceGrpc.newBlockingStub(channel).getAccount(get(accountIds.get(2)))
                .getAccount().getAccountId()).isEqualTo(accountIds.get(2));
    }

    @Test
    void transportsRefuseAMissingOrBlankOperatorToken() {
        for (String token : new String[] {null, "", "   "}) {
            assertThatThrownBy(() -> services.startNetty(0, token, resolver))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("PROTOMOLT_API_TOKEN");
            assertThatThrownBy(() -> services.startInProcess("scoped-it-no-token", token, null))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("PROTOMOLT_API_TOKEN");
        }
    }

    @Test
    void aNetworkRepoTargetWithoutATokenRefusesAtBuild() {
        assertThatThrownBy(() -> AccountServices.build(config("localhost:1")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("PROTOMOLT_API_TOKEN")
                .hasMessageContaining("DOCUMENT_PLATFORM_REPO_GRPC_TARGET")
                .hasMessageContaining("localhost:1");
        assertThatThrownBy(() -> AccountServices.build(config("localhost:1"), "  "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("PROTOMOLT_API_TOKEN");
    }

    @Test
    void theRepoChannelPresentsTheOperatorTokenToANetworkRepo() throws Exception {
        List<String> presented = new CopyOnWriteArrayList<>();
        ServerInterceptor recorder = new ServerInterceptor() {
            @Override
            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call,
                    Metadata headers, ServerCallHandler<ReqT, RespT> next) {
                String token = headers.get(
                        Metadata.Key.of("api_token", Metadata.ASCII_STRING_MARSHALLER));
                presented.add(token == null ? "<absent>" : token);
                return next.startCall(call, headers);
            }
        };
        FakeDriveService networkDrives = new FakeDriveService();
        Server networkRepo = NettyServerBuilder.forPort(0)
                .addService(ServerInterceptors.intercept(networkDrives, recorder))
                .build().start();
        try (AccountServices networked = AccountServices.build(
                config("localhost:" + networkRepo.getPort()), "repo-operator-token")) {
            String name = "scoped-it-network-repo-" + UUID.randomUUID();
            networked.startInProcess(name);
            ManagedChannel channel = InProcessChannelBuilder.forName(name).build();
            CHANNELS.add(channel);
            String accountId = "scoped-networked-" + UUID.randomUUID().toString().substring(0, 8);
            AccountServiceGrpc.newBlockingStub(channel).createAccount(
                    CreateAccountRequest.newBuilder().setAccountId(accountId).build());
            assertThat(networkDrives.callsFor(accountId)).isEqualTo(2);
            assertThat(Set.copyOf(presented)).containsExactly("repo-operator-token");
            assertThat(presented).hasSize(2);
        } finally {
            networkRepo.shutdownNow();
        }
    }
}
