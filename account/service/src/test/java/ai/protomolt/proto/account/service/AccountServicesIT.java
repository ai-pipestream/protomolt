package ai.protomolt.proto.account.service;

import ai.protomolt.proto.account.service.identity.DirectAccountIdentityResolver;
import ai.protomolt.proto.account.service.store.AccountStoreConfig;
import ai.protomolt.proto.account.v1.AccountServiceGrpc;
import ai.protomolt.proto.account.v1.AccountStatus;
import ai.protomolt.proto.account.v1.CreateAccountRequest;
import ai.protomolt.proto.account.v1.GetAccountRequest;
import ai.protomolt.proto.repo.v1.CreateDriveRequest;
import ai.protomolt.proto.repo.v1.CreateDriveResponse;
import ai.protomolt.proto.repo.v1.Drive;
import ai.protomolt.proto.repo.v1.DriveServiceGrpc;
import com.google.protobuf.Timestamp;
import io.grpc.BindableService;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.health.v1.HealthCheckRequest;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.health.v1.HealthGrpc;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The {@link AccountServices} wiring itself, against a real testcontainers
 * PostgreSQL 17: the Netty standalone transport (health service + a real RPC
 * over TCP, the path {@code AccountServiceIT}'s in-process setup never
 * takes), the lifecycle no-op branches, and the plaintext-host:port repo
 * channel's outage behavior. The RPC semantics are AccountServiceIT's
 * territory; this pins the boot/serve/stop envelope around them.
 */
@Testcontainers(disabledWithoutDocker = true)
class AccountServicesIT {

    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

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
    }

    static FakeDriveService fakeDrives;
    static Server fakeRepoServer;

    @BeforeAll
    static void bootRepo() throws Exception {
        fakeDrives = new FakeDriveService();
        fakeRepoServer = InProcessServerBuilder.forName("services-it-repo")
                .addService(fakeDrives)
                .directExecutor()
                .build()
                .start();
    }

    @AfterAll
    static void stopRepo() {
        fakeRepoServer.shutdownNow();
    }

    private static AccountStoreConfig storeConfig() {
        return new AccountStoreConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(),
                POSTGRES.getPassword());
    }

    private static String uniqueId(String prefix) {
        return prefix + "-" + UUID.randomUUID().toString().substring(0, 8);
    }

    @Test
    void nettyServesHealthAndTheAccountServiceOverTcp() throws Exception {
        AccountServiceConfig config = new AccountServiceConfig(0, storeConfig(),
                AccountServiceConfig.INPROCESS_TARGET_PREFIX + "services-it-repo",
                null, null, null, true, -1L);
        try (AccountServices services = AccountServices.build(config)) {
            Server server = services.startNetty(0);
            assertThat(server.getPort()).isPositive();

            ManagedChannel channel = NettyChannelBuilder
                    .forAddress("localhost", server.getPort()).usePlaintext().build();
            try {
                // The health service reports SERVING for the whole server.
                HealthCheckResponse health = HealthGrpc.newBlockingStub(channel)
                        .check(HealthCheckRequest.getDefaultInstance());
                assertThat(health.getStatus())
                        .isEqualTo(HealthCheckResponse.ServingStatus.SERVING);

                // And a real RPC crosses the TCP transport end to end.
                String accountId = uniqueId("acct-netty");
                AccountServiceGrpc.AccountServiceBlockingStub accounts =
                        AccountServiceGrpc.newBlockingStub(channel);
                var created = accounts.createAccount(CreateAccountRequest.newBuilder()
                        .setAccountId(accountId)
                        .setDisplayName("Netty Account")
                        .build()).getAccount();
                assertThat(created.getAccountId()).isEqualTo(accountId);
                assertThat(created.getStatus()).isEqualTo(AccountStatus.ACCOUNT_STATUS_ACTIVE);
                // Provisioning rode the in-process repo target from TCP land.
                assertThat(fakeDrives.createCalls.stream()
                        .filter(r -> r.getAccountId().equals(accountId)).toList())
                        .hasSize(2);
            } finally {
                channel.shutdownNow();
            }
        }
    }

    @Test
    void theWiredSetExposesItsSpiSeams() {
        AccountServiceConfig config = new AccountServiceConfig(0, storeConfig(),
                AccountServiceConfig.INPROCESS_TARGET_PREFIX + "services-it-repo",
                null, null, null, true, -1L);
        try (AccountServices services = AccountServices.build(config)) {
            // The SPI the README promises: one AccountService impl, and the
            // default pass-through identity resolver.
            assertThat(services.services()).hasSize(1);
            assertThat(services.services().get(0)).isInstanceOf(AccountGrpcService.class);
            assertThat(services.services()).isUnmodifiable();
            assertThat(services.identityResolver())
                    .isInstanceOf(DirectAccountIdentityResolver.class);
            // Eventing off: neither outbox nor relay exists.
            assertThat(services.eventOutbox()).isNull();
            assertThat(services.eventRelay()).isNull();
        }
    }

    @Test
    void startLifecycleIsANoOpWhenEventingIsOff() {
        AccountServiceConfig config = new AccountServiceConfig(0, storeConfig(),
                AccountServiceConfig.INPROCESS_TARGET_PREFIX + "services-it-repo",
                null, null, null, true, -1L);
        try (AccountServices services = AccountServices.build(config)) {
            // Documented no-op: no relay exists, so the loop never starts —
            // and close() must not trip over the never-started lifecycle.
            services.startLifecycle();
        }
    }

    @Test
    void startLifecycleIsANoOpWhenDisabledEvenWithKafkaConfigured() {
        AccountServiceConfig config = new AccountServiceConfig(0, storeConfig(),
                AccountServiceConfig.INPROCESS_TARGET_PREFIX + "services-it-repo",
                "localhost:1", null, null, false, -1L);
        try (AccountServices services = AccountServices.build(config)) {
            // The relay exists (eventing is on) but the toggle says no loop.
            assertThat(services.eventRelay()).isNotNull();
            services.startLifecycle();
        }
    }

    @Test
    void plaintextRepoTargetThatIsDownFailsCreateWithoutCommitting() {
        // The Netty channel branch of the repo target (no "inprocess:"
        // prefix): an unreachable host:port fails provisioning UNAVAILABLE
        // before anything commits, exactly like the in-process outage.
        AccountServiceConfig config = new AccountServiceConfig(0, storeConfig(),
                "localhost:1", null, null, null, true, -1L);
        try (AccountServices services = AccountServices.build(config)) {
            services.startInProcess("services-it-netty-repo-down");
            ManagedChannel channel = io.grpc.inprocess.InProcessChannelBuilder
                    .forName("services-it-netty-repo-down").build();
            try {
                AccountServiceGrpc.AccountServiceBlockingStub accounts =
                        AccountServiceGrpc.newBlockingStub(channel);
                String accountId = uniqueId("acct-plain-outage");

                assertThatThrownBy(() -> accounts.createAccount(
                        CreateAccountRequest.newBuilder().setAccountId(accountId).build()))
                        .isInstanceOfSatisfying(StatusRuntimeException.class, e ->
                                assertThat(e.getStatus().getCode())
                                        .isEqualTo(Status.Code.UNAVAILABLE));
                assertThatThrownBy(() -> accounts.getAccount(
                        GetAccountRequest.newBuilder().setAccountId(accountId).build()))
                        .isInstanceOfSatisfying(StatusRuntimeException.class, e ->
                                assertThat(e.getStatus().getCode())
                                        .isEqualTo(Status.Code.NOT_FOUND));
            } finally {
                channel.shutdownNow();
            }
        }
    }

    @Test
    void servicesListIsSharedAcrossTransports() throws Exception {
        // Both transports serve the SAME wired set: an account created over
        // the in-process transport is readable over Netty.
        AccountServiceConfig config = new AccountServiceConfig(0, storeConfig(),
                AccountServiceConfig.INPROCESS_TARGET_PREFIX + "services-it-repo",
                null, null, null, true, -1L);
        try (AccountServices services = AccountServices.build(config)) {
            services.startInProcess("services-it-both");
            Server netty = services.startNetty(0);
            ManagedChannel inprocess = io.grpc.inprocess.InProcessChannelBuilder
                    .forName("services-it-both").build();
            ManagedChannel tcp = NettyChannelBuilder
                    .forAddress("localhost", netty.getPort()).usePlaintext().build();
            try {
                String accountId = uniqueId("acct-both");
                AccountServiceGrpc.newBlockingStub(inprocess).createAccount(
                        CreateAccountRequest.newBuilder().setAccountId(accountId).build());
                var readOverTcp = AccountServiceGrpc.newBlockingStub(tcp).getAccount(
                        GetAccountRequest.newBuilder().setAccountId(accountId).build())
                        .getAccount();
                assertThat(readOverTcp.getAccountId()).isEqualTo(accountId);
                assertThat(readOverTcp.getStatus()).isEqualTo(AccountStatus.ACCOUNT_STATUS_ACTIVE);
            } finally {
                inprocess.shutdownNow();
                tcp.shutdownNow();
            }
            List<BindableService> exposed = services.services();
            assertThat(exposed).hasSize(1);
        }
    }
}
