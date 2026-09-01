package ai.pipestream.proto.account.service;

import ai.pipestream.proto.account.service.events.AccountEventRecord;
import ai.pipestream.proto.account.service.store.AccountRecord;
import ai.pipestream.proto.account.service.store.AccountStoreConfig;
import ai.pipestream.proto.account.v1.Account;
import ai.pipestream.proto.account.v1.AccountEvent;
import ai.pipestream.proto.account.v1.AccountServiceGrpc;
import ai.pipestream.proto.account.v1.AccountStatus;
import ai.pipestream.proto.account.v1.ActivateAccountRequest;
import ai.pipestream.proto.account.v1.CreateAccountRequest;
import ai.pipestream.proto.account.v1.DeactivateAccountRequest;
import ai.pipestream.proto.account.v1.GetAccountRequest;
import ai.pipestream.proto.account.v1.ListAccountsRequest;
import ai.pipestream.proto.account.v1.ListAccountsResponse;
import ai.pipestream.proto.repo.v1.CreateDriveRequest;
import ai.pipestream.proto.repo.v1.CreateDriveResponse;
import ai.pipestream.proto.repo.v1.Drive;
import ai.pipestream.proto.repo.v1.DriveServiceGrpc;
import ai.pipestream.proto.repo.v1.DriveType;
import com.google.protobuf.Timestamp;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * End-to-end integration test of account-service against REAL infrastructure:
 * a shared testcontainers PostgreSQL 17 (Flyway-migrated account store), with
 * the full service stack booted through {@link AccountServiceConfig} +
 * {@link AccountServices} over the gRPC in-process transport.
 * <p>
 * Drive provisioning is verified against a FAKE {@code DriveService} (a
 * recording {@code DriveServiceImplBase} on its own in-process server,
 * reached via the {@code "inprocess:<name>"} target convention) — the whole
 * repo-service stack would prove nothing extra here: the contract under test
 * is WHICH CreateDrive calls account-service issues, and when.
 * <p>
 * Kafka eventing is configured (a placeholder bootstrap server — the relay
 * loop is never started and the producer never sends) so the transactional
 * outbox is active: the tests assert the outbox rows land in the same
 * transaction as the account mutations.
 */
@Testcontainers(disabledWithoutDocker = true)
class AccountServiceIT {

    private static final String REPO_INPROCESS = "it-repo";

    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    /** A DriveService that records CreateDrive calls and answers idempotently. */
    static final class FakeDriveService extends DriveServiceGrpc.DriveServiceImplBase {
        final List<CreateDriveRequest> createCalls = new CopyOnWriteArrayList<>();

        @Override
        public void createDrive(CreateDriveRequest request,
                StreamObserver<CreateDriveResponse> responseObserver) {
            createCalls.add(request);
            // Idempotent by construction, like the real deterministic-id
            // CreateDrive: the same (account, name) always answers the same
            // drive, never an error.
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

        List<CreateDriveRequest> callsFor(String accountId) {
            return createCalls.stream()
                    .filter(r -> r.getAccountId().equals(accountId))
                    .toList();
        }
    }

    static FakeDriveService fakeDrives;
    static Server fakeRepoServer;
    static AccountServices services;
    static ManagedChannel channel;
    static AccountServiceGrpc.AccountServiceBlockingStub accounts;

    @BeforeAll
    static void boot() throws Exception {
        fakeDrives = new FakeDriveService();
        fakeRepoServer = InProcessServerBuilder.forName(REPO_INPROCESS)
                .addService(fakeDrives)
                .directExecutor()
                .build()
                .start();
        AccountServiceConfig config = new AccountServiceConfig(
                0, // unused on the in-process transport
                new AccountStoreConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(),
                        POSTGRES.getPassword()),
                AccountServiceConfig.INPROCESS_TARGET_PREFIX + REPO_INPROCESS,
                "localhost:1", // outbox on; the relay loop is never started
                null, null, true, -1L);
        services = AccountServices.build(config);
        services.startInProcess("it");
        channel = InProcessChannelBuilder.forName("it").build();
        accounts = AccountServiceGrpc.newBlockingStub(channel);
    }

    @AfterAll
    static void tearDown() {
        channel.shutdownNow();
        services.close();
        fakeRepoServer.shutdownNow();
    }

    // ------------------------------------------------------------- helpers

    private static String uniqueId(String prefix) {
        return prefix + "-" + UUID.randomUUID().toString().substring(0, 8);
    }

    private Account create(String accountId) {
        return accounts.createAccount(CreateAccountRequest.newBuilder()
                .setAccountId(accountId)
                .setDisplayName("Display " + accountId)
                .putMetadata("suite", "it")
                .build()).getAccount();
    }

    private List<AccountEventRecord> outboxRowsFor(String accountId) {
        return services.database().inTransaction(c -> {
            try (var ps = c.prepareStatement(
                    "SELECT event_id, event_type, payload, kafka_key, attempts, status,"
                            + " created_at, published_at, last_error"
                            + " FROM account_events_outbox WHERE kafka_key = ?"
                            + " ORDER BY created_at ASC, event_id ASC")) {
                ps.setString(1, accountId);
                List<AccountEventRecord> rows = new ArrayList<>();
                try (var rs = ps.executeQuery()) {
                    while (rs.next()) {
                        AccountEventRecord r = new AccountEventRecord();
                        r.eventId = rs.getObject("event_id", UUID.class);
                        r.eventType = rs.getString("event_type");
                        r.payload = rs.getBytes("payload");
                        r.kafkaKey = rs.getString("kafka_key");
                        r.status = rs.getString("status");
                        rows.add(r);
                    }
                }
                return rows;
            } catch (java.sql.SQLException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private static void assertDriveCalls(List<CreateDriveRequest> calls, String accountId) {
        assertThat(calls).hasSize(2);
        assertThat(calls.stream().map(CreateDriveRequest::getName))
                .containsExactlyInAnyOrder("intake", "pipeline");
        assertThat(calls).allSatisfy(r -> assertThat(r.getAccountId()).isEqualTo(accountId));
        assertThat(calls.stream().map(CreateDriveRequest::getDriveType))
                .containsExactlyInAnyOrder(DriveType.DRIVE_TYPE_INTAKE,
                        DriveType.DRIVE_TYPE_PIPELINE);
    }

    // ---------------------------------------------------------------- tests

    @Test
    void createProvisionsDrivesAndOutboxesTheEvent() throws Exception {
        String accountId = uniqueId("acct-create");
        Account created = create(accountId);

        assertThat(created.getAccountId()).isEqualTo(accountId);
        assertThat(created.getStatus()).isEqualTo(AccountStatus.ACCOUNT_STATUS_ACTIVE);
        assertThat(created.getDisplayName()).isEqualTo("Display " + accountId);
        assertThat(created.getMetadataMap()).containsEntry("suite", "it");
        assertThat(created.getCreatedAt().getSeconds()).isPositive();
        // A fresh row: updated_at is the create stamp (one INSERT, one now()).
        assertThat(created.getUpdatedAt()).isEqualTo(created.getCreatedAt());

        // Drive provisioning happened, for exactly intake + pipeline.
        assertDriveCalls(fakeDrives.callsFor(accountId), accountId);

        // Get reads the same row back.
        Account got = accounts.getAccount(
                GetAccountRequest.newBuilder().setAccountId(accountId).build()).getAccount();
        assertThat(got.getStatus()).isEqualTo(AccountStatus.ACCOUNT_STATUS_ACTIVE);

        // The AccountCreated event landed in the SAME transaction: visible
        // immediately, PENDING, keyed by the account id, and its payload
        // parses back to the created arm.
        List<AccountEventRecord> rows = outboxRowsFor(accountId);
        assertThat(rows).hasSize(1);
        AccountEventRecord row = rows.get(0);
        assertThat(row.eventType).isEqualTo(AccountEventRecord.TYPE_CREATED);
        assertThat(row.status).isEqualTo(AccountEventRecord.STATUS_PENDING);
        assertThat(row.kafkaKey).isEqualTo(accountId);
        AccountEvent event = AccountEvent.parseFrom(row.payload);
        assertThat(event.getEventId()).isEqualTo(row.eventId.toString());
        assertThat(event.getCreated().getAccountId()).isEqualTo(accountId);
        assertThat(event.getCreated().getDisplayName()).isEqualTo("Display " + accountId);
        assertThat(event.getCreated().getOccurredAt().getSeconds()).isPositive();
    }

    @Test
    void duplicateCreateIsAlreadyExistsAndFiresNoSecondEvent() {
        String accountId = uniqueId("acct-dup");
        create(accountId);

        assertThatThrownBy(() -> create(accountId))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e ->
                        assertThat(e.getStatus().getCode())
                                .isEqualTo(Status.Code.ALREADY_EXISTS));

        assertThat(outboxRowsFor(accountId)).hasSize(1);
    }

    @Test
    void concurrentCreateSettlesExactlyOneWinner() throws Exception {
        // N racing creates of the same id: the unique constraint picks one
        // winner, every loser gets ALREADY_EXISTS, and exactly one account
        // row and one outbox event commit.
        String accountId = uniqueId("acct-race");
        int racers = 8;
        List<Status.Code> outcomes = new CopyOnWriteArrayList<>();
        try (var executor = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            List<java.util.concurrent.Future<?>> futures = new ArrayList<>();
            for (int i = 0; i < racers; i++) {
                futures.add(executor.submit(() -> {
                    try {
                        create(accountId);
                        outcomes.add(Status.Code.OK);
                    } catch (StatusRuntimeException e) {
                        outcomes.add(e.getStatus().getCode());
                    }
                }));
            }
            for (var future : futures) {
                future.get();
            }
        }
        assertThat(outcomes.stream().filter(code -> code == Status.Code.OK).count()).isEqualTo(1);
        assertThat(outcomes.stream().filter(code -> code == Status.Code.ALREADY_EXISTS).count())
                .isEqualTo(racers - 1);

        Account winner = accounts.getAccount(
                GetAccountRequest.newBuilder().setAccountId(accountId).build()).getAccount();
        assertThat(winner.getStatus()).isEqualTo(AccountStatus.ACCOUNT_STATUS_ACTIVE);
        assertThat(outboxRowsFor(accountId)).hasSize(1);
    }

    @Test
    void getAndValidationErrors() {
        assertThatThrownBy(() -> accounts.getAccount(
                GetAccountRequest.newBuilder().setAccountId(uniqueId("ghost")).build()))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e ->
                        assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.NOT_FOUND));

        // A blank account id is INVALID_ARGUMENT on the read path too, not
        // just on create.
        assertThatThrownBy(() -> accounts.getAccount(
                GetAccountRequest.newBuilder().setAccountId("  ").build()))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e ->
                        assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT));

        assertThatThrownBy(() -> accounts.createAccount(
                CreateAccountRequest.getDefaultInstance()))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e -> {
                    assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
                    assertThat(e.getStatus().getDescription()).contains("account_id");
                });

        // Transitions on a missing account are NOT_FOUND (both the peeked
        // activation path and the locked deactivation path).
        assertThatThrownBy(() -> accounts.activateAccount(ActivateAccountRequest.newBuilder()
                .setAccountId(uniqueId("ghost")).build()))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e ->
                        assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.NOT_FOUND));
        assertThatThrownBy(() -> accounts.deactivateAccount(DeactivateAccountRequest.newBuilder()
                .setAccountId(uniqueId("ghost")).build()))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e ->
                        assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.NOT_FOUND));

        assertThatThrownBy(() -> accounts.listAccounts(ListAccountsRequest.newBuilder()
                .setContinuationToken("not-a-number").build()))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e ->
                        assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT));

        // A negative offset is rejected, not silently clamped.
        assertThatThrownBy(() -> accounts.listAccounts(ListAccountsRequest.newBuilder()
                .setContinuationToken("-7").build()))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e -> {
                    assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
                    assertThat(e.getStatus().getDescription()).contains("continuation_token");
                });
    }

    @Test
    void listPaginatesWithContinuationTokens() {
        // A fresh status cohort the test owns: three accounts, deactivated,
        // listed through the status filter so other tests' rows cannot
        // interfere.
        String a = uniqueId("acct-page-a");
        String b = uniqueId("acct-page-b");
        String c = uniqueId("acct-page-c");
        List<String> own = List.of(a, b, c);
        for (String id : own) {
            create(id);
            accounts.deactivateAccount(DeactivateAccountRequest.newBuilder()
                    .setAccountId(id).build());
        }

        // Drain the cohort through the tokens until they run out: every page
        // honors the limit, no page repeats a row, and the drain covers
        // exactly totalCount rows — the token arithmetic terminates.
        List<Account> seen = new ArrayList<>();
        String token = "";
        long totalCount = -1;
        for (int pages = 0; ; pages++) {
            assertThat(pages).as("paging must terminate").isLessThan(50);
            ListAccountsResponse page = accounts.listAccounts(ListAccountsRequest.newBuilder()
                    .setStatusFilter(AccountStatus.ACCOUNT_STATUS_DEACTIVATED)
                    .setLimit(2)
                    .setContinuationToken(token)
                    .build());
            assertThat(page.getAccountsCount()).isLessThanOrEqualTo(2);
            assertThat(page.getAccountsList()).allSatisfy(acct ->
                    assertThat(acct.getStatus()).isEqualTo(AccountStatus.ACCOUNT_STATUS_DEACTIVATED));
            if (totalCount < 0) {
                totalCount = page.getTotalCount();
                assertThat(totalCount).isGreaterThanOrEqualTo(3);
            }
            seen.addAll(page.getAccountsList());
            if (page.getNextContinuationToken().isEmpty()) {
                break;
            }
            token = page.getNextContinuationToken();
        }
        assertThat(seen).hasSize((int) totalCount);
        assertThat(seen.stream().map(Account::getAccountId).toList()).doesNotHaveDuplicates();

        // The three rows this test owns all surfaced, status and metadata
        // round-tripped through the list projection.
        List<Account> mine = seen.stream()
                .filter(acct -> own.contains(acct.getAccountId()))
                .toList();
        assertThat(mine).hasSize(3);
        assertThat(mine).allSatisfy(acct -> {
            assertThat(acct.getDisplayName()).isEqualTo("Display " + acct.getAccountId());
            assertThat(acct.getMetadataMap()).containsEntry("suite", "it");
            assertThat(acct.getCreatedAt().getSeconds()).isPositive();
        });
    }

    @Test
    void listAccountsOnUnpopulatedStatusFilterIsEmpty() {
        // No wire path sets SUSPENDED, so this cohort is deterministically
        // empty: the empty page is honest (zero rows, zero total, no token).
        ListAccountsResponse page = accounts.listAccounts(ListAccountsRequest.newBuilder()
                .setStatusFilter(AccountStatus.ACCOUNT_STATUS_SUSPENDED)
                .setLimit(10)
                .build());
        assertThat(page.getAccountsCount()).isZero();
        assertThat(page.getTotalCount()).isZero();
        assertThat(page.getNextContinuationToken()).isEmpty();
    }

    @Test
    void deactivateThenActivateTransitionsAndReprovisions() throws Exception {
        String accountId = uniqueId("acct-cycle");
        create(accountId);
        int driveCallsAfterCreate = fakeDrives.callsFor(accountId).size();

        Account deactivated = accounts.deactivateAccount(
                DeactivateAccountRequest.newBuilder().setAccountId(accountId).build()).getAccount();
        assertThat(deactivated.getStatus()).isEqualTo(AccountStatus.ACCOUNT_STATUS_DEACTIVATED);
        // Deactivation provisions nothing.
        assertThat(fakeDrives.callsFor(accountId)).hasSize(driveCallsAfterCreate);

        // Deactivating again is a no-op: same state, no new event.
        Account deactivatedAgain = accounts.deactivateAccount(
                DeactivateAccountRequest.newBuilder().setAccountId(accountId).build()).getAccount();
        assertThat(deactivatedAgain.getStatus()).isEqualTo(AccountStatus.ACCOUNT_STATUS_DEACTIVATED);
        assertThat(outboxRowsFor(accountId)).hasSize(2); // created + deactivated

        Account activated = accounts.activateAccount(
                ActivateAccountRequest.newBuilder().setAccountId(accountId).build()).getAccount();
        assertThat(activated.getStatus()).isEqualTo(AccountStatus.ACCOUNT_STATUS_ACTIVE);
        // The transition stamps updated_at; a no-op read cannot explain it.
        assertThat(activated.getUpdatedAt().getSeconds())
                .isGreaterThanOrEqualTo(activated.getCreatedAt().getSeconds());
        // Activation re-ensures the drives (idempotent on repo's side).
        assertDriveCalls(fakeDrives.callsFor(accountId)
                .subList(driveCallsAfterCreate, fakeDrives.callsFor(accountId).size()), accountId);

        // The full event trail: created, deactivated, activated — in order,
        // and each payload carries the snapshot its consumers need.
        List<AccountEventRecord> rows = outboxRowsFor(accountId);
        assertThat(rows.stream().map(r -> r.eventType))
                .containsExactly(AccountEventRecord.TYPE_CREATED,
                        AccountEventRecord.TYPE_DEACTIVATED,
                        AccountEventRecord.TYPE_ACTIVATED);

        AccountEvent deactivatedEvent = AccountEvent.parseFrom(rows.get(1).payload);
        assertThat(deactivatedEvent.getEventId()).isEqualTo(rows.get(1).eventId.toString());
        assertThat(deactivatedEvent.getDeactivated().getAccountId()).isEqualTo(accountId);
        assertThat(deactivatedEvent.getDeactivated().getOccurredAt().getSeconds()).isPositive();
        assertThat(deactivatedEvent.getDeactivated().getMetadataMap())
                .containsEntry("suite", "it");

        AccountEvent activatedEvent = AccountEvent.parseFrom(rows.get(2).payload);
        assertThat(activatedEvent.getEventId()).isEqualTo(rows.get(2).eventId.toString());
        assertThat(activatedEvent.getActivated().getAccountId()).isEqualTo(accountId);
        assertThat(activatedEvent.getActivated().getOccurredAt().getSeconds()).isPositive();
        assertThat(activatedEvent.getActivated().getMetadataMap())
                .containsEntry("suite", "it");
    }

    @Test
    void reactivatingAnActiveAccountIsAFullNoOp() {
        String accountId = uniqueId("acct-noop");
        create(accountId);
        int driveCallsAfterCreate = fakeDrives.callsFor(accountId).size();

        Account again = accounts.activateAccount(
                ActivateAccountRequest.newBuilder().setAccountId(accountId).build()).getAccount();
        assertThat(again.getStatus()).isEqualTo(AccountStatus.ACCOUNT_STATUS_ACTIVE);

        // Idempotent re-activation duplicates NOTHING: no extra CreateDrive
        // calls, no second ACTIVATED event.
        assertThat(fakeDrives.callsFor(accountId)).hasSize(driveCallsAfterCreate);
        assertThat(outboxRowsFor(accountId)).hasSize(1);
    }

    @Test
    void unreachableRepoFailsCreateWithoutCommitting() {
        // A second service set pointed at a repo target that does not exist:
        // provisioning must fail the create BEFORE anything commits.
        AccountServiceConfig downConfig = new AccountServiceConfig(0,
                new AccountStoreConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(),
                        POSTGRES.getPassword()),
                AccountServiceConfig.INPROCESS_TARGET_PREFIX + "no-such-repo",
                null, null, null, true, -1L);
        try (AccountServices down = AccountServices.build(downConfig)) {
            down.startInProcess("it-repo-down");
            ManagedChannel downChannel = InProcessChannelBuilder.forName("it-repo-down").build();
            AccountServiceGrpc.AccountServiceBlockingStub downAccounts =
                    AccountServiceGrpc.newBlockingStub(downChannel);
            String accountId = uniqueId("acct-outage");

            assertThatThrownBy(() -> downAccounts.createAccount(CreateAccountRequest.newBuilder()
                    .setAccountId(accountId).build()))
                    .isInstanceOfSatisfying(StatusRuntimeException.class, e ->
                            assertThat(e.getStatus().getCode())
                                    .isEqualTo(Status.Code.UNAVAILABLE));

            // Nothing committed: the account does not exist, no outbox row.
            assertThatThrownBy(() -> downAccounts.getAccount(
                    GetAccountRequest.newBuilder().setAccountId(accountId).build()))
                    .isInstanceOfSatisfying(StatusRuntimeException.class, e ->
                            assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.NOT_FOUND));
            assertThat(outboxRowsFor(accountId)).isEmpty();
            downChannel.shutdownNow();
        }
    }

    @Test
    void provisioningFailureMidwayFailsCreateCleanly() throws Exception {
        // A repo whose CreateDrive fails on the pipeline drive only: the
        // intake drive lands on repo's side, but the account commit must not
        // — and the recorded calls show the provisioner stopped at the
        // failure (intake attempted, pipeline attempted, nothing after).
        List<String> attemptedDrives = new CopyOnWriteArrayList<>();
        DriveServiceGrpc.DriveServiceImplBase halfDown =
                new DriveServiceGrpc.DriveServiceImplBase() {
            @Override
            public void createDrive(CreateDriveRequest request,
                    StreamObserver<CreateDriveResponse> responseObserver) {
                attemptedDrives.add(request.getName());
                if (request.getName().equals("pipeline")) {
                    responseObserver.onError(Status.INTERNAL
                            .withDescription("pipeline store wedged").asRuntimeException());
                    return;
                }
                responseObserver.onNext(CreateDriveResponse.newBuilder()
                        .setDrive(Drive.newBuilder()
                                .setDriveId("drive-" + request.getAccountId() + "-"
                                        + request.getName())
                                .setName(request.getName())
                                .setAccountId(request.getAccountId())
                                .setDriveType(request.getDriveType())
                                .setCreatedAt(Timestamp.getDefaultInstance()))
                        .build());
                responseObserver.onCompleted();
            }
        };
        Server halfDownServer = InProcessServerBuilder.forName("it-repo-half-down")
                .addService(halfDown)
                .directExecutor()
                .build()
                .start();
        try {
            AccountServiceConfig halfConfig = new AccountServiceConfig(0,
                    new AccountStoreConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(),
                            POSTGRES.getPassword()),
                    AccountServiceConfig.INPROCESS_TARGET_PREFIX + "it-repo-half-down",
                    "localhost:1", null, null, true, -1L);
            try (AccountServices half = AccountServices.build(halfConfig)) {
                half.startInProcess("it-half-down");
                ManagedChannel halfChannel =
                        InProcessChannelBuilder.forName("it-half-down").build();
                AccountServiceGrpc.AccountServiceBlockingStub halfAccounts =
                        AccountServiceGrpc.newBlockingStub(halfChannel);
                String accountId = uniqueId("acct-half");

                assertThatThrownBy(() -> halfAccounts.createAccount(
                        CreateAccountRequest.newBuilder().setAccountId(accountId).build()))
                        .isInstanceOfSatisfying(StatusRuntimeException.class, e -> {
                            assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.UNAVAILABLE);
                            assertThat(e.getStatus().getDescription()).contains(accountId);
                        });
                assertThat(attemptedDrives).containsExactly("intake", "pipeline");

                // Nothing committed: a retried create (against a healed repo)
                // converges instead of colliding with a half-written account.
                assertThatThrownBy(() -> halfAccounts.getAccount(
                        GetAccountRequest.newBuilder().setAccountId(accountId).build()))
                        .isInstanceOfSatisfying(StatusRuntimeException.class, e ->
                                assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.NOT_FOUND));
                assertThat(outboxRowsFor(accountId)).isEmpty();
                halfChannel.shutdownNow();
            }
        } finally {
            halfDownServer.shutdownNow();
        }
    }

    @Test
    void eventingOffSkipsTheOutboxEntirely() {
        AccountServiceConfig offConfig = new AccountServiceConfig(0,
                new AccountStoreConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(),
                        POSTGRES.getPassword()),
                AccountServiceConfig.INPROCESS_TARGET_PREFIX + REPO_INPROCESS,
                null, null, null, true, -1L);
        try (AccountServices off = AccountServices.build(offConfig)) {
            assertThat(off.eventOutbox()).isNull();
            off.startInProcess("it-no-kafka");
            ManagedChannel offChannel = InProcessChannelBuilder.forName("it-no-kafka").build();
            AccountServiceGrpc.AccountServiceBlockingStub offAccounts =
                    AccountServiceGrpc.newBlockingStub(offChannel);
            String accountId = uniqueId("acct-silent");

            Account created = offAccounts.createAccount(CreateAccountRequest.newBuilder()
                    .setAccountId(accountId).build()).getAccount();
            assertThat(created.getStatus()).isEqualTo(AccountStatus.ACCOUNT_STATUS_ACTIVE);
            assertThat(outboxRowsFor(accountId)).isEmpty();
            offChannel.shutdownNow();
        }
    }

    @Test
    void accountIdIsTrimmedOnTheWayIn() {
        // Whitespace around a caller-minted id is not part of the key: the
        // row, the drive calls, and every later lookup use the trimmed form.
        String accountId = uniqueId("acct-padded");
        Account created = accounts.createAccount(CreateAccountRequest.newBuilder()
                .setAccountId("  " + accountId + "  ")
                .build()).getAccount();
        assertThat(created.getAccountId()).isEqualTo(accountId);

        // Provisioning saw the trimmed id, not the padded one.
        assertThat(fakeDrives.callsFor(accountId)).hasSize(2);

        // And reads trim too: the padded form finds the same row.
        Account got = accounts.getAccount(GetAccountRequest.newBuilder()
                .setAccountId(" " + accountId + " ")
                .build()).getAccount();
        assertThat(got.getAccountId()).isEqualTo(accountId);
    }

    @Test
    void listDefaultsToOneHundredAndClampsAtOneThousand() {
        // Seed more than MAX_LIST_LIMIT rows directly through the store (no
        // RPC ceremony, no outbox rows): 1001 ACTIVE accounts. The cohort is
        // shared with the other tests, so every assertion is a page-size one
        // — deterministic no matter what the other rows are.
        String prefix = "acct-flood-" + UUID.randomUUID().toString().substring(0, 8) + "-";
        services.database().inTransaction(c -> {
            for (int i = 0; i < 1001; i++) {
                AccountRecord record = new AccountRecord();
                record.accountId = prefix + i;
                record.displayName = "flood " + i;
                record.status = AccountStatus.ACCOUNT_STATUS_ACTIVE;
                services.accountStore().create(c, record);
            }
            return null;
        });

        // No limit on the wire → the DEFAULT_LIST_LIMIT (100) applies.
        ListAccountsResponse defaultPage = accounts.listAccounts(
                ListAccountsRequest.newBuilder()
                        .setStatusFilter(AccountStatus.ACCOUNT_STATUS_ACTIVE)
                        .build());
        assertThat(defaultPage.getAccountsCount()).isEqualTo(100);
        assertThat(defaultPage.getTotalCount()).isGreaterThanOrEqualTo(1001);
        assertThat(defaultPage.getNextContinuationToken()).isEqualTo("100");

        // A limit past MAX_LIST_LIMIT is clamped to 1000, not honored.
        ListAccountsResponse clamped = accounts.listAccounts(
                ListAccountsRequest.newBuilder()
                        .setStatusFilter(AccountStatus.ACCOUNT_STATUS_ACTIVE)
                        .setLimit(Integer.MAX_VALUE)
                        .build());
        assertThat(clamped.getAccountsCount()).isEqualTo(1000);
        assertThat(clamped.getNextContinuationToken()).isEqualTo("1000");

        // The clamped token resumes the drain where the clamp left off.
        ListAccountsResponse resumed = accounts.listAccounts(
                ListAccountsRequest.newBuilder()
                        .setStatusFilter(AccountStatus.ACCOUNT_STATUS_ACTIVE)
                        .setLimit(Integer.MAX_VALUE)
                        .setContinuationToken(clamped.getNextContinuationToken())
                        .build());
        assertThat(resumed.getAccountsCount()).isGreaterThanOrEqualTo(1);
    }
}
