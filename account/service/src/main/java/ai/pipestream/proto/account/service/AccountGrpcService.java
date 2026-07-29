package ai.pipestream.proto.account.service;

import ai.pipestream.proto.account.service.events.AccountEventFactory;
import ai.pipestream.proto.account.service.events.JdbcAccountEventOutbox;
import ai.pipestream.proto.account.service.provision.DriveProvisioner;
import ai.pipestream.proto.account.service.store.AccountDatabase;
import ai.pipestream.proto.account.service.store.AccountRecord;
import ai.pipestream.proto.account.service.store.AccountStore;
import ai.pipestream.proto.account.service.store.AccountStoreException;
import ai.pipestream.proto.account.service.store.ListAccountsResult;
import ai.pipestream.proto.account.v1.AccountServiceGrpc;
import ai.pipestream.proto.account.v1.AccountStatus;
import ai.pipestream.proto.account.v1.ActivateAccountRequest;
import ai.pipestream.proto.account.v1.ActivateAccountResponse;
import ai.pipestream.proto.account.v1.CreateAccountRequest;
import ai.pipestream.proto.account.v1.CreateAccountResponse;
import ai.pipestream.proto.account.v1.DeactivateAccountRequest;
import ai.pipestream.proto.account.v1.DeactivateAccountResponse;
import ai.pipestream.proto.account.v1.GetAccountRequest;
import ai.pipestream.proto.account.v1.GetAccountResponse;
import ai.pipestream.proto.account.v1.ListAccountsRequest;
import ai.pipestream.proto.account.v1.ListAccountsResponse;
import io.grpc.stub.StreamObserver;

import java.time.Instant;

import static ai.pipestream.proto.account.service.GrpcErrors.invalidArgument;

/**
 * The AccountService gRPC implementation: blocking handlers (the server's
 * call executor is virtual-thread-per-task) over the {@link AccountStore}
 * SPI, with drive provisioning and the transactional outbox composed around
 * the store mutations.
 * <p>
 * Ordering contract, the same at every commit point:
 * <ol>
 *   <li>repo-service calls (drive provisioning) happen FIRST, outside any
 *   database transaction — an unreachable repo fails the RPC UNAVAILABLE
 *   before anything commits, and the retried call converges because
 *   CreateDrive is idempotent;</li>
 *   <li>the account mutation and its outbox event commit in ONE transaction
 *   — an event can never drift from the state change it describes;</li>
 *   <li>no-op transitions (activating an ACTIVE account, deactivating a
 *   DEACTIVATED one) write nothing and fire nothing — and skip the drive
 *   calls too, so idempotent re-activation provokes no repo traffic.</li>
 * </ol>
 */
public final class AccountGrpcService extends AccountServiceGrpc.AccountServiceImplBase {

    private static final int DEFAULT_LIST_LIMIT = 100;
    private static final int MAX_LIST_LIMIT = 1000;

    private final AccountDatabase database;
    private final AccountStore store;
    private final JdbcAccountEventOutbox outbox;
    private final DriveProvisioner provisioner;

    /**
     * @param database the account store's transaction wrapper
     * @param store the account store SPI implementation
     * @param outbox the account-events outbox, or null when eventing is off
     *        (no outbox writes; the mutation commits alone)
     * @param provisioner the repo-service drive provisioner
     */
    public AccountGrpcService(AccountDatabase database, AccountStore store,
            JdbcAccountEventOutbox outbox, DriveProvisioner provisioner) {
        this.database = database;
        this.store = store;
        this.outbox = outbox;
        this.provisioner = provisioner;
    }

    @Override
    public void createAccount(CreateAccountRequest request,
            StreamObserver<CreateAccountResponse> responseObserver) {
        GrpcErrors.run(responseObserver, () -> {
            String accountId = requireAccountId(request.getAccountId());
            // Provisioning precedes the commit: a repo outage fails the
            // create here and no account row lands (see class javadoc).
            provisioner.ensureAccountDrives(accountId);
            return database.inTransaction(c -> {
                AccountRecord record = new AccountRecord();
                record.accountId = accountId;
                record.displayName = request.getDisplayName();
                record.status = AccountStatus.ACCOUNT_STATUS_ACTIVE;
                record.metadata = request.getMetadataMap();
                AccountRecord stored = store.create(c, record);
                if (outbox != null) {
                    outbox.enqueue(c, AccountEventFactory.created(stored, Instant.now()));
                }
                return CreateAccountResponse.newBuilder().setAccount(stored.toProto()).build();
            });
        });
    }

    @Override
    public void getAccount(GetAccountRequest request,
            StreamObserver<GetAccountResponse> responseObserver) {
        GrpcErrors.run(responseObserver, () -> {
            String accountId = requireAccountId(request.getAccountId());
            AccountRecord record = database.readOnly(c -> store.findById(c, accountId))
                    .orElseThrow(() -> AccountStoreException.notFound(accountId));
            return GetAccountResponse.newBuilder().setAccount(record.toProto()).build();
        });
    }

    @Override
    public void listAccounts(ListAccountsRequest request,
            StreamObserver<ListAccountsResponse> responseObserver) {
        GrpcErrors.run(responseObserver, () -> {
            int limit = request.getLimit() <= 0 ? DEFAULT_LIST_LIMIT
                    : Math.min(request.getLimit(), MAX_LIST_LIMIT);
            long offset = parseContinuationToken(request.getContinuationToken());
            AccountStatus filter = request.getStatusFilter();
            ListAccountsResult result = database.readOnly(
                    c -> store.list(c, filter, limit, offset));

            ListAccountsResponse.Builder response = ListAccountsResponse.newBuilder()
                    .setTotalCount(result.totalCount());
            result.rows().forEach(row -> response.addAccounts(row.toProto()));
            long nextOffset = offset + result.rows().size();
            if (nextOffset < result.totalCount()) {
                response.setNextContinuationToken(String.valueOf(nextOffset));
            }
            return response.build();
        });
    }

    @Override
    public void activateAccount(ActivateAccountRequest request,
            StreamObserver<ActivateAccountResponse> responseObserver) {
        GrpcErrors.run(responseObserver, () -> {
            String accountId = requireAccountId(request.getAccountId());
            // The no-op check rides the row lock inside the transaction, but
            // the drive calls are only honest when a transition actually
            // happens — so peek first, cheaply. The locked re-check inside
            // the transaction is the authoritative one.
            AccountRecord current = database.readOnly(c -> store.findById(c, accountId))
                    .orElseThrow(() -> AccountStoreException.notFound(accountId));
            if (current.status != AccountStatus.ACCOUNT_STATUS_ACTIVE) {
                // Re-activation re-ensures the drives: idempotent on repo's
                // side, and heals an account created while repo was
                // unreachable. Before the commit, like creation.
                provisioner.ensureAccountDrives(accountId);
            }
            return database.inTransaction(c -> {
                AccountRecord locked = store.findByIdForUpdate(c, accountId)
                        .orElseThrow(() -> AccountStoreException.notFound(accountId));
                if (locked.status == AccountStatus.ACCOUNT_STATUS_ACTIVE) {
                    return ActivateAccountResponse.newBuilder()
                            .setAccount(locked.toProto()).build();
                }
                AccountRecord updated = store.updateStatus(c, accountId,
                        AccountStatus.ACCOUNT_STATUS_ACTIVE, Instant.now());
                if (outbox != null) {
                    outbox.enqueue(c, AccountEventFactory.activated(updated, Instant.now()));
                }
                return ActivateAccountResponse.newBuilder().setAccount(updated.toProto()).build();
            });
        });
    }

    @Override
    public void deactivateAccount(DeactivateAccountRequest request,
            StreamObserver<DeactivateAccountResponse> responseObserver) {
        GrpcErrors.run(responseObserver, () -> {
            String accountId = requireAccountId(request.getAccountId());
            return database.inTransaction(c -> {
                AccountRecord locked = store.findByIdForUpdate(c, accountId)
                        .orElseThrow(() -> AccountStoreException.notFound(accountId));
                if (locked.status == AccountStatus.ACCOUNT_STATUS_DEACTIVATED) {
                    return DeactivateAccountResponse.newBuilder()
                            .setAccount(locked.toProto()).build();
                }
                AccountRecord updated = store.updateStatus(c, accountId,
                        AccountStatus.ACCOUNT_STATUS_DEACTIVATED, Instant.now());
                if (outbox != null) {
                    outbox.enqueue(c, AccountEventFactory.deactivated(updated, Instant.now()));
                }
                return DeactivateAccountResponse.newBuilder().setAccount(updated.toProto()).build();
            });
        });
    }

    private static String requireAccountId(String accountId) {
        if (accountId == null || accountId.isBlank()) {
            throw invalidArgument("account_id is required");
        }
        return accountId.trim();
    }

    private static long parseContinuationToken(String token) {
        if (token == null || token.isBlank()) {
            return 0;
        }
        try {
            long offset = Long.parseLong(token.trim());
            if (offset < 0) {
                throw invalidArgument("continuation_token must be a non-negative row offset");
            }
            return offset;
        } catch (NumberFormatException e) {
            throw invalidArgument("continuation_token must be a non-negative row offset");
        }
    }
}
