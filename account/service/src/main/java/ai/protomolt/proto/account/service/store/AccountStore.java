package ai.protomolt.proto.account.service.store;

import ai.protomolt.proto.account.v1.AccountStatus;

import java.sql.Connection;
import java.time.Instant;
import java.util.Collection;
import java.util.Optional;

/**
 * The AccountStore SPI — the seam between the account lifecycle and where
 * account rows actually live. Postgres is the default
 * ({@link JdbcAccountStore}); other backends slot in here.
 * <p>
 * Transaction contract (this is the point of the shape): every method rides
 * the CALLER's {@link Connection}. The store never opens or commits a
 * connection itself, because an account mutation and its outbox event must
 * commit atomically — the gRPC layer composes both inside one
 * {@link AccountDatabase#inTransaction} unit of work. Callers run on virtual
 * threads; every unit of work is short, blocking JDBC, and a transaction is
 * never held open across the repo-service drive-provisioning RPC.
 */
public interface AccountStore {

    /**
     * Insert a new account row.
     *
     * @param c the caller's connection (its transaction is the commit unit)
     * @param record the account to store ({@code createdAt}/{@code updatedAt}
     *        are read back from the database onto the returned record)
     * @return the stored row
     * @throws AccountStoreException with {@link AccountStoreException.Kind#CONFLICT}
     *         when the account id is taken
     */
    AccountRecord create(Connection c, AccountRecord record);

    /**
     * Fetch an account by id.
     *
     * @param c the caller's connection
     * @param accountId the tenancy key
     * @return the row, or empty
     */
    Optional<AccountRecord> findById(Connection c, String accountId);

    /**
     * Fetch an account by id, locking the row ({@code FOR UPDATE}) until the
     * caller's transaction ends. Status transitions go through this so two
     * concurrent transitions cannot interleave into a lost update or a
     * duplicated outbox event.
     *
     * @param c the caller's connection
     * @param accountId the tenancy key
     * @return the row, or empty
     */
    Optional<AccountRecord> findByIdForUpdate(Connection c, String accountId);

    /**
     * One page of accounts in stable (created_at, account_id) order.
     *
     * @param c the caller's connection
     * @param statusFilter only rows in this status; null = every status
     * @param limit page size
     * @param offset rows to skip (the continuation token's payload)
     * @return the page plus the total across all pages
     */
    ListAccountsResult list(Connection c, AccountStatus statusFilter, int limit, long offset);

    /**
     * One page of the accounts among {@code accountIds}, in the same stable
     * (created_at, account_id) order, status filter, and limit/offset
     * semantics as {@link #list}. The restriction is part of the query, so
     * the total and the continuation arithmetic count only the named
     * accounts; an id with no row contributes nothing.
     *
     * @param c the caller's connection
     * @param accountIds the accounts the page may hold; empty yields no rows
     * @param statusFilter only rows in this status; null = every status
     * @param limit page size
     * @param offset rows to skip (the continuation token's payload)
     * @return the page plus the total across all pages of the restricted set
     */
    ListAccountsResult listWithin(Connection c, Collection<String> accountIds,
            AccountStatus statusFilter, int limit, long offset);

    /**
     * Set an account's status and {@code updated_at}. The caller is expected
     * to hold the row lock ({@link #findByIdForUpdate}) and to have decided
     * the transition is real — this method writes unconditionally.
     *
     * @param c the caller's connection
     * @param accountId the tenancy key
     * @param to the new status
     * @param when the transition instant
     * @return the updated row
     */
    AccountRecord updateStatus(Connection c, String accountId, AccountStatus to, Instant when);
}
