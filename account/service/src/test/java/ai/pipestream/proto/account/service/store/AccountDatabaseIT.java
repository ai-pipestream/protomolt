package ai.pipestream.proto.account.service.store;

import ai.pipestream.proto.account.v1.AccountStatus;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@link AccountDatabase}'s transaction wrapper against a real testcontainers
 * PostgreSQL 17: the commit/rollback contract every commit point relies on
 * (RuntimeException → rollback + rethrow intact, a loser's writes vanish),
 * the read-only lane's error wrap, the fail-fast boot, and the datasource
 * accessor. The store SQL itself is {@code JdbcAccountStoreIT}'s territory.
 */
@Testcontainers(disabledWithoutDocker = true)
class AccountDatabaseIT {

    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:17-alpine");

    static AccountDatabase database;
    static JdbcAccountStore store;

    @BeforeAll
    static void boot() {
        database = new AccountDatabase(new AccountStoreConfig(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()));
        store = new JdbcAccountStore();
    }

    @AfterAll
    static void tearDown() {
        database.close();
    }

    @BeforeEach
    void clean() {
        database.inTransaction(c -> {
            try {
                c.createStatement().execute("DELETE FROM accounts");
            } catch (java.sql.SQLException e) {
                throw new RuntimeException(e);
            }
            return null;
        });
    }

    private static AccountRecord newAccount(String accountId) {
        AccountRecord record = new AccountRecord();
        record.accountId = accountId;
        record.displayName = accountId;
        record.status = AccountStatus.ACCOUNT_STATUS_ACTIVE;
        return record;
    }

    @Test
    void runtimeExceptionRollsBackAndPropagatesIntact() {
        RuntimeException marker = new RuntimeException("force rollback");

        assertThatThrownBy(() -> database.inTransaction(c -> {
            store.create(c, newAccount("acct-rolled-back"));
            throw marker;
        })).isSameAs(marker);

        // The insert went out with the transaction: nothing is visible.
        Optional<AccountRecord> rolledBack =
                database.readOnly(c -> store.findById(c, "acct-rolled-back"));
        assertThat(rolledBack).isEmpty();
    }

    @Test
    void conflictMidTransactionRollsBackTheEarlierWrites() {
        // The second create collides: the WHOLE unit of work rolls back, so
        // even the first (valid) insert never commits — the atomicity the
        // outbox co-commit relies on.
        assertThatThrownBy(() -> database.inTransaction(c -> {
            store.create(c, newAccount("acct-atomic"));
            store.create(c, newAccount("acct-atomic"));
            return null;
        })).isInstanceOfSatisfying(AccountStoreException.class, e ->
                assertThat(e.kind()).isEqualTo(AccountStoreException.Kind.CONFLICT));

        Optional<AccountRecord> atomic = database.readOnly(c -> store.findById(c, "acct-atomic"));
        assertThat(atomic).isEmpty();
    }

    @Test
    void committedWorkIsVisibleAfterwards() {
        AccountRecord stored = database.inTransaction(c -> store.create(c, newAccount("acct-commit")));
        assertThat(stored.createdAt).isNotNull();

        AccountRecord found = database.readOnly(c -> store.findById(c, "acct-commit"))
                .orElseThrow();
        assertThat(found.accountId).isEqualTo("acct-commit");
    }

    @Test
    void readOnlyWrapsSqlFailuresAsUnclassifiedStoreFailures() {
        assertThatThrownBy(() -> database.readOnly(c -> {
            try {
                c.createStatement().execute("SELECT * FROM no_such_table");
            } catch (java.sql.SQLException e) {
                throw AccountStoreException.wrap("query failed", e);
            }
            return null;
        })).isInstanceOfSatisfying(AccountStoreException.class, e ->
                assertThat(e.kind()).isEqualTo(AccountStoreException.Kind.NONE));
    }

    @Test
    void bootFailsFastWhenTheDatabaseIsUnreachable() {
        // Port 1 answers nothing: Flyway's migrate must blow up construction,
        // and the half-started pool must not leak (the constructor closes it).
        AccountStoreConfig dead = new AccountStoreConfig(
                "jdbc:postgresql://localhost:1/accounts", "accounts", "accounts");
        assertThatThrownBy(() -> new AccountDatabase(dead))
                .isInstanceOf(RuntimeException.class);
    }

    @Test
    void dataSourceIsTheLivePool() {
        assertThat(database.dataSource()).isInstanceOfSatisfying(HikariDataSource.class, ds -> {
            assertThat(ds.getPoolName()).isEqualTo("account-store");
            assertThat(ds.isClosed()).isFalse();
        });
    }
}
