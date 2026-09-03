package ai.protomolt.proto.account.service.store;

import ai.protomolt.proto.account.v1.AccountStatus;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The Postgres default {@link AccountStore} against a real testcontainers
 * PostgreSQL 17 (Flyway-migrated schema): CRUD, the status transitions with
 * their row locking, and list paging. No mocks — the schema, the SQL, and
 * the JSONB metadata round-trip are all exercised as deployed.
 */
@Testcontainers(disabledWithoutDocker = true)
class JdbcAccountStoreIT {

    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

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

    private static AccountRecord newAccount(String accountId, String displayName) {
        AccountRecord record = new AccountRecord();
        record.accountId = accountId;
        record.displayName = displayName;
        record.status = AccountStatus.ACCOUNT_STATUS_ACTIVE;
        return record;
    }

    @Test
    void createAndFindRoundTrips() {
        AccountRecord created = database.inTransaction(c -> {
            AccountRecord record = newAccount("acct-1", "Acme Corp");
            record.metadata = Map.of("tier", "gold", "region", "eu");
            return store.create(c, record);
        });
        assertThat(created.createdAt).isNotNull();
        assertThat(created.updatedAt).isNotNull();

        Optional<AccountRecord> found = database.readOnly(c -> store.findById(c, "acct-1"));
        assertThat(found).isPresent();
        AccountRecord row = found.orElseThrow();
        assertThat(row.displayName).isEqualTo("Acme Corp");
        assertThat(row.status).isEqualTo(AccountStatus.ACCOUNT_STATUS_ACTIVE);
        assertThat(row.metadata).containsExactlyInAnyOrderEntriesOf(
                Map.of("tier", "gold", "region", "eu"));
        // timestamptz stores micros; the create response (full nanos) and the
        // re-fetched row agree at second granularity at least.
        assertThat(row.createdAt.getEpochSecond()).isEqualTo(created.createdAt.getEpochSecond());

        Optional<AccountRecord> missing = database.readOnly(c -> store.findById(c, "no-such"));
        assertThat(missing).isEmpty();
    }

    @Test
    void createWithoutMetadataStoresNull() {
        database.inTransaction(c -> store.create(c, newAccount("acct-bare", "")));
        AccountRecord row = database.readOnly(c -> store.findById(c, "acct-bare")).orElseThrow();
        assertThat(row.metadata).isEmpty();
        assertThat(row.displayName).isEmpty();
    }

    @Test
    void duplicateAccountIdConflicts() {
        database.inTransaction(c -> store.create(c, newAccount("acct-dup", "first")));
        assertThatThrownBy(() -> database.inTransaction(
                c -> store.create(c, newAccount("acct-dup", "second"))))
                .isInstanceOfSatisfying(AccountStoreException.class, e ->
                        assertThat(e.kind()).isEqualTo(AccountStoreException.Kind.CONFLICT));
        // The loser rolled back: the original row is intact.
        assertThat(database.readOnly(c -> store.findById(c, "acct-dup")).orElseThrow()
                .displayName).isEqualTo("first");
    }

    @Test
    void statusTransitionUpdatesRowAndTimestamp() {
        AccountRecord created = database.inTransaction(
                c -> store.create(c, newAccount("acct-status", "s")));
        // Pre-truncate to micros: timestamptz ROUNDS sub-micro digits rather
        // than truncating them, so a nano-precision clock (JDK 25+) would
        // otherwise store a value one micro off the truncated expectation.
        Instant deactivatedAt = Instant.now().truncatedTo(java.time.temporal.ChronoUnit.MICROS);
        AccountRecord deactivated = database.inTransaction(c -> {
            assertThat(store.findByIdForUpdate(c, "acct-status")).isPresent();
            return store.updateStatus(c, "acct-status",
                    AccountStatus.ACCOUNT_STATUS_DEACTIVATED, deactivatedAt);
        });
        assertThat(deactivated.status).isEqualTo(AccountStatus.ACCOUNT_STATUS_DEACTIVATED);
        // updateStatus writes the caller's instant (timestamptz keeps micros),
        // and it advanced past the create stamp.
        assertThat(deactivated.updatedAt)
                .isEqualTo(deactivatedAt.truncatedTo(java.time.temporal.ChronoUnit.MICROS));
        assertThat(deactivated.updatedAt).isAfter(created.updatedAt);
        // The transition touches updated_at only: created_at and the payload
        // columns are untouched.
        assertThat(deactivated.createdAt).isEqualTo(created.createdAt);
        assertThat(deactivated.displayName).isEqualTo("s");

        AccountRecord reactivated = database.inTransaction(c ->
                store.updateStatus(c, "acct-status", AccountStatus.ACCOUNT_STATUS_ACTIVE,
                        Instant.now()));
        assertThat(reactivated.status).isEqualTo(AccountStatus.ACCOUNT_STATUS_ACTIVE);

        // The store also carries the SUSPENDED hold the wire does not set yet.
        AccountRecord suspended = database.inTransaction(c ->
                store.updateStatus(c, "acct-status", AccountStatus.ACCOUNT_STATUS_SUSPENDED,
                        Instant.now()));
        assertThat(suspended.status).isEqualTo(AccountStatus.ACCOUNT_STATUS_SUSPENDED);
    }

    @Test
    void updateStatusOfMissingAccountIsNotFound() {
        assertThatThrownBy(() -> database.inTransaction(c ->
                store.updateStatus(c, "ghost", AccountStatus.ACCOUNT_STATUS_ACTIVE, Instant.now())))
                .isInstanceOfSatisfying(AccountStoreException.class, e ->
                        assertThat(e.kind()).isEqualTo(AccountStoreException.Kind.NOT_FOUND));
    }

    @Test
    void corruptStatusInDbIsAnUnclassifiedStoreFailure() {
        // A status string the enum doesn't know (constraint dropped by hand,
        // a manual edit) is a server-side data problem: the store must raise
        // an unclassified AccountStoreException — which GrpcErrors maps to
        // INTERNAL — never a raw IllegalArgumentException (→ INVALID_ARGUMENT,
        // blaming the client). Everything happens in one transaction that is
        // forced to roll back, so the CHECK constraint is restored.
        RuntimeException rollback = new RuntimeException("force rollback");
        try {
            database.inTransaction(c -> {
                try {
                    c.createStatement().execute(
                            "ALTER TABLE accounts DROP CONSTRAINT chk_accounts_status");
                    try (var ps = c.prepareStatement(
                            "INSERT INTO accounts (account_id, status)"
                                    + " VALUES ('acct-corrupt', 'BOGUS')")) {
                        ps.executeUpdate();
                    }
                } catch (java.sql.SQLException e) {
                    throw new RuntimeException(e);
                }
                assertThatThrownBy(() -> store.findById(c, "acct-corrupt"))
                        .isInstanceOfSatisfying(AccountStoreException.class, e ->
                                assertThat(e.kind()).isEqualTo(AccountStoreException.Kind.NONE));
                throw rollback;
            });
        } catch (RuntimeException e) {
            assertThat(e).isSameAs(rollback);
        }
        // The guardrail survived: the constraint still rejects bad writes.
        assertThatThrownBy(() -> database.inTransaction(c -> {
            try {
                c.createStatement().execute(
                        "INSERT INTO accounts (account_id, status)"
                                + " VALUES ('acct-corrupt-2', 'BOGUS')");
            } catch (java.sql.SQLException e) {
                throw new RuntimeException(e);
            }
            return null;
        })).isInstanceOf(RuntimeException.class)
                .hasCauseInstanceOf(java.sql.SQLException.class);
    }

    @Test
    void listPaginatesInStableOrderAndFiltersByStatus() {
        database.inTransaction(c -> {
            for (int i = 0; i < 5; i++) {
                store.create(c, newAccount("acct-list-" + i, "a" + i));
            }
            store.updateStatus(c, "acct-list-4", AccountStatus.ACCOUNT_STATUS_DEACTIVATED,
                    Instant.now());
            return null;
        });

        ListAccountsResult all = database.readOnly(c -> store.list(c, null, 100, 0));
        assertThat(all.totalCount()).isEqualTo(5);
        assertThat(all.rows()).hasSize(5);

        ListAccountsResult page1 = database.readOnly(c -> store.list(c, null, 2, 0));
        ListAccountsResult page2 = database.readOnly(c -> store.list(c, null, 2, 2));
        ListAccountsResult page3 = database.readOnly(c -> store.list(c, null, 2, 4));
        assertThat(page1.rows()).hasSize(2);
        assertThat(page2.rows()).hasSize(2);
        assertThat(page3.rows()).hasSize(1);
        // No page repeats a row, and the pages cover the whole set.
        assertThat(page1.rows().stream().map(r -> r.accountId).toList())
                .doesNotContainAnyElementsOf(page2.rows().stream().map(r -> r.accountId).toList());
        java.util.List<String> allIds = new java.util.ArrayList<>();
        page1.rows().forEach(r -> allIds.add(r.accountId));
        page2.rows().forEach(r -> allIds.add(r.accountId));
        page3.rows().forEach(r -> allIds.add(r.accountId));
        assertThat(allIds).containsExactlyInAnyOrder(
                "acct-list-0", "acct-list-1", "acct-list-2", "acct-list-3", "acct-list-4");

        // The order is (created_at, account_id): one transaction stamps every
        // row the same created_at, so the account_id tie-break decides — the
        // pages' exact contents are deterministic.
        assertThat(page1.rows().stream().map(r -> r.accountId).toList())
                .containsExactly("acct-list-0", "acct-list-1");
        assertThat(page2.rows().stream().map(r -> r.accountId).toList())
                .containsExactly("acct-list-2", "acct-list-3");
        assertThat(page3.rows().stream().map(r -> r.accountId).toList())
                .containsExactly("acct-list-4");

        // An exact page boundary: offset 4 with limit 2 lands on the last
        // row, and the page past the end is empty — with the total still
        // honest on both.
        ListAccountsResult pastEnd = database.readOnly(c -> store.list(c, null, 2, 5));
        assertThat(pastEnd.rows()).isEmpty();
        assertThat(pastEnd.totalCount()).isEqualTo(5);

        // A filter nothing matches: empty page, zero total.
        ListAccountsResult suspended = database.readOnly(c ->
                store.list(c, AccountStatus.ACCOUNT_STATUS_SUSPENDED, 100, 0));
        assertThat(suspended.rows()).isEmpty();
        assertThat(suspended.totalCount()).isZero();

        ListAccountsResult deactivated = database.readOnly(c ->
                store.list(c, AccountStatus.ACCOUNT_STATUS_DEACTIVATED, 100, 0));
        assertThat(deactivated.totalCount()).isEqualTo(1);
        assertThat(deactivated.rows().get(0).accountId).isEqualTo("acct-list-4");
    }
}
