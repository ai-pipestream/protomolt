package ai.protomolt.proto.repo.container.ledger;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.*;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static org.assertj.core.api.Assertions.*;

/** Real PostgreSQL cancellation and a single pooled connection to detect setting leakage. */
@Testcontainers
@Timeout(15)
class TxTimeoutIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static LedgerDatabase database;
    private static Tx tx;
    private static final SqlTimeouts LIMITS = new SqlTimeouts(Duration.ofMillis(75), Duration.ofMillis(200));
    private record Settings(int pid, String lock, String statement) {}

    @BeforeAll static void open() {
        database = new LedgerDatabase(new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword(),
                1, LedgerConfig.DEFAULT_MIGRATION_LOCATION));
        tx = new Tx(database.entityManagerFactory());
        tx.inTransaction(em -> {
            em.createNativeQuery("CREATE TABLE timeout_probe(id integer PRIMARY KEY, value integer NOT NULL)").executeUpdate();
            em.createNativeQuery("INSERT INTO timeout_probe VALUES (1,0)").executeUpdate();
        });
    }
    @AfterAll static void close() { if (database != null) database.close(); }

    @Test void successfulTransactionRestoresSettingsAndBorrowedCloseKeepsFactoryOpen() {
        var before = settings();
        try (var bounded = tx.withTimeouts(LIMITS)) {
            var inside = bounded.inTransaction(em -> (Object[]) em.createNativeQuery(
                    "SELECT current_setting('lock_timeout'),current_setting('statement_timeout')").getSingleResult());
            assertThat(inside).containsExactly("75ms", "200ms");
        }
        assertThat(database.entityManagerFactory().isOpen()).isTrue();
        assertThat(settings()).isEqualTo(before);
    }

    @Test void heldRowLockTimesOutBeforeBlockerReleasesAndRollsBackEarlierWrites() throws Exception {
        var before = settings();
        try (var blocker = DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())) {
            blocker.setAutoCommit(false);
            try (var statement = blocker.createStatement()) { statement.executeUpdate("UPDATE timeout_probe SET value=1 WHERE id=1"); }
            try {
                var failure = catchThrowable(() -> tx.withTimeouts(LIMITS).inTransaction(em -> {
                    em.createNativeQuery("INSERT INTO timeout_probe VALUES (2,2)").executeUpdate();
                    em.createNativeQuery("UPDATE timeout_probe SET value=2 WHERE id=1").executeUpdate();
                }));
                assertSqlState(failure, "55P03");
                assertThat(count(2)).isZero();
                assertThat(settings()).isEqualTo(before);
            } finally { blocker.rollback(); }
        }
    }

    @Test void slowStatementTimesOutAndRollsBackEarlierWrites() {
        var before = settings();
        var failure = catchThrowable(() -> tx.withTimeouts(LIMITS).inTransaction(em -> {
            em.createNativeQuery("INSERT INTO timeout_probe VALUES (3,3)").executeUpdate();
            em.createNativeQuery("SELECT pg_sleep(3)").getSingleResult();
        }));
        assertSqlState(failure, "57014");
        assertThat(count(3)).isZero();
        assertThat(settings()).isEqualTo(before);
    }

    @Test void failedTimeoutSetupNeverRunsWorkAndLeavesConnectionUsable() {
        var before = settings();
        var ran = new AtomicBoolean();
        tx.inTransaction(em -> {
            em.createNativeQuery("CREATE ROLE timeout_setup_denied").executeUpdate();
            em.createNativeQuery("REVOKE EXECUTE ON FUNCTION pg_catalog.set_config(text,text,boolean) FROM PUBLIC").executeUpdate();
            em.createNativeQuery("SET ROLE timeout_setup_denied").executeUpdate();
        });
        try {
            var failure = catchThrowable(() -> tx.withTimeouts(LIMITS).inTransaction(em -> { ran.set(true); }));
            assertSqlState(failure, "42501");
            assertThat(ran).isFalse();
        } finally {
            tx.inTransaction(em -> {
                em.createNativeQuery("RESET ROLE").executeUpdate();
                em.createNativeQuery("GRANT EXECUTE ON FUNCTION pg_catalog.set_config(text,text,boolean) TO PUBLIC").executeUpdate();
                em.createNativeQuery("DROP ROLE timeout_setup_denied").executeUpdate();
            });
        }
        assertThat(settings()).isEqualTo(before);
    }

    @Test void invalidLimitsAndUnboundedReadPathAreRejected() {
        for (var invalid : new Duration[]{Duration.ZERO, Duration.ofNanos(1), Duration.ofMillis(1).plusNanos(1),
                Duration.ofMillis(-1), Duration.ofDays(2)}) {
            assertThatThrownBy(() -> new SqlTimeouts(invalid, Duration.ofSeconds(1))).isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> new SqlTimeouts(Duration.ofMillis(1), invalid)).isInstanceOf(IllegalArgumentException.class);
        }
        assertThatThrownBy(() -> new SqlTimeouts(Duration.ofSeconds(2), Duration.ofSeconds(1)))
                .hasMessageContaining("must not exceed");
        var ran = new AtomicBoolean();
        assertThatThrownBy(() -> tx.withTimeouts(LIMITS).readOnly(em -> { ran.set(true); return true; }))
                .hasMessageContaining("require inTransaction");
        assertThat(ran).isFalse();
    }

    private static Settings settings() {
        return tx.inTransaction(em -> {
            var values = (Object[]) em.createNativeQuery(
                    "SELECT pg_backend_pid(),current_setting('lock_timeout'),current_setting('statement_timeout')").getSingleResult();
            return new Settings(((Number) values[0]).intValue(), (String) values[1], (String) values[2]);
        });
    }
    private static long count(int id) {
        return tx.readOnly(em -> ((Number) em.createNativeQuery("SELECT count(*) FROM timeout_probe WHERE id=:id")
                .setParameter("id", id).getSingleResult()).longValue());
    }
    private static void assertSqlState(Throwable failure, String state) {
        assertThat(failure).isNotNull();
        for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
            if (cause instanceof SQLException sql && state.equals(sql.getSQLState())) return;
        }
        fail("Missing SQLState " + state, failure);
    }
}
