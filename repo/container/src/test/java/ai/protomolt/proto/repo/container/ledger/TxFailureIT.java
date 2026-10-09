package ai.protomolt.proto.repo.container.ledger;

import jakarta.persistence.EntityManager;
import jakarta.persistence.RollbackException;
import java.util.UUID;
import java.util.function.Function;
import org.junit.jupiter.api.*;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static org.assertj.core.api.Assertions.*;

/** Transaction failure evidence against a real database, including connection loss. */
@Testcontainers
class TxFailureIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static LedgerDatabase database;
    private static Tx tx;

    @BeforeAll static void open() {
        database = new LedgerDatabase(new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()));
        tx = new Tx(database.entityManagerFactory());
        tx.inTransaction(em -> { em.createNativeQuery("CREATE TABLE tx_failure_probe(id UUID PRIMARY KEY)").executeUpdate(); });
    }
    @AfterAll static void close() { if (database != null) database.close(); }

    @Test void rollbackOnlyCannotReturnSuccessfulResult() {
        var id = UUID.randomUUID();
        assertThatThrownBy(() -> tx.inTransaction(em -> {
            insert(em, id);
            em.getTransaction().setRollbackOnly();
            return "must never be returned";
        })).isInstanceOf(RollbackException.class).hasMessageContaining("rollback-only");
        assertAbsent(id);
    }

    @Test void errorsRollBackAndPropagateIntact() {
        var id = UUID.randomUUID();
        var failure = new AssertionError("coordinator invariant failed");
        assertThatThrownBy(() -> tx.inTransaction((Function<EntityManager, Void>) em -> {
            insert(em, id);
            throw failure;
        })).isSameAs(failure);
        assertAbsent(id);
    }

    @Test void lostConnectionDuringRollbackPreservesBothFailures() {
        var id = UUID.randomUUID();
        var failure = new IllegalStateException("work failed before rollback");
        assertThatThrownBy(() -> tx.inTransaction((Function<EntityManager, Void>) em -> {
            insert(em, id);
            int pid = ((Number) em.createNativeQuery("SELECT pg_backend_pid()").getSingleResult()).intValue();
            // Terminate only this test transaction using another real connection.
            try (var connection = database.dataSource().getConnection();
                    var statement = connection.prepareStatement("SELECT pg_terminate_backend(?)")) {
                statement.setInt(1, pid);
                try (var rows = statement.executeQuery()) {
                    assertThat(rows.next()).isTrue();
                    assertThat(rows.getBoolean(1)).isTrue();
                }
            } catch (java.sql.SQLException unexpected) {
                throw new IllegalStateException("Cannot inject test connection loss", unexpected);
            }
            throw failure;
        })).isSameAs(failure);
        assertThat(failure.getSuppressed()).isNotEmpty();
        assertThat(failure.getSuppressed()[0]).hasStackTraceContaining("rollback");
        assertAbsent(id);
    }

    private static void insert(EntityManager em, UUID id) {
        em.createNativeQuery("INSERT INTO tx_failure_probe(id) VALUES (:id)").setParameter("id", id).executeUpdate();
    }
    private static void assertAbsent(UUID id) {
        long count = tx.readOnly(em -> ((Number) em.createNativeQuery("SELECT count(*) FROM tx_failure_probe WHERE id=:id")
                .setParameter("id", id).getSingleResult()).longValue());
        assertThat(count).isZero();
    }
}
