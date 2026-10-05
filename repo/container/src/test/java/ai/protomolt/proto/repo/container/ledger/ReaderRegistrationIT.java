package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.container.archive.ArchiveReadLedger;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.context;
import static org.assertj.core.api.Assertions.*;

/** Registration failures around real PostgreSQL commits, before any read handle escapes. */
@Testcontainers
class ReaderRegistrationIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void lostCommitAcknowledgmentQuiescesTheUnexposedReader(boolean archive) {
        try (var c = context(POSTGRES)) {
            var armed = new AtomicBoolean();
            var source = DocumentJdbcFaults.afterCommit(c.pool(), () -> {
                if (armed.compareAndSet(true, false)) throw new java.sql.SQLException("Reader registration acknowledgment lost", "08006");
            });
            try (var emf = jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger",
                    Map.of("hibernate.connection.datasource", source, "hibernate.hbm2ddl.auto", "validate"))) {
                var id = UUID.randomUUID();
                armed.set(true);
                assertThatThrownBy(() -> register(new Tx(emf), id, archive))
                        .hasStackTraceContaining("Reader registration acknowledgment lost");
                assertThat(armed).isFalse();
                assertThat(state(c.tx(), id)).isEqualTo("QUIESCED");
                assertThat(c.tx().readOnly(em -> em.createNativeQuery(
                        "SELECT quiescence_source FROM repository_reader_incarnations WHERE incarnation=:id")
                        .setParameter("id", id).getSingleResult()).toString()).isEqualTo("LOCAL_DRAIN");
            }
        }
    }

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void cleanupCommitAcknowledgmentLossRetainsIdentityForExplicitRetry(boolean archive) {
        try (var c = context(POSTGRES)) {
            var faults = new AtomicInteger();
            var source = DocumentJdbcFaults.afterCommit(c.pool(), () -> {
                if (faults.getAndUpdate(value -> Math.max(0, value - 1)) > 0)
                    throw new java.sql.SQLException("Registration or cleanup acknowledgment lost", "08006");
            });
            ReaderRegistration.Failure failure;
            var id = UUID.randomUUID();
            try (var emf = jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger",
                    Map.of("hibernate.connection.datasource", source, "hibernate.hbm2ddl.auto", "validate"))) {
                faults.set(2);
                failure = catchThrowableOfType(ReaderRegistration.Failure.class, () -> register(new Tx(emf), id, archive));
                assertThat(failure).isNotNull();
                assertThat(failure.cleanup()).isEqualTo(ReaderRegistration.Cleanup.PENDING);
                assertThat(failure.incarnation()).isEqualTo(id);
                assertThat(failure.getSuppressed()).hasSize(1);
                assertThat(state(c.tx(), id)).isEqualTo("QUIESCED");
            }
            // Retry is possible with a live Tx after the failed startup factory closes.
            assertThat(failure.retryCleanup(c.tx())).isEqualTo(ReaderRegistration.Cleanup.QUIESCED);
            assertThat(failure.retryCleanup(c.tx())).isEqualTo(ReaderRegistration.Cleanup.QUIESCED);
            assertThat(state(c.tx(), id)).isEqualTo("QUIESCED");
        }
    }

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void rollbackReservesAPermanentQuiescedTombstone(boolean archive) {
        try (var c = context(POSTGRES)) {
            var armed = new AtomicBoolean();
            var source = DocumentJdbcFaults.beforeCommit(c.pool(), connection -> {
                if (armed.compareAndSet(true, false)) throw new java.sql.SQLException("Registration rolled back before commit");
            });
            try (var emf = jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger",
                    Map.of("hibernate.connection.datasource", source, "hibernate.hbm2ddl.auto", "validate"))) {
                var id = UUID.randomUUID();
                armed.set(true);
                var failed = catchThrowableOfType(ReaderRegistration.Failure.class, () -> register(new Tx(emf), id, archive));
                assertThat(failed.cleanup()).isEqualTo(ReaderRegistration.Cleanup.QUIESCED);
                assertThat(state(c.tx(), id)).isEqualTo("QUIESCED");
                var duplicate = catchThrowableOfType(ReaderRegistration.Failure.class, () -> register(c.tx(), id, archive));
                assertThat(duplicate.cleanup()).isEqualTo(ReaderRegistration.Cleanup.FOREIGN_IDENTITY);
                assertThat(state(c.tx(), id)).isEqualTo("QUIESCED");
            }
        }
    }

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void failedCleanupLeavesActiveProtectionAndCanRetryWithLiveDatabase(boolean archive) {
        try (var c = context(POSTGRES)) {
            var armed = new AtomicBoolean();
            var commits = new AtomicInteger();
            var source = DocumentJdbcFaults.beforeCommit(DocumentJdbcFaults.afterCommit(c.pool(), () -> {
                if (armed.get() && commits.get() == 1)
                    throw new java.sql.SQLException("Original registration acknowledgment lost", "08006");
            }), connection -> {
                if (armed.get() && commits.incrementAndGet() == 2)
                    throw new java.sql.SQLException("Cleanup database unavailable before commit", "08006");
            });
            var id = UUID.randomUUID();
            ReaderRegistration.Failure failed;
            try (var emf = jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger",
                    Map.of("hibernate.connection.datasource", source, "hibernate.hbm2ddl.auto", "validate"))) {
                armed.set(true);
                failed = catchThrowableOfType(ReaderRegistration.Failure.class, () -> register(new Tx(emf), id, archive));
                assertThat(failed).hasStackTraceContaining("Original registration acknowledgment lost")
                        .hasStackTraceContaining("Cleanup database unavailable before commit");
                assertThat(failed.cleanup()).isEqualTo(ReaderRegistration.Cleanup.PENDING);
                assertThat(failed.getSuppressed()).hasSize(1);
                assertThat(state(c.tx(), id)).isEqualTo("ACTIVE");
            }
            assertThat(failed.retryCleanup(c.tx())).isEqualTo(ReaderRegistration.Cleanup.QUIESCED);
            assertThat(state(c.tx(), id)).isEqualTo("QUIESCED");
        }
    }

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void replacementRegistrationAfterRollbackMustNotBeFencedByFailedOwner(boolean archive) {
        try (var c = context(POSTGRES)) {
            var id = UUID.randomUUID();
            var armed = new AtomicBoolean();
            var source = DocumentJdbcFaults.beforeCommit(c.pool(), connection -> {
                if (armed.compareAndSet(true, false)) {
                    // Deterministic real-transaction interleaving: release the first
                    // insertion, let a different owner register, then report failure.
                    connection.rollback();
                    register(c.tx(), id, !archive);
                    throw new java.sql.SQLException("Original registration rolled back and another owner won");
                }
            });
            try (var emf = jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger",
                    Map.of("hibernate.connection.datasource", source, "hibernate.hbm2ddl.auto", "validate"))) {
                armed.set(true);
                var failure = catchThrowableOfType(ReaderRegistration.Failure.class, () -> register(new Tx(emf), id, archive));
                assertThat(failure.cleanup()).isEqualTo(ReaderRegistration.Cleanup.FOREIGN_IDENTITY);
                assertThat(failure.retryCleanup(c.tx())).isEqualTo(ReaderRegistration.Cleanup.FOREIGN_IDENTITY);
                assertThat(state(c.tx(), id)).isEqualTo("ACTIVE");
                var duplicate = catchThrowableOfType(ReaderRegistration.Failure.class, () -> register(c.tx(), id, archive));
                assertThat(duplicate.cleanup()).isEqualTo(ReaderRegistration.Cleanup.FOREIGN_IDENTITY);
                assertThat(state(c.tx(), id)).isEqualTo("ACTIVE");
            }
        }
    }

    @Test void migrationPreservesExistingReaderStatesAndMakesRegistrationIdentityImmutable() {
        try (var c = DocumentNativePublicationFixture.context(POSTGRES, "73")) {
            var active = UUID.randomUUID(); var fenced = UUID.randomUUID(); var quiesced = UUID.randomUUID();
            c.tx().inTransaction(em -> {
                for (var id : java.util.List.of(active, fenced, quiesced))
                    em.createNativeQuery("INSERT INTO repository_reader_incarnations(incarnation,state) VALUES(:id,'ACTIVE')")
                            .setParameter("id", id).executeUpdate();
                em.createNativeQuery("SELECT fence_repository_reader(:id)").setParameter("id", fenced).getSingleResult();
                em.createNativeQuery("SELECT fence_repository_reader(:id)").setParameter("id", quiesced).getSingleResult();
                em.createNativeQuery("SELECT attest_local_reader_quiescence(:id)").setParameter("id", quiesced).getSingleResult();
            });
            org.flywaydb.core.Flyway.configure().dataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())
                    .schemas(c.pool().getSchema()).defaultSchema(c.pool().getSchema())
                    .locations("classpath:db/migration/repo").load().migrate();
            assertThat(state(c.tx(), active)).isEqualTo("ACTIVE");
            assertThat(state(c.tx(), fenced)).isEqualTo("FENCED");
            assertThat(state(c.tx(), quiesced)).isEqualTo("QUIESCED");
            var nonces = c.tx().readOnly(em -> em.createNativeQuery("SELECT registration_nonce FROM repository_reader_incarnations").getResultList());
            assertThat(nonces).hasSize(3).doesNotContainNull().doesNotHaveDuplicates();
            assertThatThrownBy(() -> c.tx().inTransaction(em -> {
                em.createNativeQuery("UPDATE repository_reader_incarnations SET registration_nonce=:nonce WHERE incarnation=:id")
                        .setParameter("nonce", UUID.randomUUID()).setParameter("id", active).executeUpdate();
            })).hasStackTraceContaining("Reader registration identity is immutable");
        }
    }

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void cleanupWaitsForAnUncommittedForeignInsertion(boolean commitForeign) throws Exception {
        try (var c = context(POSTGRES)) {
            var armed = new AtomicBoolean();
            var source = DocumentJdbcFaults.beforeCommit(c.pool(), connection -> {
                if (armed.get()) throw new java.sql.SQLException("Registration and initial cleanup rolled back");
            });
            var id = UUID.randomUUID();
            ReaderRegistration.Failure failed;
            try (var emf = jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger",
                    Map.of("hibernate.connection.datasource", source, "hibernate.hbm2ddl.auto", "validate"))) {
                armed.set(true);
                failed = catchThrowableOfType(ReaderRegistration.Failure.class, () -> register(new Tx(emf), id, false));
                assertThat(failed.cleanup()).isEqualTo(ReaderRegistration.Cleanup.PENDING);
            }
            try (var connection = c.pool().getConnection();
                    var executor = java.util.concurrent.Executors.newSingleThreadExecutor()) {
                connection.setAutoCommit(false);
                int blocker;
                try (var query = connection.createStatement(); var result = query.executeQuery("SELECT pg_backend_pid()")) {
                    result.next(); blocker = result.getInt(1);
                }
                try (var insert = connection.prepareStatement(
                        "INSERT INTO repository_reader_incarnations(incarnation,state) VALUES(?,'ACTIVE')")) {
                    insert.setObject(1, id); insert.executeUpdate();
                }
                var pending = executor.submit(() -> failed.retryCleanup(c.tx()));
                try {
                    long deadline = System.nanoTime() + java.time.Duration.ofSeconds(5).toNanos();
                    boolean blocked = false;
                    do {
                        blocked = c.tx().readOnly(em -> ((Number) em.createNativeQuery("""
                                SELECT count(*) FROM pg_stat_activity
                                WHERE :blocker = ANY(pg_blocking_pids(pid))
                                """).setParameter("blocker", blocker).getSingleResult()).longValue()) > 0;
                        if (!blocked) Thread.sleep(10);
                    } while (!blocked && System.nanoTime() < deadline);
                    assertThat(blocked).as("cleanup must wait on the actual conflicting PostgreSQL transaction").isTrue();
                    assertThat(pending).isNotDone();
                    if (commitForeign) connection.commit(); else connection.rollback();
                    assertThat(pending.get(10, java.util.concurrent.TimeUnit.SECONDS)).isEqualTo(commitForeign
                            ? ReaderRegistration.Cleanup.FOREIGN_IDENTITY : ReaderRegistration.Cleanup.QUIESCED);
                    assertThat(state(c.tx(), id)).isEqualTo(commitForeign ? "ACTIVE" : "QUIESCED");
                } finally {
                    // Release the SQL blocker before ExecutorService.close waits for the worker.
                    connection.rollback();
                }
            }
        }
    }

    private static void register(Tx tx, UUID id, boolean archive) {
        if (archive) new ArchiveReadLedger(tx, id);
        else new DocumentReadLedger(tx, id);
    }

    private static String state(Tx tx, UUID id) {
        return tx.readOnly(em -> em.createNativeQuery("SELECT state FROM repository_reader_incarnations WHERE incarnation=:id")
                .setParameter("id", id).getSingleResult()).toString();
    }
}
