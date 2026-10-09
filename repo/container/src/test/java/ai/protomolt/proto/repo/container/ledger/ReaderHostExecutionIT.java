package ai.protomolt.proto.repo.container.ledger;

import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.context;
import static org.assertj.core.api.Assertions.*;

@Testcontainers
class ReaderHostExecutionIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    @Test void bindingAndTombstonesArePermanentAndFenceDoesNotAttestQuiescence() {
        try (var c = context(POSTGRES)) {
            var host = UUID.randomUUID(); var reader = UUID.randomUUID(); var local = UUID.randomUUID();
            host(c.tx(), host); reader(c.tx(), reader, host); ReaderRegistration.register(c.tx(), local);
            admit(c.tx(), reader); fence(c.tx(), host); fence(c.tx(), host);
            assertThatThrownBy(() -> admit(c.tx(), reader)).hasStackTraceContaining("host execution is not ACTIVE");
            admit(c.tx(), local);
            assertThatThrownBy(() -> reader(c.tx(), UUID.randomUUID(), host)).hasStackTraceContaining("host execution is not ACTIVE");
            assertThatThrownBy(() -> host(c.tx(), host)).hasStackTraceContaining("duplicate key");
            assertThatThrownBy(() -> c.tx().inTransaction(em -> {
                em.createNativeQuery("UPDATE repository_reader_incarnations SET host_execution=NULL WHERE incarnation=:id")
                    .setParameter("id", reader).executeUpdate();
            })).hasStackTraceContaining("binding is immutable");
            assertThatThrownBy(() -> c.tx().inTransaction(em -> {
                em.createNativeQuery("DELETE FROM repository_reader_host_executions WHERE execution=:id")
                    .setParameter("id", host).executeUpdate();
            })).hasStackTraceContaining("tombstones are permanent");
            assertThat(c.tx().readOnly(em -> em.createNativeQuery(
                "SELECT state FROM repository_reader_incarnations WHERE incarnation=:id")
                .setParameter("id", reader).getSingleResult()).toString()).isEqualTo("ACTIVE");
        }
    }

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void committedAdmissionPrecedesFenceWithoutBlockingOtherHosts(boolean registration) throws Exception {
        try (var c = context(POSTGRES); var blocker = c.pool().getConnection(); var workers = Executors.newSingleThreadExecutor()) {
            var host = UUID.randomUUID(); var other = UUID.randomUUID(); var reader = UUID.randomUUID();
            host(c.tx(), host); host(c.tx(), other);
            if (!registration) reader(c.tx(), reader, host);
            blocker.setAutoCommit(false);
            int pid;
            try (var s = blocker.createStatement(); var r = s.executeQuery("SELECT pg_backend_pid()")) { r.next(); pid = r.getInt(1); }
            try {
                try (var s = blocker.prepareStatement(registration
                        ? "INSERT INTO repository_reader_incarnations(incarnation,state,host_execution) VALUES(?,'ACTIVE',?)"
                        : "SELECT require_active_repository_reader(?)")) {
                    s.setObject(1, reader); if (registration) s.setObject(2, host); s.execute();
                }
                var pending = workers.submit(() -> fence(c.tx(), host));
                awaitBlocked(c.tx(), pid);
                var independent = UUID.randomUUID(); reader(c.tx(), independent, other); admit(c.tx(), independent);
                assertThat(pending).isNotDone();
                blocker.commit(); pending.get(10, TimeUnit.SECONDS);
                assertThatThrownBy(() -> admit(c.tx(), reader)).hasStackTraceContaining("host execution is not ACTIVE");
            } finally { blocker.rollback(); }
        }
    }

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void fenceWinningRaceRejectsRegistrationAndAdmission(boolean registration) throws Exception {
        try (var c = context(POSTGRES); var blocker = c.pool().getConnection(); var workers = Executors.newSingleThreadExecutor()) {
            var host = UUID.randomUUID(); var reader = UUID.randomUUID(); host(c.tx(), host);
            if (!registration) reader(c.tx(), reader, host);
            blocker.setAutoCommit(false);
            int pid;
            try (var s = blocker.createStatement(); var r = s.executeQuery("SELECT pg_backend_pid()")) { r.next(); pid = r.getInt(1); }
            try {
                try (var s = blocker.prepareStatement("SELECT fence_repository_reader_host(?)")) { s.setObject(1, host); s.execute(); }
                var pending = workers.submit(() -> catchThrowable(() -> {
                    if (registration) reader(c.tx(), reader, host); else admit(c.tx(), reader);
                }));
                awaitBlocked(c.tx(), pid); blocker.commit();
                assertThat(pending.get(10, TimeUnit.SECONDS)).hasStackTraceContaining("host execution is not ACTIVE");
                if (registration) assertThat(c.tx().<Integer>readOnly(em -> ((Number) em.createNativeQuery(
                    "SELECT count(*) FROM repository_reader_incarnations WHERE incarnation=:id")
                    .setParameter("id", reader).getSingleResult()).intValue())).isZero();
            } finally { blocker.rollback(); }
        }
    }

    @Test void migrationKeepsExistingReadersLocalAndRejectsRetrofittedBindings() {
        try (var c = DocumentNativePublicationFixture.context(POSTGRES, "111")) {
            var reader = UUID.randomUUID();
            c.tx().inTransaction(em -> { em.createNativeQuery("INSERT INTO repository_reader_incarnations(incarnation,state) VALUES(:id,'ACTIVE')")
                .setParameter("id", reader).executeUpdate(); });

            org.flywaydb.core.Flyway.configure().dataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())
                .schemas(c.pool().getSchema()).defaultSchema(c.pool().getSchema())
                .locations("classpath:db/migration/repo").load().migrate();
            var host = UUID.randomUUID(); host(c.tx(), host);
            assertThatThrownBy(() -> c.tx().inTransaction(em -> {
                em.createNativeQuery("UPDATE repository_reader_incarnations SET host_execution=:host WHERE incarnation=:id")
                    .setParameter("host", host).setParameter("id", reader).executeUpdate();
            })).hasStackTraceContaining("binding is immutable");
            fence(c.tx(), host); admit(c.tx(), reader);
            assertThat(c.tx().<Integer>readOnly(em -> ((Number) em.createNativeQuery(
                "SELECT count(*) FROM repository_reader_incarnations WHERE incarnation=:id AND host_execution IS NULL")
                .setParameter("id", reader).getSingleResult()).intValue())).isEqualTo(1);
        }
    }

    @ParameterizedTest @org.junit.jupiter.params.provider.CsvSource({"false,false", "false,true", "true,false", "true,true"})
    void failedBoundRegistrationKeepsItsBindingAfterHostFence(boolean committed, boolean archive) {
        try (var c = context(POSTGRES)) {
            var host = UUID.randomUUID(); var reader = UUID.randomUUID(); host(c.tx(), host);
            var armed = new java.util.concurrent.atomic.AtomicBoolean();
            javax.sql.DataSource source = committed
                ? DocumentJdbcFaults.afterCommit(c.pool(), () -> {
                    if (armed.compareAndSet(true, false)) {
                        fence(c.tx(), host);
                        throw new java.sql.SQLException("Registration committed but acknowledgment lost", "08006");
                    }
                })
                : DocumentJdbcFaults.beforeCommit(c.pool(), connection -> {
                    if (armed.compareAndSet(true, false)) {
                        connection.rollback(); fence(c.tx(), host);
                        throw new java.sql.SQLException("Registration rolled back before host fence", "08006");
                    }
                });
            try (var emf = jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger",
                    java.util.Map.of("hibernate.connection.datasource", source, "hibernate.hbm2ddl.auto", "validate"))) {
                armed.set(true);
                var failure = catchThrowableOfType(ReaderRegistration.Failure.class,
                    () -> construct(new Tx(emf), reader, host, archive));
                assertThat(failure.cleanup()).isEqualTo(ReaderRegistration.Cleanup.QUIESCED);
                assertThat(armed).isFalse();
                assertThat(c.tx().<Integer>readOnly(em -> ((Number) em.createNativeQuery(
                    "SELECT count(*) FROM repository_reader_incarnations WHERE incarnation=:id AND host_execution=:host AND state='QUIESCED' AND quiescence_source='LOCAL_DRAIN'")
                    .setParameter("id", reader).setParameter("host", host).getSingleResult()).intValue())).isEqualTo(1);
                assertThatThrownBy(() -> admit(c.tx(), reader)).isInstanceOf(RuntimeException.class);
            }
        }
    }

    @Test void failedRegistrationCannotQuiesceForeignHostReader() {
        try (var c = context(POSTGRES)) {
            var first = UUID.randomUUID(); var second = UUID.randomUUID(); var reader = UUID.randomUUID();
            host(c.tx(), first); host(c.tx(), second); ReaderRegistration.register(c.tx(), reader, first);
            var failure = catchThrowableOfType(ReaderRegistration.Failure.class,
                () -> ReaderRegistration.register(c.tx(), reader, second));
            assertThat(failure.cleanup()).isEqualTo(ReaderRegistration.Cleanup.FOREIGN_IDENTITY);
            admit(c.tx(), reader);
            assertThat(c.tx().<Integer>readOnly(em -> ((Number) em.createNativeQuery(
                "SELECT count(*) FROM repository_reader_incarnations WHERE incarnation=:id AND host_execution=:host AND state='ACTIVE'")
                .setParameter("id", reader).setParameter("host", first).getSingleResult()).intValue())).isEqualTo(1);
        }
    }

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void ledgerConstructorsRequireHostAndPreserveLocalShutdown(boolean archive) {
        try (var c = context(POSTGRES)) {
            var host = UUID.randomUUID(); var id = UUID.randomUUID(); host(c.tx(), host);
            Runnable shutdown;
            if (archive) {
                var ledger = new ai.protomolt.proto.repo.container.archive.ArchiveReadLedger(c.tx(), id, host);
                shutdown = () -> { ledger.fence(); ledger.attestLocalQuiescence(); };
            } else {
                var ledger = new DocumentReadLedger(c.tx(), id, host, 2);
                shutdown = () -> { ledger.fence(); ledger.attestLocalQuiescence(); };
            }
            admit(c.tx(), id); fence(c.tx(), host);
            assertThatThrownBy(() -> admit(c.tx(), id)).hasStackTraceContaining("host execution is not ACTIVE");
            shutdown.run(); shutdown.run();
            assertThat(c.tx().<Integer>readOnly(em -> ((Number) em.createNativeQuery(
                "SELECT count(*) FROM repository_reader_incarnations WHERE incarnation=:id AND host_execution=:host AND state='QUIESCED'")
                .setParameter("id", id).setParameter("host", host).getSingleResult()).intValue())).isEqualTo(1);
            var rejected = UUID.randomUUID();
            var failure = catchThrowableOfType(ReaderRegistration.Failure.class,
                () -> construct(c.tx(), rejected, host, archive));
            assertThat(failure.cleanup()).isEqualTo(ReaderRegistration.Cleanup.QUIESCED);
        }
    }

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void missingHostCannotProduceALedgerOrClaimSuccessfulCleanup(boolean archive) {
        try (var c = context(POSTGRES)) {
            var id = UUID.randomUUID(); var host = UUID.randomUUID();
            var failure = catchThrowableOfType(ReaderRegistration.Failure.class,
                () -> construct(c.tx(), id, host, archive));
            assertThat(failure.cleanup()).isEqualTo(ReaderRegistration.Cleanup.PENDING);
            assertThat(failure.getSuppressed()).hasSize(1);
            assertThatThrownBy(() -> failure.retryCleanup(c.tx())).isInstanceOf(RuntimeException.class);
            assertThat(failure.cleanup()).isEqualTo(ReaderRegistration.Cleanup.PENDING);
            assertThat(c.tx().<Integer>readOnly(em -> ((Number) em.createNativeQuery(
                "SELECT count(*) FROM repository_reader_incarnations WHERE incarnation=:id")
                .setParameter("id", id).getSingleResult()).intValue())).isZero();
            assertThatThrownBy(() -> construct(c.tx(), UUID.randomUUID(), null, archive))
                .isInstanceOf(NullPointerException.class);
        }
    }

    private static void construct(Tx tx, UUID id, UUID host, boolean archive) {
        if (archive) new ai.protomolt.proto.repo.container.archive.ArchiveReadLedger(tx, id, host);
        else new DocumentReadLedger(tx, id, host);
    }

    private static void awaitBlocked(Tx tx, int pid) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        do {
            if (tx.readOnly(em -> ((Number) em.createNativeQuery(
                "SELECT count(*) FROM pg_stat_activity WHERE :pid=ANY(pg_blocking_pids(pid))")
                .setParameter("pid", pid).getSingleResult()).intValue()) > 0) return;
            Thread.sleep(10);
        } while (System.nanoTime() < deadline);
        fail("Expected an actual PostgreSQL lock wait");
    }
    private static void host(Tx tx, UUID id) {
        ReaderHostExecutions.register(tx, id, "test-host", "test-boot");
    }
    private static void reader(Tx tx, UUID id, UUID host) {
        tx.inTransaction(em -> { em.createNativeQuery("INSERT INTO repository_reader_incarnations(incarnation,state,host_execution) VALUES(:id,'ACTIVE',:host)")
            .setParameter("id", id).setParameter("host", host).executeUpdate(); });
    }
    private static void admit(Tx tx, UUID id) {
        tx.inTransaction(em -> { em.createNativeQuery("SELECT require_active_repository_reader(:id)").setParameter("id", id).getSingleResult(); });
    }
    private static void fence(Tx tx, UUID id) {
        ReaderHostExecutions.fence(tx, id);
    }
}
