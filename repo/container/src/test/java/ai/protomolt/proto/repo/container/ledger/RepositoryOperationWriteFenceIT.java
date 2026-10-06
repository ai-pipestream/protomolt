package ai.protomolt.proto.repo.container.ledger;

import com.google.protobuf.ByteString;
import java.sql.Connection;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static org.assertj.core.api.Assertions.*;

/** Real PostgreSQL fence proof; the probe is test-only DML, not an upload implementation. */
@Testcontainers
class RepositoryOperationWriteFenceIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static LedgerDatabase database;
    private static Tx tx;
    private static RepositoryOperationLedger ledger;

    @BeforeAll static void open() {
        database = new LedgerDatabase(new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()));
        tx = new Tx(database.entityManagerFactory());
        ledger = new RepositoryOperationLedger(tx);
        tx.inTransaction(em -> {
            em.createNativeQuery("""
                    CREATE TABLE operation_fence_probe(account_id text, principal text, operation_id uuid, generation bigint)
                    """).executeUpdate();
            em.createNativeQuery("""
                    CREATE FUNCTION guard_operation_fence_probe() RETURNS trigger LANGUAGE plpgsql AS $$
                    BEGIN
                        PERFORM require_repository_operation_write_fence(NEW.account_id,NEW.principal,NEW.operation_id,NEW.generation);
                        RETURN NEW;
                    END; $$
                    """).executeUpdate();
            em.createNativeQuery("""
                    CREATE TRIGGER operation_fence_probe_guard BEFORE INSERT ON operation_fence_probe
                    FOR EACH ROW EXECUTE FUNCTION guard_operation_fence_probe()
                    """).executeUpdate();
        });
    }
    @AfterAll static void close() { if (database != null) database.close(); }

    @Test void oneOwnerStatementAuthorizesManyChecksButCannotBeReplayed() {
        var owner = owner(Duration.ofMinutes(1));
        var statistics = database.entityManagerFactory().unwrap(org.hibernate.SessionFactory.class).getStatistics();
        statistics.setStatisticsEnabled(true);
        try {
            tx.inTransaction(em -> {
                statistics.clear();
                assertThat(RepositoryOperationLedger.fenceLiveOwner(em, owner)).isEqualTo(owner);
                assertThat(statistics.getPrepareStatementCount()).isEqualTo(1);
                em.createNativeQuery("""
                        INSERT INTO operation_fence_probe
                        SELECT :account,:principal,:operation,:generation FROM generate_series(1,256)
                        """).setParameter("account", owner.key().account()).setParameter("principal", owner.key().principal())
                        .setParameter("operation", owner.key().operationId()).setParameter("generation", owner.generation()).executeUpdate();
            });
        } finally { statistics.setStatisticsEnabled(false); }
        assertThat(ledger.find(owner.key()).orElseThrow().leaseUntil()).isEqualTo(owner.leaseUntil());
        assertThatThrownBy(() -> tx.inTransaction(em -> { probe(em, owner); }))
                .hasStackTraceContaining("requires a live owner write fence");
    }

    @Test void rowLockAloneIsNotAWriteFence() {
        var owner = owner(Duration.ofMinutes(1));
        assertThatThrownBy(() -> tx.inTransaction(em -> {
            RepositoryOperationLedger.lockLiveOwner(em, owner);
            probe(em, owner);
        })).hasStackTraceContaining("requires a live owner write fence");
    }

    @Test void forgedTokenFailurePoisonsCallerTransaction() {
        var owner = owner(Duration.ofMinutes(1));
        var wrong = new RepositoryOperationLedger.Owner(owner.key(), owner.generation(), UUID.randomUUID(), owner.leaseUntil());
        assertThatThrownBy(() -> tx.inTransaction(em -> {
            assertThatThrownBy(() -> RepositoryOperationLedger.fenceLiveOwner(em, wrong))
                    .isInstanceOf(RepositoryOperationLedger.OwnerFencedException.class);
            return "not committed";
        })).isInstanceOf(jakarta.persistence.RollbackException.class);
        assertThatThrownBy(() -> tx.readOnly(em -> RepositoryOperationLedger.fenceLiveOwner(em, owner)))
                .isInstanceOf(IllegalStateException.class);
    }

    @ParameterizedTest @ValueSource(strings = {"rollback", "release", "before-savepoint"})
    void savepointProofTracksActualOwnerWrite(String mode) throws Exception {
        var owner = owner(Duration.ofMinutes(1));
        try (var connection = database.dataSource().getConnection()) {
            connection.setAutoCommit(false);
            if (mode.equals("before-savepoint")) stamp(connection, owner);
            var savepoint = connection.setSavepoint();
            if (!mode.equals("before-savepoint")) stamp(connection, owner);
            if (mode.equals("release")) connection.releaseSavepoint(savepoint);
            else connection.rollback(savepoint);
            if (mode.equals("rollback")) {
                assertThatThrownBy(() -> probe(connection, owner)).hasMessageContaining("requires a live owner write fence");
                connection.rollback();
            } else {
                probe(connection, owner);
                connection.commit();
            }
        }
    }

    @ParameterizedTest @ValueSource(strings = {"account", "principal", "operation", "generation"})
    void exactScopeAndGenerationRequired(String field) {
        var owner = owner(Duration.ofMinutes(1));
        var key = owner.key();
        var wrong = new RepositoryOperationLedger.Owner(new RepositoryOperationLedger.Key(
                field.equals("account") ? "other" : key.account(), field.equals("principal") ? "other" : key.principal(),
                field.equals("operation") ? UUID.randomUUID() : key.operationId()),
                field.equals("generation") ? owner.generation() + 1 : owner.generation(), owner.token(), owner.leaseUntil());
        if (!field.equals("generation")) {
            assertThatThrownBy(() -> tx.inTransaction(em -> {
                RepositoryOperationLedger.fenceLiveOwner(em, owner);
                probe(em, wrong);
            })).hasStackTraceContaining("Repository execution scope is absent");
            // Populate the other scope so the next assertion reaches the owner fence.
            // Its owner was admitted in another transaction and has no proof in ours.
            ledger.admit(wrong.key(), ledger.find(owner.key()).orElseThrow().command(), UUID.randomUUID(), Duration.ofMinutes(1));
        }
        assertThatThrownBy(() -> tx.inTransaction(em -> {
            RepositoryOperationLedger.fenceLiveOwner(em, owner);
            probe(em, wrong);
        })).hasStackTraceContaining("requires a live owner write fence");
        long inserted = tx.readOnly(em -> ((Number) em.createNativeQuery("""
                SELECT count(*) FROM operation_fence_probe WHERE account_id=:a AND principal=:p AND operation_id=:o
                """).setParameter("a", wrong.key().account()).setParameter("p", wrong.key().principal())
                .setParameter("o", wrong.key().operationId()).getSingleResult()).longValue());
        assertThat(inserted).isZero();
    }

    @Test void competingProbeFailsWithoutWaitingAndIndependentOwnerStillProgresses() throws Exception {
        var owner = owner(Duration.ofMinutes(1));
        var independent = owner(Duration.ofMinutes(1));
        try (var held = database.dataSource().getConnection(); var other = database.dataSource().getConnection()) {
            held.setAutoCommit(false); stamp(held, owner);
            other.setAutoCommit(false);
            try (var statement = other.createStatement()) {
                statement.execute("SET LOCAL lock_timeout='250ms'");
                statement.execute("SET LOCAL statement_timeout='2s'");
            }
            // The first transaction remains open for both operations: no timing guess.
            stamp(other, independent);
            probe(other, independent);
            assertThatThrownBy(() -> probe(other, owner)).hasMessageContaining("requires a live owner write fence")
                    .hasMessageNotContaining("timeout");
            other.rollback(); held.rollback();
        }
    }

    @Test void expiredProofAndReplacedGenerationCannotMutate() throws Exception {
        var owner = owner(Duration.ofSeconds(1));
        try (var held = database.dataSource().getConnection()) {
            held.setAutoCommit(false); stamp(held, owner);
            try (var statement = held.prepareStatement("""
                    SELECT pg_sleep(GREATEST(0,EXTRACT(EPOCH FROM lease_until-clock_timestamp()))+0.02)
                    FROM repository_operation_owners WHERE operation_id=?
                    """)) {
                statement.setObject(1, owner.key().operationId()); statement.execute();
            }
            assertThatThrownBy(() -> probe(held, owner)).hasMessageContaining("requires a live owner write fence");
            held.rollback();
        }
        var next = ledger.takeOver(owner.key(), owner.generation(), UUID.randomUUID(), Duration.ofMinutes(1));
        assertThatThrownBy(() -> tx.inTransaction(em -> { return RepositoryOperationLedger.fenceLiveOwner(em, owner); }))
                .isInstanceOf(RepositoryOperationLedger.OwnerFencedException.class);
        tx.inTransaction(em -> {
            RepositoryOperationLedger.fenceLiveOwner(em, next);
            probe(em, next);
        });
    }

    @Test void databaseWriterCanObtainLockProofWithoutTokenButCannotForgeTransactionId() throws Exception {
        // Explicit trust boundary: this proves lock order, not application authorization.
        var owner = owner(Duration.ofMinutes(1));
        try (var connection = database.dataSource().getConnection()) {
            connection.setAutoCommit(false);
            try (var statement = connection.prepareStatement("""
                    UPDATE repository_operation_owners SET write_fence_xid='1'::xid8 WHERE operation_id=?
                    RETURNING write_fence_xid=pg_current_xact_id()
                    """)) {
                statement.setObject(1, owner.key().operationId());
                try (var result = statement.executeQuery()) { assertThat(result.next()).isTrue(); assertThat(result.getBoolean(1)).isTrue(); }
            }
            probe(connection, owner);
            connection.rollback();
        }
    }

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void fenceRechecksExpiryAndGenerationAfterOwnerLockWait(boolean replace) throws Exception {
        var owner = owner(Duration.ofSeconds(2));
        try (var held = database.dataSource().getConnection(); var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            held.setAutoCommit(false);
            int pid;
            try (var statement = held.createStatement(); var result = statement.executeQuery("SELECT pg_backend_pid()")) {
                assertThat(result.next()).isTrue(); pid = result.getInt(1);
            }
            try (var statement = held.prepareStatement("SELECT operation_id FROM repository_operation_owners WHERE operation_id=? FOR UPDATE")) {
                statement.setObject(1, owner.key().operationId()); statement.executeQuery().close();
            }
            var contenderPid = new java.util.concurrent.CompletableFuture<Integer>();
            var contender = executor.submit(() -> tx.inTransaction(em -> {
                em.createNativeQuery("SET LOCAL statement_timeout='12s'").executeUpdate();
                contenderPid.complete(((Number) em.createNativeQuery("SELECT pg_backend_pid()").getSingleResult()).intValue());
                return RepositoryOperationLedger.fenceLiveOwner(em, owner);
            }));
            try {
                int waitingPid = contenderPid.get(5, TimeUnit.SECONDS);
                boolean waiting = false;
                long end = System.nanoTime() + Duration.ofSeconds(5).toNanos();
                while (!waiting && System.nanoTime() < end) {
                    waiting = tx.readOnly(em -> (Boolean) em.createNativeQuery(
                            "SELECT :holder=ANY(pg_blocking_pids(:contender))")
                            .setParameter("holder", pid).setParameter("contender", waitingPid).getSingleResult());
                    if (!waiting) Thread.sleep(10);
                }
                assertThat(waiting).as("actual PostgreSQL owner-lock wait").isTrue();
                try (var statement = held.prepareStatement("""
                        SELECT pg_sleep(GREATEST(0,EXTRACT(EPOCH FROM lease_until-clock_timestamp()))+0.02)
                        FROM repository_operation_owners WHERE operation_id=?
                        """)) {
                    statement.setObject(1, owner.key().operationId()); statement.execute();
                }
                if (replace) try (var statement = held.prepareStatement("""
                        UPDATE repository_operation_owners SET owner_generation=owner_generation+1,owner_token=?,
                        lease_until=clock_timestamp()+interval '1 minute' WHERE operation_id=?
                        """)) {
                    statement.setObject(1, UUID.randomUUID()); statement.setObject(2, owner.key().operationId());
                    assertThat(statement.executeUpdate()).isEqualTo(1);
                }
                held.commit();
                assertThatThrownBy(() -> contender.get(15, TimeUnit.SECONDS)).satisfies(failure -> {
                    if (!(failure.getCause() instanceof RepositoryOperationLedger.OwnerFencedException))
                        assertThat(failure.getCause()).hasStackTraceContaining("renewal requires its live owner");
                });
            } finally { held.rollback(); }
        }
    }

    @Test void migrationLeavesExistingOwnerUnstampedUntilItsNextWrite() throws Exception {
        String schema = "write_fence_migration";
        var flyway = org.flywaydb.core.Flyway.configure().dataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())
                .schemas(schema).defaultSchema(schema).locations("classpath:db/migration/repo");
        flyway.target("34").load().migrate();
        var id = UUID.randomUUID(); var token = UUID.randomUUID();
        try (var connection = database.dataSource().getConnection()) {
            connection.setAutoCommit(false);
            try (var statement = connection.createStatement()) { statement.execute("SET LOCAL search_path=write_fence_migration"); }
            try (var statement = connection.prepareStatement("""
                    INSERT INTO repository_operations(account_id,principal,operation_id,command_codec,command_version,command,command_sha256)
                    VALUES ('account','principal',?,'test.fixture',1,decode('01','hex'),sha256(decode('01','hex')))
                    """)) { statement.setObject(1, id); statement.executeUpdate(); }
            try (var statement = connection.prepareStatement("""
                    INSERT INTO repository_operation_owners(account_id,principal,operation_id,owner_token,owner_generation,lease_until)
                    VALUES ('account','principal',?,?,1,clock_timestamp()+interval '1 hour')
                    """)) { statement.setObject(1, id); statement.setObject(2, token); statement.executeUpdate(); }
            connection.commit();
        }
        org.flywaydb.core.Flyway.configure().dataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())
                .schemas(schema).defaultSchema(schema).locations("classpath:db/migration/repo").load().migrate();
        try (var connection = database.dataSource().getConnection()) {
            connection.setAutoCommit(false);
            try (var statement = connection.createStatement()) { statement.execute("SET LOCAL search_path=write_fence_migration"); }
            try (var statement = connection.createStatement(); var result = statement.executeQuery(
                    "SELECT owner_token,owner_generation,write_fence_xid FROM repository_operation_owners")) {
                assertThat(result.next()).isTrue(); assertThat(result.getObject(1)).isEqualTo(token);
                assertThat(result.getLong(2)).isEqualTo(1); assertThat(result.getObject(3)).isNull();
                assertThat(result.next()).isFalse();
            }
            try (var statement = connection.createStatement()) {
                assertThat(statement.executeUpdate("UPDATE repository_operation_owners SET write_fence_xid='1'::xid8")).isEqualTo(1);
            }
            try (var statement = connection.prepareStatement("SELECT require_repository_operation_write_fence('account','principal',?,1)")) {
                statement.setObject(1, id);
                try (var result = statement.executeQuery()) { assertThat(result.next()).isTrue(); assertThat(result.getBoolean(1)).isTrue(); }
            }
            connection.rollback();
        }
    }

    private static RepositoryOperationLedger.Owner owner(Duration lease) {
        var key = new RepositoryOperationLedger.Key("account", "principal", UUID.randomUUID());
        return ledger.admit(key, new RepositoryOperationLedger.EncodedCommand("test.fixture", 1, ByteString.copyFromUtf8("command")),
                UUID.randomUUID(), lease).owner().orElseThrow();
    }

    private static void stamp(Connection connection, RepositoryOperationLedger.Owner owner) throws Exception {
        try (var statement = connection.prepareStatement("""
                UPDATE repository_operation_owners SET write_fence_xid=pg_current_xact_id()
                WHERE account_id=? AND principal=? AND operation_id=? AND owner_generation=? AND owner_token=?
                """)) {
            statement.setString(1, owner.key().account()); statement.setString(2, owner.key().principal());
            statement.setObject(3, owner.key().operationId()); statement.setLong(4, owner.generation());
            statement.setObject(5, owner.token()); assertThat(statement.executeUpdate()).isEqualTo(1);
        }
    }

    private static void probe(Connection connection, RepositoryOperationLedger.Owner owner) throws Exception {
        try (var statement = connection.prepareStatement("INSERT INTO operation_fence_probe VALUES (?,?,?,?)")) {
            statement.setString(1, owner.key().account()); statement.setString(2, owner.key().principal());
            statement.setObject(3, owner.key().operationId()); statement.setLong(4, owner.generation()); statement.executeUpdate();
        }
    }

    private static void probe(jakarta.persistence.EntityManager em, RepositoryOperationLedger.Owner owner) {
        em.createNativeQuery("INSERT INTO operation_fence_probe VALUES (:account,:principal,:operation,:generation)")
                .setParameter("account", owner.key().account()).setParameter("principal", owner.key().principal())
                .setParameter("operation", owner.key().operationId()).setParameter("generation", owner.generation()).executeUpdate();
    }
}
