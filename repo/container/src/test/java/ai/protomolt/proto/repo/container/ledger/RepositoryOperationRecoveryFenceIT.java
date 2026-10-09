package ai.protomolt.proto.repo.container.ledger;

import com.google.protobuf.ByteString;
import jakarta.persistence.EntityManager;
import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.*;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static org.assertj.core.api.Assertions.*;

/** Real PostgreSQL recovery proof, separate from owner liveness and retention decisions. */
@Testcontainers
class RepositoryOperationRecoveryFenceIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static LedgerDatabase database;
    private static Tx tx;
    private static RepositoryOperationLedger ledger;

    @BeforeAll static void open() {
        database = new LedgerDatabase(new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()));
        tx = new Tx(database.entityManagerFactory());
        ledger = new RepositoryOperationLedger(tx);
    }
    @AfterAll static void close() { database.close(); }

    @Test void recoveryPreservesBusinessOwnershipAndDoesNotCreateWriteProof() {
        var owner = owner(Duration.ofMinutes(1));
        Object[] before = state(owner);
        tx.inTransaction(em -> {
            assertThat(stamp(em, owner)).isEqualTo(owner.generation());
            assertThat(stamp(em, owner)).isEqualTo(owner.generation());
            require(em, owner);
            boolean current = (Boolean) em.createNativeQuery("""
                    SELECT recovery_fence_xid=pg_current_xact_id() AND write_fence_xid<>pg_current_xact_id()
                    FROM repository_operation_owners WHERE operation_id=:id
                    """).setParameter("id", owner.key().operationId()).getSingleResult();
            assertThat(current).isTrue();
        });
        assertThat(state(owner)).containsExactly(before);
        assertThatThrownBy(() -> tx.inTransaction(em -> { require(em, owner); }))
                .hasStackTraceContaining("recovery fence in this transaction");
    }

    @Test void expiredOwnerCanBeRecoveryFencedButCannotStageOrRenew() {
        var owner = owner(Duration.ofSeconds(1));
        awaitExpiry(owner);
        Object[] before = state(owner);
        tx.inTransaction(em -> { stamp(em, owner); require(em, owner); });
        assertThat(state(owner)).containsExactly(before);
        assertThatThrownBy(() -> tx.inTransaction(em -> {
            stamp(em, owner);
            em.createNativeQuery("SELECT require_repository_operation_write_fence(:account,'principal',:id,1)")
                    .setParameter("account", owner.key().account()).setParameter("id", owner.key().operationId()).getSingleResult();
        })).hasStackTraceContaining("live owner write fence");
        assertThatThrownBy(() -> ledger.renew(owner, Duration.ofMinutes(1)))
                .isInstanceOf(RepositoryOperationLedger.OwnerFencedException.class);
        assertThatThrownBy(() -> new RepositorySchemaArtifacts(tx).stage(owner,
                java.util.List.of(ByteString.copyFromUtf8("storage fixture")), () -> {}))
                .isInstanceOf(RepositoryOperationLedger.OwnerFencedException.class);
    }

    @Test void recoveryPreservesPreviouslyEarnedWriteProofButOrdinaryWriteClearsRecovery() {
        var owner = owner(Duration.ofMinutes(1));
        tx.inTransaction(em -> {
            RepositoryOperationLedger.fenceLiveOwner(em, owner);
            stamp(em, owner);
            require(em, owner);
            boolean both = (Boolean) em.createNativeQuery("""
                    SELECT write_fence_xid=pg_current_xact_id() AND recovery_fence_xid=pg_current_xact_id()
                    FROM repository_operation_owners WHERE operation_id=:id
                    """).setParameter("id", owner.key().operationId()).getSingleResult();
            assertThat(both).isTrue();
            RepositoryOperationLedger.fenceLiveOwner(em, owner);
            boolean cleared = (Boolean) em.createNativeQuery("SELECT recovery_fence_xid IS NULL FROM repository_operation_owners WHERE operation_id=:id")
                    .setParameter("id", owner.key().operationId()).getSingleResult();
            assertThat(cleared).isTrue();
        });
    }

    @Test void forgedRecoveryIdentifierIsReplacedAndCannotChangeExpiredOwner() {
        var owner = owner(Duration.ofSeconds(1));
        awaitExpiry(owner);
        tx.inTransaction(em -> {
            em.createNativeQuery("UPDATE repository_operation_owners SET recovery_fence_xid='17'::xid8 WHERE operation_id=:id")
                    .setParameter("id", owner.key().operationId()).executeUpdate();
            require(em, owner);
            boolean actual = (Boolean) em.createNativeQuery("SELECT recovery_fence_xid=pg_current_xact_id() FROM repository_operation_owners WHERE operation_id=:id")
                    .setParameter("id", owner.key().operationId()).getSingleResult();
            assertThat(actual).isTrue();
        });
        assertThatThrownBy(() -> tx.inTransaction(em -> {
            em.createNativeQuery("""
                    UPDATE repository_operation_owners SET recovery_fence_xid='0'::xid8,
                    lease_until=clock_timestamp()+interval '1 minute' WHERE operation_id=:id
                    """).setParameter("id", owner.key().operationId()).executeUpdate();
        })).hasStackTraceContaining("renewal requires its live owner");
        assertThatThrownBy(() -> tx.inTransaction(em -> {
            em.createNativeQuery("""
                    UPDATE repository_operation_owners SET recovery_fence_xid='0'::xid8,
                    write_fence_xid=pg_current_xact_id() WHERE operation_id=:id
                    """).setParameter("id", owner.key().operationId()).executeUpdate();
        })).hasStackTraceContaining("renewal requires its live owner");
    }

    @Test void proofIsScopedAndRollbackDoesNotPersistIt() {
        var owner = owner(Duration.ofMinutes(1));
        var other = owner(Duration.ofMinutes(1));
        assertThatThrownBy(() -> tx.inTransaction(em -> { stamp(em, owner); require(em, other); }))
                .hasStackTraceContaining("recovery fence in this transaction");
        boolean absent = tx.readOnly(em -> (Boolean) em.createNativeQuery("SELECT recovery_fence_xid IS NULL FROM repository_operation_owners WHERE operation_id=:id")
                .setParameter("id", owner.key().operationId()).getSingleResult());
        assertThat(absent).isTrue();
        var missing = new RepositoryOperationLedger.Owner(new RepositoryOperationLedger.Key("missing", "principal", UUID.randomUUID()),
                1, UUID.randomUUID(), owner.leaseUntil());
        assertThatThrownBy(() -> tx.inTransaction(em -> { stamp(em, missing); }))
                .hasStackTraceContaining("requires an existing operation owner");
    }

    @Test void takeoverClearsPriorRecoveryProof() {
        var owner = owner(Duration.ofSeconds(1));
        awaitExpiry(owner);
        tx.inTransaction(em -> { stamp(em, owner); });
        var next = ledger.takeOver(owner.key(), owner.generation(), UUID.randomUUID(), Duration.ofMinutes(1));
        boolean absent = tx.readOnly(em -> (Boolean) em.createNativeQuery("SELECT recovery_fence_xid IS NULL FROM repository_operation_owners WHERE operation_id=:id")
                .setParameter("id", owner.key().operationId()).getSingleResult());
        assertThat(absent).isTrue();
        assertThat(next.generation()).isEqualTo(2);
        tx.inTransaction(em -> {
            assertThat(stamp(em, owner)).isEqualTo(2);
            require(em, next);
        });
        assertThat(state(owner)[3]).isEqualTo(next.token());
    }

    @Test void takeoverWaitsForRecoveryTransactionAndClearsItsProof() throws Exception {
        var owner = owner(Duration.ofSeconds(1));
        awaitExpiry(owner);
        try (var held = database.dataSource().getConnection();
                var executor = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            held.setAutoCommit(false);
            int holderPid;
            try (var statement = held.createStatement(); var result = statement.executeQuery("SELECT pg_backend_pid()")) {
                assertThat(result.next()).isTrue(); holderPid = result.getInt(1);
            }
            try (var statement = held.prepareStatement("SELECT fence_repository_operation_recovery(?,?,?)")) {
                statement.setString(1, owner.key().account()); statement.setString(2, owner.key().principal());
                statement.setObject(3, owner.key().operationId()); statement.executeQuery().close();
            }
            var contenderPid = new java.util.concurrent.CompletableFuture<Integer>();
            UUID nextToken = UUID.randomUUID();
            var contender = executor.submit(() -> {
                try (var connection = database.dataSource().getConnection()) {
                    connection.setAutoCommit(false);
                    try (var statement = connection.createStatement()) { statement.execute("SET LOCAL statement_timeout='10s'"); }
                    try (var statement = connection.createStatement(); var result = statement.executeQuery("SELECT pg_backend_pid()")) {
                        result.next(); contenderPid.complete(result.getInt(1));
                    }
                    try (var statement = connection.prepareStatement("""
                            UPDATE repository_operation_owners SET owner_generation=owner_generation+1,
                            owner_token=?,lease_until=clock_timestamp()+interval '1 minute'
                            WHERE operation_id=? AND owner_generation=1 RETURNING recovery_fence_xid IS NULL
                            """)) {
                        statement.setObject(1, nextToken); statement.setObject(2, owner.key().operationId());
                        try (var result = statement.executeQuery()) {
                            assertThat(result.next()).isTrue(); assertThat(result.getBoolean(1)).isTrue();
                        }
                    }
                    connection.commit();
                    return true;
                }
            });
            try {
                int waitingPid = contenderPid.get(5, java.util.concurrent.TimeUnit.SECONDS);
                long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
                boolean waiting = false;
                while (!waiting && System.nanoTime() < deadline) {
                    waiting = tx.readOnly(em -> (Boolean) em.createNativeQuery("SELECT :holder=ANY(pg_blocking_pids(:contender))")
                            .setParameter("holder", holderPid).setParameter("contender", waitingPid).getSingleResult());
                    if (!waiting) Thread.sleep(10);
                }
                assertThat(waiting).as("takeover is blocked on the recovery owner lock").isTrue();
                assertThat(contender.isDone()).isFalse();
                held.commit();
                assertThat(contender.get(10, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
            } finally { held.rollback(); }
            Object[] after = state(owner);
            assertThat(after[3]).isEqualTo(nextToken);
            assertThat(((Number) after[4]).longValue()).isEqualTo(2);
        }
    }

    private static long stamp(EntityManager em, RepositoryOperationLedger.Owner owner) {
        return ((Number) em.createNativeQuery("SELECT fence_repository_operation_recovery(:account,:principal,:id)")
                .setParameter("account", owner.key().account()).setParameter("principal", owner.key().principal())
                .setParameter("id", owner.key().operationId()).getSingleResult()).longValue();
    }
    private static void require(EntityManager em, RepositoryOperationLedger.Owner owner) {
        em.createNativeQuery("SELECT require_repository_operation_recovery_fence(:account,:principal,:id)")
                .setParameter("account", owner.key().account()).setParameter("principal", owner.key().principal())
                .setParameter("id", owner.key().operationId()).getSingleResult();
    }
    private static Object[] state(RepositoryOperationLedger.Owner owner) {
        return tx.readOnly(em -> (Object[]) em.createNativeQuery("""
                SELECT account_id,principal,operation_id,owner_token,owner_generation,lease_until,write_fence_xid::text
                FROM repository_operation_owners WHERE operation_id=:id
                """).setParameter("id", owner.key().operationId()).getSingleResult());
    }
    private static void awaitExpiry(RepositoryOperationLedger.Owner owner) {
        tx.readOnly(em -> em.createNativeQuery("""
                SELECT pg_sleep(GREATEST(0,EXTRACT(EPOCH FROM lease_until-clock_timestamp()))+0.02)
                FROM repository_operation_owners WHERE operation_id=:id
                """).setParameter("id", owner.key().operationId()).getSingleResult());
    }
    private static RepositoryOperationLedger.Owner owner(Duration lease) {
        return ledger.admit(new RepositoryOperationLedger.Key(UUID.randomUUID().toString(), "principal", UUID.randomUUID()),
                new RepositoryOperationLedger.EncodedCommand("test.fixture", 1, ByteString.copyFromUtf8("command")),
                UUID.randomUUID(), lease).owner().orElseThrow();
    }
}
