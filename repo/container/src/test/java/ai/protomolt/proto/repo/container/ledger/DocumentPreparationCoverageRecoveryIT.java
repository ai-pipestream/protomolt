package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static ai.protomolt.proto.repo.container.ledger.DocumentPreparationCoverageReconciliation.Result.*;
import static ai.protomolt.proto.repo.spi.RepositoryReadControl.NONE;
import static org.assertj.core.api.Assertions.*;

/** Actual PostgreSQL commits and blocked transactions; only acknowledgement/barrier injection is synthetic. */
@Testcontainers
class DocumentPreparationCoverageRecoveryIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static final RepositoryCaller CALLER = new RepositoryCaller("principal", true);
    private static final SqlTimeouts TIMEOUTS = new SqlTimeouts(Duration.ofSeconds(15), Duration.ofSeconds(25));

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void lostCommitReplyRetriesTheDurableProof(boolean successor) {
        try (var c = context(POSTGRES, successor ? "latest" : "117")) {
            var budget = new PayloadBudget(64L * 1024 * 1024);
            var record = prepare(c, budget, successor);
            String table = successor ? "repository_preparation_coverage_lineage" : "repository_preparation_coverage_certificates";
            var armed = new AtomicBoolean(true);
            var datasource = DocumentJdbcFaults.afterCommit(c.pool(), () -> {
                if (count(c, table, record) == 1 && armed.compareAndSet(true, false))
                    throw new java.sql.SQLException("injected coverage commit reply loss", "08006");
            });
            try (var emf = jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger", Map.of(
                    "hibernate.connection.datasource", datasource, "hibernate.hbm2ddl.auto", "validate"))) {
                var reconciliation = new DocumentPreparationCoverageReconciliation(new Tx(emf), budget, TIMEOUTS);
                assertThatThrownBy(() -> reconciliation.reconcile(CALLER, record.key(), record.predecessorGeneration(), NONE))
                        .hasStackTraceContaining("coverage commit reply loss");
                assertThat(armed).isFalse();
                assertThat(count(c, table, record)).isEqualTo(1);
                assertThat(count(c, "repository_preparation_coverage_unresolved", record)).isZero();
                assertThat(reconciliation.reconcile(CALLER, record.key(), record.predecessorGeneration(), NONE))
                        .isEqualTo(successor ? ALREADY_VERIFIED_SUCCESSOR : ALREADY_CERTIFIED);
                assertThat(count(c, table, record)).isEqualTo(1);
                assertThat(budget.reservedBytes()).isZero();
            }
        }
    }

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void concurrentReconcilersSerializeOnTheClaimAndReturnOneProof(boolean successor) throws Exception {
        try (var c = context(POSTGRES, successor ? "latest" : "117")) {
            var firstBudget = new PayloadBudget(64L * 1024 * 1024);
            var secondBudget = new PayloadBudget(64L * 1024 * 1024);
            var record = prepare(c, firstBudget, successor);
            String table = successor ? "repository_preparation_coverage_lineage" : "repository_preparation_coverage_certificates";
            var held = new CountDownLatch(1);
            var finish = new CountDownLatch(1);
            var armed = new AtomicBoolean(true);
            var pid = new AtomicInteger();
            var datasource = DocumentJdbcFaults.beforeCommit(c.pool(), connection -> {
                try (var statement = connection.prepareStatement("SELECT pg_backend_pid(),EXISTS(SELECT 1 FROM " + table
                        + " WHERE operation_id=? AND predecessor_generation=?)")) {
                    statement.setObject(1, record.key().operationId()); statement.setLong(2, record.predecessorGeneration());
                    try (var rows = statement.executeQuery()) {
                        rows.next();
                        if (!rows.getBoolean(2) || !armed.compareAndSet(true, false)) return;
                        pid.set(rows.getInt(1));
                    }
                }
                held.countDown();
                try {
                    if (!finish.await(15, TimeUnit.SECONDS)) throw new java.sql.SQLException("coverage commit barrier timed out");
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt(); throw new java.sql.SQLException("coverage commit barrier interrupted", interrupted);
                }
            });
            try (var emf = jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger", Map.of(
                    "hibernate.connection.datasource", datasource, "hibernate.hbm2ddl.auto", "validate"));
                 var workers = Executors.newVirtualThreadPerTaskExecutor()) {
                var first = new DocumentPreparationCoverageReconciliation(new Tx(emf), firstBudget, TIMEOUTS);
                var second = new DocumentPreparationCoverageReconciliation(c.tx(), secondBudget, TIMEOUTS);
                try {
                    var a = workers.submit(() -> first.reconcile(CALLER, record.key(), record.predecessorGeneration(), NONE));
                    assertThat(held.await(10, TimeUnit.SECONDS)).isTrue();
                    var b = workers.submit(() -> second.reconcile(CALLER, record.key(), record.predecessorGeneration(), NONE));
                    boolean blocked = false;
                    long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
                    while (System.nanoTime() < deadline) {
                        blocked = c.tx().readOnly(em -> (Boolean) em.createNativeQuery("""
                                SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE wait_event_type='Lock'
                                 AND :holder=ANY(pg_blocking_pids(pid)) AND query LIKE '%repository_execution_claims%')
                                """).setParameter("holder", pid.get()).getSingleResult());
                        if (blocked) break;
                        Thread.sleep(10);
                    }
                    assertThat(blocked).as("second reconciler blocked on actual claim row").isTrue();
                    assertThat(count(c, table, record)).isZero();
                    assertThat(count(c, "repository_preparation_coverage_unresolved", record)).isEqualTo(1);
                    finish.countDown();
                    assertThat(a.get(10, TimeUnit.SECONDS)).isEqualTo(successor ? VERIFIED_SUCCESSOR : VERIFIED_LIVE);
                    assertThat(b.get(10, TimeUnit.SECONDS)).isEqualTo(successor ? ALREADY_VERIFIED_SUCCESSOR : ALREADY_CERTIFIED);
                    assertThat(count(c, table, record)).isEqualTo(1);
                    assertThat(count(c, "repository_preparation_coverage_unresolved", record)).isZero();
                    assertThat(firstBudget.reservedBytes()).isZero();
                    assertThat(secondBudget.reservedBytes()).isZero();
                } finally { finish.countDown(); }
            }
        }
    }

    private static DocumentPublicationPreparationRecord prepare(Context c, PayloadBudget budget, boolean successor) {
        if (successor) {
            var plan = RepositorySuccessorInstallIT.plan(c);
            RepositorySuccessorInstall.install(c.tx(), budget, CALLER, plan, NONE);
            return plan.next();
        }
        var record = DocumentPublicationPreparationCodecIT.input(c);
        c.tx().inTransaction(em -> {
            RepositoryExecutionClaimLedger.acquireInitialInTransaction(em, record.key(), record.command(), UUID.randomUUID(), record.lease());
            LegacyPublicationPreparationFixture.insertProjected(em, record, List.of());
        });
        DocumentPreparationCoverageCertificatesIT.migrate(c);
        return record;
    }
    private static long count(Context c, String table, DocumentPublicationPreparationRecord record) {
        return c.tx().readOnly(em -> ((Number) em.createNativeQuery("SELECT count(*) FROM " + table
                + " WHERE operation_id=:operation AND predecessor_generation=:generation")
                .setParameter("operation", record.key().operationId()).setParameter("generation", record.predecessorGeneration())
                .getSingleResult()).longValue());
    }
}
