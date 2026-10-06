package ai.protomolt.proto.repo.container.ledger;

import jakarta.persistence.EntityManagerFactory;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/** Pauses an actual success-writing JDBC transaction before commit; all SQL still reaches PostgreSQL. */
final class DocumentPublicationCommitBarrier implements AutoCloseable {
    private final CompletableFuture<Integer> entered = new CompletableFuture<>();
    private final CompletableFuture<Void> release = new CompletableFuture<>();
    private final java.util.concurrent.atomic.AtomicBoolean armed = new java.util.concurrent.atomic.AtomicBoolean(true);
    private final EntityManagerFactory factory;
    private final Tx tx;

    DocumentPublicationCommitBarrier(javax.sql.DataSource source, UUID operation) {
        var observed = DocumentJdbcFaults.beforeCommit(source, connection -> {
            if (!armed.get()) return;
            try (var statement = connection.prepareStatement(
                    "SELECT pg_backend_pid() FROM repository_operation_success WHERE operation_id=?")) {
                statement.setObject(1, operation);
                try (var rows = statement.executeQuery()) {
                    if (!rows.next() || !armed.compareAndSet(true, false)) return;
                    entered.complete(rows.getInt(1));
                }
            }
            try { release.get(30, TimeUnit.SECONDS); }
            catch (InterruptedException failure) {
                Thread.currentThread().interrupt();
                throw new java.sql.SQLException("Publication barrier interrupted", failure);
            } catch (java.util.concurrent.ExecutionException | java.util.concurrent.TimeoutException failure) {
                throw new java.sql.SQLException("Publication barrier was not released", failure);
            }
        });
        factory = jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger",
                Map.of("hibernate.connection.datasource", observed, "hibernate.hbm2ddl.auto", "validate"));
        tx = new Tx(factory);
    }

    Tx tx() { return tx; }
    int awaitCommit() throws Exception { return entered.get(30, TimeUnit.SECONDS); }
    void release() { release.complete(null); }

    static void awaitGrantRevocationWaiter(Tx tx, int publisher) throws Exception {
        long deadline = System.nanoTime()+TimeUnit.SECONDS.toNanos(10);
        while (System.nanoTime()<deadline) {
            var rows = tx.readOnly(em -> em.createNativeQuery("""
                    SELECT pid FROM pg_stat_activity WHERE :publisher=ANY(pg_blocking_pids(pid))
                      AND wait_event_type='Lock' AND query LIKE '%UPDATE repository_creation_grants SET revoked%'
                    """).setParameter("publisher", publisher).getResultList());
            if (!rows.isEmpty()) return;
            Thread.sleep(10);
        }
        throw new AssertionError("Grant revocation did not wait on the publication transaction");
    }

    @Override public void close() { release(); factory.close(); }
}
