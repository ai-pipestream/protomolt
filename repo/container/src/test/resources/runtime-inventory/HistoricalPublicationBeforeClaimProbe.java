package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.*;
import java.lang.reflect.*;
import java.sql.Connection;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.DataSource;

/** Pause only the publication connection so takeover can commit before old claim acquisition. */
final class HistoricalPublicationBeforeClaimProbe {
    @FunctionalInterface interface Publication { void run(Tx tx) throws Exception; }

    static RepositoryCoordinatorReservation.ExpiredUnquiesced run(DataSource database, Tx independent,
            RepositoryCaller coordinator, RepositoryOperationLedger.Owner owner, RepositorySuccessorInstall.Plan plan, DocumentAssessmentCreation.Created stage,
            Publication publication) throws Exception {
        var armed = new AtomicBoolean();
        var publisherPid = new AtomicInteger();
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var source = (DataSource) Proxy.newProxyInstance(DataSource.class.getClassLoader(), new Class<?>[]{DataSource.class},
                (proxy, method, args) -> {
                    Object value = invoke(database, method, args);
                    if (!method.getName().equals("getConnection") || !armed.compareAndSet(true, false)) return value;
                    var connection = (Connection) value;
                    try {
                        if (!connection.getAutoCommit()) throw new AssertionError("Gate must precede transaction admission");
                        try (var query = connection.createStatement(); var rows = query.executeQuery("SELECT pg_backend_pid()")) {
                            if (!rows.next()) throw new AssertionError("Publisher PID missing");
                            publisherPid.set(rows.getInt(1));
                        }
                        entered.countDown();
                        if (!release.await(50, TimeUnit.SECONDS)) throw new AssertionError("Before-claim gate timed out");
                        return connection;
                    } catch (Exception | Error failure) {
                        try { connection.close(); } catch (Exception cleanup) { failure.addSuppressed(cleanup); }
                        throw failure;
                    }
                });
        try (var factory = jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger",
                Map.of("hibernate.connection.datasource", source, "hibernate.hbm2ddl.auto", "validate"))) {
            var executor = Executors.newVirtualThreadPerTaskExecutor();
            Future<?> publisher = null;
            Throwable primary = null;
            try {
                armed.set(true);
                publisher = executor.submit(() -> { publication.run(new Tx(factory).withTimeouts(new SqlTimeouts(Duration.ofSeconds(5), Duration.ofSeconds(10)))); return null; });
                if (!entered.await(10, TimeUnit.SECONDS)) throw new AssertionError("Publisher did not reach connection gate");
                var claim = owner.executionClaim().orElseThrow();
                var identity = new RepositoryCoordinatorDrain.Identity(owner.key(), claim.commandSha256(), claim.epoch(),
                        claim.token(), plan.reservation().successorIncarnation());
                var proposal = new RepositoryCoordinatorReservation.ExpiredUnquiesced(identity, UUID.randomUUID(), UUID.randomUUID(),
                        Duration.ofMinutes(2), new RepositoryCoordinatorReservation.OwnerIdentity(owner.generation(), owner.token()));
                try (var observer = database.getConnection()) {
                    if (expired(observer, owner)) throw new AssertionError("Claim expired before publisher entered gate");
                    long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(35);
                    while (!expired(observer, owner)) {
                        if (publisher.isDone() || System.nanoTime() >= until) throw new AssertionError("Publisher did not remain gated through expiry");
                        Thread.sleep(10);
                    }
                }
                RepositoryCoordinatorExpiration.reserve(independent.withTimeouts(
                        new SqlTimeouts(Duration.ofSeconds(5), Duration.ofSeconds(10))), coordinator, proposal, RepositoryReadControl.NONE);
                if (publisher.isDone() || release.getCount() != 1
                        || RepositoryCoordinatorReservation.confirm(independent, coordinator, proposal, RepositoryReadControl.NONE).isEmpty())
                    throw new AssertionError("Takeover must commit while publisher remains gated");
                independent.readOnly(em -> {
                    Object[] row = (Object[]) em.createNativeQuery("""
                            SELECT claim_epoch,claim_token FROM repository_execution_claims
                            WHERE account_id=:account AND principal=:principal AND operation_id=:op
                            """).setParameter("account", owner.key().account()).setParameter("principal", owner.key().principal())
                            .setParameter("op", owner.key().operationId()).getSingleResult();
                    if (((Number)row[0]).longValue() != claim.epoch() + 1 || !row[1].equals(proposal.successorToken()))
                        throw new AssertionError("Committed takeover did not replace exact claim identity");
                    return null;
                });
                try (var observer = database.getConnection()) {
                    try (var query = observer.prepareStatement("SELECT state='idle' AND xact_start IS NULL AND backend_xid IS NULL FROM pg_stat_activity WHERE pid=?")) {
                        query.setInt(1, publisherPid.get());
                        try (var rows = query.executeQuery()) {
                            if (!rows.next() || !rows.getBoolean(1)) throw new AssertionError("Publisher acquired a transaction before gate release");
                        }
                    }
                    try (var query = observer.prepareStatement("SELECT clock_timestamp()<?")) {
                        query.setObject(1, java.time.OffsetDateTime.ofInstant(stage.retainUntil(), java.time.ZoneOffset.UTC));
                        try (var rows = query.executeQuery()) {
                            rows.next(); if (!rows.getBoolean(1)) throw new AssertionError("Assessment expired before old claim check");
                        }
                    }
                    for (UUID attempt : plan.next().seeds().attempts().values()) {
                        try (var query = observer.prepareStatement("SELECT state='VERIFIED' AND lease_until>clock_timestamp() FROM document_part_attempts WHERE attempt_id=?")) {
                            query.setObject(1, attempt);
                            try (var rows = query.executeQuery()) {
                                if (!rows.next() || !rows.getBoolean(1)) throw new AssertionError("Selected upload is not live and verified");
                            }
                        }
                    }
                }
                release.countDown();
                try {
                    publisher.get(10, TimeUnit.SECONDS);
                    throw new AssertionError("Old publisher passed a replaced claim");
                } catch (ExecutionException failure) {
                    if (!(failure.getCause() instanceof RepositoryExecutionClaimLedger.Fenced))
                        throw new AssertionError("Expected exact replaced-claim rejection", failure);
                }
                for (String table : List.of("repository_operation_success", "document_revision_commits")) {
                    long count = independent.readOnly(em -> ((Number) em.createNativeQuery(
                            "SELECT count(*) FROM " + table + " WHERE account_id=:account AND principal=:principal AND operation_id=:op")
                            .setParameter("account", owner.key().account()).setParameter("principal", owner.key().principal())
                            .setParameter("op", owner.key().operationId()).getSingleResult()).longValue());
                    if (count != 0) throw new AssertionError("Old publisher persisted " + table);
                }
                System.out.println("HISTORICAL_TAKEOVER_BEFORE_CLAIM_OK");
                return proposal;
            } catch (Exception | Error failure) { primary = failure; throw failure; }
            finally {
                release.countDown();
                if (publisher != null && !publisher.isDone()) publisher.cancel(true);
                executor.shutdownNow();
                try {
                    if (!executor.awaitTermination(15, TimeUnit.SECONDS)) throw new AssertionError("Publisher did not drain before factory close");
                } catch (Exception | Error cleanup) {
                    if (primary == null) throw cleanup;
                    if (primary != cleanup) primary.addSuppressed(cleanup);
                }
            }
        }
    }
    private static boolean expired(Connection connection, RepositoryOperationLedger.Owner owner) throws Exception {
        try (var query = connection.prepareStatement("""
                SELECT c.lease_until<=clock_timestamp() AND o.lease_until<=clock_timestamp()
                FROM repository_execution_claims c JOIN repository_operation_owners o USING(account_id,principal,operation_id)
                WHERE c.account_id=? AND c.principal=? AND c.operation_id=?
                """)) {
            query.setString(1, owner.key().account()); query.setString(2, owner.key().principal()); query.setObject(3, owner.key().operationId());
            try (var rows = query.executeQuery()) { if (!rows.next()) throw new AssertionError("Claim owner missing"); return rows.getBoolean(1); }
        }
    }
    private static Object invoke(Object target, Method method, Object[] args) throws Throwable {
        try { return method.invoke(target, args); }
        catch (InvocationTargetException failure) { throw failure.getCause(); }
    }
}
