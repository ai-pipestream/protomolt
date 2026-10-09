package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.DocumentPublicationCommand;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import java.sql.SQLException;
import java.time.Duration;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.DataSource;
import static ai.protomolt.proto.repo.container.ledger.DocumentCaptureAdmissionClosureIT.Rig;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.Context;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Shared helpers for the coverage qualification suites. Every SQL effect here is real
 * PostgreSQL behavior; the only injected faults are a held or aborted JDBC commit.
 * Nothing in this file creates or enables a prune operation.
 */
final class RepositoryCoverageQualificationSupport {
    private RepositoryCoverageQualificationSupport() {}

    static final RepositoryCaller CALLER = new RepositoryCaller("principal", true);
    static final RepositoryReadControl NONE = RepositoryReadControl.NONE;
    /** Ordinary bounded limits for non-contended calls. */
    static final SqlTimeouts TIMEOUTS = new SqlTimeouts(Duration.ofSeconds(2), Duration.ofSeconds(10));
    /**
     * Lock wait for the operation that deliberately queues behind a held commit. The
     * blocked state is detected within milliseconds and the barrier then opens, so this
     * bound is a safety net against a hung gate, not an expected wait.
     */
    static final SqlTimeouts RACE_TIMEOUTS = new SqlTimeouts(Duration.ofSeconds(15), Duration.ofSeconds(30));

    /** This connection's own uncommitted certificate: the reconciliation certify transaction. */
    static final String CERTIFICATE_MARKER = """
            SELECT count(*),pg_backend_pid() FROM repository_preparation_coverage_certificates
            WHERE operation_id=? AND xmin=pg_current_xact_id()::xid
            """;
    /** This connection's own uncommitted release receipt: the root release transaction. */
    static final String RELEASE_MARKER = """
            SELECT count(*),pg_backend_pid() FROM repository_preparation_root_releases
            WHERE operation_id=? AND creation_xid=pg_current_xact_id()
            """;

    /**
     * One-shot barrier on the first commit whose connection observes {@code marker}.
     * The transaction stays open with every lock it holds until {@link #open()}; when
     * {@code abort} is set the real commit is replaced by a serialization failure and
     * the transaction rolls back.
     */
    static final class CommitGate {
        final CountDownLatch entered = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);
        final AtomicInteger pid = new AtomicInteger();
        private final AtomicBoolean fired = new AtomicBoolean();
        private final String marker;
        private final UUID operation;
        private final boolean abort;

        CommitGate(String marker, UUID operation, boolean abort) {
            this.marker = marker; this.operation = operation; this.abort = abort;
        }

        DataSource wrap(DataSource delegate) {
            return DocumentJdbcFaults.beforeCommit(delegate, connection -> {
                try (var statement = connection.prepareStatement(marker)) {
                    statement.setObject(1, operation);
                    try (var rows = statement.executeQuery()) {
                        rows.next();
                        if (rows.getInt(1) != 1 || !fired.compareAndSet(false, true)) return;
                        pid.set(rows.getInt(2));
                        entered.countDown();
                        try {
                            if (!release.await(15, TimeUnit.SECONDS)) throw new SQLException("commit gate timed out");
                        } catch (InterruptedException interrupted) {
                            Thread.currentThread().interrupt();
                            throw new SQLException("commit gate interrupted", interrupted);
                        }
                        if (abort) throw new SQLException("gated commit deliberately aborted", "40001");
                    }
                }
            });
        }

        boolean fired() { return fired.get(); }
        void open() { release.countDown(); }
    }

    static jakarta.persistence.EntityManagerFactory factory(DataSource datasource) {
        return jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger",
                Map.of("hibernate.connection.datasource", datasource, "hibernate.hbm2ddl.auto", "validate"));
    }

    /**
     * True once {@code waiter} is a live backend blocked on the execution-claim row lock
     * held by {@code blocker}, observed through pg_blocking_pids. Surfaces the waiter's
     * failure instead of timing out when it finishes early.
     */
    static boolean awaitBlockedOnClaim(Context c, int blocker, Future<?> waiter, Duration bound) throws Exception {
        long deadline = System.nanoTime() + bound.toNanos();
        while (System.nanoTime() < deadline) {
            if (waiter.isDone()) waiter.get();
            boolean blocked = c.tx().readOnly(em -> ((Number) em.createNativeQuery("""
                    SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid()
                     AND state='active' AND wait_event_type='Lock'
                     AND query ILIKE '%repository_execution_claims%FOR UPDATE%'
                     AND :blocker=ANY(pg_blocking_pids(pid))
                    """).setParameter("blocker", blocker).getSingleResult()).longValue() > 0);
            if (blocked) return true;
            Thread.sleep(10);
        }
        return false;
    }

    static long count(Context c, String table, UUID operation) {
        return c.tx().readOnly(em -> ((Number) em.createNativeQuery("SELECT count(*) FROM " + table + " WHERE operation_id=:o")
                .setParameter("o", operation).getSingleResult()).longValue());
    }

    static long count(Context c, String table, UUID operation, long generation) {
        return c.tx().readOnly(em -> ((Number) em.createNativeQuery("SELECT count(*) FROM " + table
                + " WHERE operation_id=:o AND predecessor_generation=:g")
                .setParameter("o", operation).setParameter("g", generation).getSingleResult()).longValue());
    }

    static long total(Context c, String table) {
        return c.tx().readOnly(em -> ((Number) em.createNativeQuery("SELECT count(*) FROM " + table).getSingleResult()).longValue());
    }

    /** Certificate row for one preparation, or null. Column order is documented in the evidence note. */
    static Object[] certificate(Context c, UUID operation, long generation) {
        var rows = c.tx().readOnly(em -> em.createNativeQuery("""
                SELECT proof_kind,encode(preparation_sha256,'hex'),encode(command_sha256,'hex'),root_count,
                 encode(roots_sha256,'hex'),verifier_version
                FROM repository_preparation_coverage_certificates WHERE operation_id=:o AND predecessor_generation=:g
                """).setParameter("o", operation).setParameter("g", generation).getResultList());
        return rows.isEmpty() ? null : (Object[]) rows.getFirst();
    }

    /** Lineage row for one successor preparation, or null. */
    static Object[] lineage(Context c, UUID operation, long generation) {
        var rows = c.tx().readOnly(em -> em.createNativeQuery("""
                SELECT anchor_generation,encode(anchor_sha256,'hex'),depth,root_count,encode(roots_sha256,'hex'),
                 encode(preparation_sha256,'hex'),encode(previous_preparation_sha256,'hex'),encode(command_sha256,'hex')
                FROM repository_preparation_coverage_lineage WHERE operation_id=:o AND predecessor_generation=:g
                """).setParameter("o", operation).setParameter("g", generation).getResultList());
        return rows.isEmpty() ? null : (Object[]) rows.getFirst();
    }

    /** Release receipt row for one preparation, or null. */
    static Object[] release(Context c, UUID operation, long generation) {
        var rows = c.tx().readOnly(em -> em.createNativeQuery("""
                SELECT encode(preparation_sha256,'hex'),encode(command_sha256,'hex'),root_count,encode(roots_sha256,'hex'),
                 capture_count,encode(captures_sha256,'hex'),terminal_kind,
                 encode(repository_preparation_capture_fingerprint(account_id,principal,operation_id,predecessor_generation),'hex')
                FROM repository_preparation_root_releases WHERE operation_id=:o AND predecessor_generation=:g
                """).setParameter("o", operation).setParameter("g", generation).getResultList());
        return rows.isEmpty() ? null : (Object[]) rows.getFirst();
    }

    static String preparationSha(DocumentPublicationPreparationRecord record) {
        return HexFormat.of().formatHex(DocumentPublicationPreparationJournal.digest(DocumentPublicationPreparationCodec.encode(record)));
    }

    static String rootsSha(DocumentPublicationPreparationRecord record) {
        return HexFormat.of().formatHex(DocumentPreparationHistoryRoots.digest(DocumentPreparationHistoryRoots.roots(record.command())));
    }

    static Object[] leases(Context c, UUID operation) {
        return c.tx().readOnly(em -> (Object[]) em.createNativeQuery("""
                SELECT c.lease_until,o.lease_until FROM repository_execution_claims c
                LEFT JOIN repository_operation_owners o USING(account_id,principal,operation_id) WHERE c.operation_id=:o
                """).setParameter("o", operation).getSingleResult());
    }

    /**
     * A retained root owner written by the actual pre-V118 writer under an older schema,
     * with an explicit operation id so keyset order is deterministic. The template
     * supplies command members, placements and lease.
     */
    static DocumentPublicationPreparationRecord legacyRootOwner(Context c, DocumentPublicationPreparationRecord template,
            String principal, UUID operation) {
        var command = new DocumentPublicationCommand(template.command().intent().toBuilder().setOperationId(operation.toString()).build());
        var key = new RepositoryOperationLedger.Key(template.key().account(), principal, command.operationId());
        var record = new DocumentPublicationPreparationRecord(key, command, DocumentPublicationSeeds.mint(key, command),
                template.placements(), template.lease(), 0);
        c.tx().inTransaction(em -> {
            RepositoryExecutionClaimLedger.acquireInitialInTransaction(em, key, command, UUID.randomUUID(), record.lease());
            LegacyPublicationPreparationFixture.insertProjected(em, record, List.of());
        });
        return record;
    }

    static void waitExpired(Context c, Rig rig) {
        c.tx().readOnly(em -> em.createNativeQuery("""
                SELECT pg_sleep(GREATEST(0,EXTRACT(EPOCH FROM (GREATEST(c.lease_until,o.lease_until)-clock_timestamp())))+0.05)
                FROM repository_execution_claims c JOIN repository_operation_owners o USING(account_id,principal,operation_id)
                WHERE c.operation_id=:id
                """).setParameter("id", rig.record().key().operationId()).getSingleResult());
    }

    /**
     * Installs the next unactivated successor through the supported supersession protocol:
     * expired leases, recovery discovery, an immutable supersession reservation and the
     * V93 install. No lease or recovery limit is bypassed and nothing is activated.
     */
    static RepositorySuccessorInstall.Plan installNextUnactivated(Context c, Rig rig,
            RepositorySuccessorInstall.Plan current, Duration lease) {
        waitExpired(c, rig);
        var observed = new RepositoryCoordinatorRecoveryDiscovery(c.tx(), TIMEOUTS)
                .inspect(CALLER, rig.record().key(), rig.record().command().sha256(), NONE);
        assertThat(observed.status()).isEqualTo(RepositoryCoordinatorRecoveryDiscovery.Status.INSTALLED_NOT_ACTIVATED);
        var source = observed.unactivated().orElseThrow();
        var reservation = new RepositoryCoordinatorReservation.SupersededUnactivated(source.predecessor(),
                UUID.randomUUID(), UUID.randomUUID(), lease, source.owner(), source.preparationSha256(), source.installation());
        RepositoryCoordinatorSupersession.reserve(c.tx(), CALLER, reservation, NONE);
        var next = RepositorySuccessorInstall.prepare(reservation, current.next(), lease, current.modes());
        RepositorySuccessorInstall.install(c.tx(), rig.budget(), CALLER, next, NONE);
        return next;
    }
}
