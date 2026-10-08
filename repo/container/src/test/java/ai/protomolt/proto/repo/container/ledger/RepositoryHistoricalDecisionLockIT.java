package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import java.time.Duration;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.context;
import static ai.protomolt.proto.repo.container.ledger.DocumentCaptureAdmissionClosureIT.*;
import static org.assertj.core.api.Assertions.*;

/** Real PostgreSQL decision locking; source fixtures use synthetic provider observations. */
@Testcontainers
class RepositoryHistoricalDecisionLockIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static final RepositoryCaller CALLER = new RepositoryCaller("principal", true);

    @Test void terminalCancellationReplaysAfterClaimAndOwnerExpire() throws Exception {
        var lease = Duration.ofSeconds(5);
        try (var c = context(POSTGRES); var rig = historicalInitial(c, lease); var work = rig.sources().work()) {
            var command = rig.record().command();
            var modes = DocumentPublicationModesJournal.encode(command,
                    java.util.Map.of(command.intent().getMembers(0).getMemberId(), DocumentPublicationCandidate.Mode.TYPED));
            var admission = RepositoryOperationLedger.prepareHistoricalAdmission(rig.record().key(), command,
                    rig.record().seeds().ownerNonce(), lease, work);
            var owner = c.tx().inTransaction(em -> {
                RepositoryExecutionClaimLedger.lockLive(em, rig.claim());
                DocumentPublicationModesJournal.insert(em, rig.claim(), rig.record(), modes);
                return admission.apply(em, rig.claim()).owner().orElseThrow();
            });
            var decisions = new DocumentPublicationRejections(c.tx());
            var result = decisions.cancel(CALLER, owner, command, RepositoryReadControl.NONE);
            assertThat(result.state()).isEqualTo(DocumentPublicationReplay.State.TERMINATED);
            assertThat(result.rejection()).isPresent();
            c.tx().withTimeouts(new SqlTimeouts(Duration.ofSeconds(5), Duration.ofSeconds(10))).inTransaction(em -> {
                em.createNativeQuery("""
                        SELECT pg_sleep(GREATEST(0, extract(epoch FROM (GREATEST(c.lease_until,o.lease_until)-clock_timestamp())))+0.05)
                        FROM repository_execution_claims c JOIN repository_operation_owners o USING(account_id,principal,operation_id)
                        WHERE c.operation_id=:id
                        """).setParameter("id", command.operationId()).getSingleResult();
                assertThat(em.createNativeQuery("""
                        SELECT c.lease_until<=clock_timestamp() AND o.lease_until<=clock_timestamp()
                        FROM repository_execution_claims c JOIN repository_operation_owners o USING(account_id,principal,operation_id)
                        WHERE c.operation_id=:id
                        """).setParameter("id", command.operationId()).getSingleResult()).isEqualTo(true);
            });
            assertThat(decisions.cancel(CALLER, owner, command, RepositoryReadControl.NONE)).isEqualTo(result);
        }
    }

    @Test void cancellationWaitsForClaimWithoutHoldingOwner() throws Exception {
        competingCancellations(1);
    }

    @Test void concurrentCancellationsReturnOneReceipt() throws Exception {
        competingCancellations(2);
    }

    private void competingCancellations(int contenders) throws Exception {
        try (var c = context(POSTGRES); var rig = historicalInitial(c, Duration.ofMinutes(5));
             var work = rig.sources().work();
             var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            var key = rig.record().key();
            var command = rig.record().command();
            var modes = DocumentPublicationModesJournal.encode(command,
                    java.util.Map.of(command.intent().getMembers(0).getMemberId(), DocumentPublicationCandidate.Mode.TYPED));
            var admission = RepositoryOperationLedger.prepareHistoricalAdmission(key, command,
                    rig.record().seeds().ownerNonce(), Duration.ofMinutes(5), work);
            var owner = c.tx().inTransaction(em -> {
                RepositoryExecutionClaimLedger.lockLive(em, rig.claim());
                DocumentPublicationModesJournal.insert(em, rig.claim(), rig.record(), modes);
                return admission.apply(em, rig.claim()).owner().orElseThrow();
            });
            var cancelled = new java.util.ArrayList<Future<DocumentPublicationReplay.Observation>>();
            c.tx().withTimeouts(new SqlTimeouts(Duration.ofSeconds(10), Duration.ofSeconds(10))).inTransaction(em -> {
                int holder = ((Number) em.createNativeQuery("SELECT pg_backend_pid()").getSingleResult()).intValue();
                em.createNativeQuery("SELECT claim_token FROM repository_execution_claims WHERE operation_id=:o FOR UPDATE")
                        .setParameter("o", key.operationId()).getSingleResult();
                for (int i = 0; i < contenders; i++) cancelled.add(workers.submit(() -> new DocumentPublicationRejections(c.tx().withTimeouts(
                        new SqlTimeouts(Duration.ofSeconds(10), Duration.ofSeconds(10))))
                        .cancel(CALLER, owner, command, RepositoryReadControl.NONE)));
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                boolean blocked = false;
                while (System.nanoTime() < deadline) {
                    em.createNativeQuery("SELECT pg_stat_clear_snapshot()").getSingleResult();
                    blocked = ((Number) em.createNativeQuery("""
                            WITH RECURSIVE waiting(pid) AS (
                                SELECT pid FROM pg_stat_activity WHERE :holder=ANY(pg_blocking_pids(pid))
                                UNION
                                SELECT a.pid FROM pg_stat_activity a JOIN waiting w ON w.pid=ANY(pg_blocking_pids(a.pid))
                            ) SELECT count(*) FROM waiting
                            """).setParameter("holder", holder).getSingleResult()).longValue() == contenders;
                    if (blocked) break;
                    java.util.concurrent.locks.LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
                }
                assertThat(blocked).as("all cancellation transactions are waiting for the held claim").isTrue();
                // A claim-first decision must leave the owner available while waiting.
                em.createNativeQuery("SELECT owner_token FROM repository_operation_owners WHERE operation_id=:o FOR UPDATE NOWAIT")
                        .setParameter("o", key.operationId()).getSingleResult();
            });
            var result = cancelled.getFirst().get(5, TimeUnit.SECONDS);
            assertThat(result.state()).isEqualTo(DocumentPublicationReplay.State.TERMINATED);
            assertThat(result.rejection()).isPresent();
            for (var contender : cancelled) assertThat(contender.get(5, TimeUnit.SECONDS)).isEqualTo(result);
            c.tx().readOnly(em -> {
                assertThat(((Number) em.createNativeQuery("""
                        SELECT count(*) FROM repository_operation_rejection
                        WHERE account_id=:a AND principal=:p AND operation_id=:o
                        """).setParameter("a", key.account()).setParameter("p", key.principal())
                        .setParameter("o", key.operationId()).getSingleResult()).longValue()).isEqualTo(1);
                return null;
            });
            assertThat(new DocumentPublicationRejections(c.tx()).cancel(CALLER, owner, command, RepositoryReadControl.NONE))
                    .isEqualTo(result);
        }
    }
}
