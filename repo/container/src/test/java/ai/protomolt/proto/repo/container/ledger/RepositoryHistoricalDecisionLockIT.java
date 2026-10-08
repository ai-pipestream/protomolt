package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
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

    @Test void cancellationWaitsForClaimWithoutHoldingOwner() throws Exception {
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
            var cancelled = new AtomicReference<Future<DocumentPublicationReplay.Observation>>();
            c.tx().withTimeouts(new SqlTimeouts(Duration.ofSeconds(10), Duration.ofSeconds(10))).inTransaction(em -> {
                int holder = ((Number) em.createNativeQuery("SELECT pg_backend_pid()").getSingleResult()).intValue();
                em.createNativeQuery("SELECT claim_token FROM repository_execution_claims WHERE operation_id=:o FOR UPDATE")
                        .setParameter("o", key.operationId()).getSingleResult();
                cancelled.set(workers.submit(() -> new DocumentPublicationRejections(c.tx().withTimeouts(
                        new SqlTimeouts(Duration.ofSeconds(10), Duration.ofSeconds(10))))
                        .cancel(CALLER, owner, command, RepositoryReadControl.NONE)));
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                boolean blocked = false;
                while (System.nanoTime() < deadline) {
                    em.createNativeQuery("SELECT pg_stat_clear_snapshot()").getSingleResult();
                    blocked = ((Number) em.createNativeQuery("""
                            SELECT count(*) FROM pg_stat_activity
                            WHERE :holder=ANY(pg_blocking_pids(pid))
                            """).setParameter("holder", holder).getSingleResult()).longValue() == 1;
                    if (blocked) break;
                    java.util.concurrent.locks.LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
                }
                assertThat(blocked).as("cancellation is waiting for the held claim").isTrue();
                // A claim-first decision must leave the owner available while waiting.
                em.createNativeQuery("SELECT owner_token FROM repository_operation_owners WHERE operation_id=:o FOR UPDATE NOWAIT")
                        .setParameter("o", key.operationId()).getSingleResult();
            });
            var result = cancelled.get().get(5, TimeUnit.SECONDS);
            assertThat(result.state()).isEqualTo(DocumentPublicationReplay.State.TERMINATED);
            assertThat(new DocumentPublicationRejections(c.tx()).cancel(CALLER, owner, command, RepositoryReadControl.NONE))
                    .isEqualTo(result);
        }
    }
}
