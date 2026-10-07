package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import java.sql.Connection;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentCaptureAdmissionClosureIT.historicalInitial;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.context;
import static org.assertj.core.api.Assertions.*;

/** A V83 retry must check expiry after waiting for the actual PostgreSQL row lock. */
@Testcontainers
class DocumentHistoricalStartExpiryWaitIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static final RepositoryCaller CALLER = new RepositoryCaller("principal", true);
    private static final RepositoryReadControl NONE = RepositoryReadControl.NONE;

    @Test void lockedExistingStartExpiresWhileExactRetryWaits() throws Exception {
        try (var c = context(POSTGRES); var rig = historicalInitial(c, Duration.ofMinutes(5))) {
            var record = rig.record();
            var command = record.command();
            var retention = Duration.ofSeconds(5);
            var modes = Map.of(command.intent().getMembers(0).getMemberId(), DocumentPublicationCandidate.Mode.TYPED);
            RepositoryOperationLedger.Owner owner;
            try (var work = rig.sources().work()) {
                var admission = RepositoryOperationLedger.prepareHistoricalAdmission(record.key(), command,
                        record.seeds().ownerNonce(), record.lease(), work);
                owner = c.tx().inTransaction(em -> {
                    RepositoryExecutionClaimLedger.lockLive(em, rig.claim());
                    DocumentPublicationModesJournal.insert(em, rig.claim(), record,
                            DocumentPublicationModesJournal.encode(command, modes));
                    return admission.apply(em, rig.claim()).owner().orElseThrow();
                });
            }
            var original = c.tx().inTransaction(em -> {
                RepositoryExecutionClaimLedger.lockLive(em, rig.claim());
                em.createNativeQuery("""
                        SELECT owner_nonce FROM repository_publication_modes
                        WHERE operation_id=:o AND predecessor_generation=0 FOR UPDATE
                        """).setParameter("o", command.operationId()).getSingleResult();
                RepositoryOperationLedger.fenceLiveOwner(em, owner);
                RepositoryOperationLedger.requireCommand(em, owner.key(), command);
                return DocumentAssessmentStartJournal.startOrLoadHistorical(em, owner, command, retention, NONE);
            });

            try (Connection holder = c.pool().getConnection();
                 var workers = Executors.newVirtualThreadPerTaskExecutor()) {
                holder.setAutoCommit(false);
                int holderPid;
                try (var statement = holder.prepareStatement("""
                        SELECT pg_backend_pid(),assessment_id FROM repository_publication_assessment_starts
                        WHERE operation_id=? FOR UPDATE
                        """)) {
                    statement.setObject(1, command.operationId());
                    try (var row = statement.executeQuery()) {
                        assertThat(row.next()).isTrue();
                        holderPid = row.getInt(1);
                        assertThat(row.getObject(2)).isEqualTo(original.assessment());
                    }
                }
                var retry = workers.submit(() -> c.tx().inTransaction(em -> {
                    RepositoryExecutionClaimLedger.lockLive(em, rig.claim());
                    RepositoryOperationLedger.fenceLiveOwner(em, owner);
                    RepositoryOperationLedger.requireCommand(em, owner.key(), command);
                    return DocumentAssessmentStartJournal.startOrLoadHistorical(em, owner, command, retention, NONE);
                }));
                try {
                    long waitDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                    boolean blocked = false;
                    while (!blocked && System.nanoTime() < waitDeadline) {
                        if (retry.isDone()) retry.get();
                        blocked = c.tx().readOnly(em -> (Boolean) em.createNativeQuery("""
                                SELECT EXISTS(SELECT 1 FROM pg_stat_activity
                                  WHERE datname=current_database() AND wait_event_type='Lock'
                                    AND :holder=ANY(pg_blocking_pids(pid))
                                    AND query LIKE '%repository_publication_assessment_starts%')
                                """).setParameter("holder", holderPid).getSingleResult());
                        if (!blocked) Thread.sleep(10);
                    }
                    assertThat(blocked).as("exact V83 retry waits for the holder's row lock").isTrue();
                    assertThat(live(c, command.operationId())).as("deadline is live while retry is blocked").isTrue();
                    // The wait is calculated from PostgreSQL's stored deadline and database clock.
                    c.tx().readOnly(em -> em.createNativeQuery("""
                            SELECT pg_sleep(GREATEST(0,EXTRACT(EPOCH FROM (retain_until-clock_timestamp())))+0.05)
                            FROM repository_publication_assessment_starts WHERE operation_id=:o
                            """).setParameter("o", command.operationId()).getSingleResult());
                    assertThat(live(c, command.operationId())).as("deadline passed before releasing the lock").isFalse();
                    assertThat(retry.isDone()).as("retry still waits on the actual V83 row").isFalse();
                    holder.commit();
                    assertThatThrownBy(() -> retry.get(10, TimeUnit.SECONDS))
                            .hasStackTraceContaining("Historical assessment start has expired");
                } finally {
                    // Release the waiter even if an assertion fails before the planned commit.
                    holder.rollback();
                }
            }
            var persisted = c.tx().readOnly(em -> (Object[]) em.createNativeQuery("""
                    SELECT assessment_id,retain_until FROM repository_publication_assessment_starts WHERE operation_id=:o
                    """).setParameter("o", command.operationId()).getSingleResult());
            assertThat(persisted[0]).isEqualTo(original.assessment());
            assertThat(instant(persisted[1])).isEqualTo(original.retainUntil());
        }
    }

    private static boolean live(DocumentNativePublicationFixture.Context c, java.util.UUID operation) {
        return c.tx().readOnly(em -> (Boolean) em.createNativeQuery("""
                SELECT retain_until>clock_timestamp() FROM repository_publication_assessment_starts
                WHERE operation_id=:o
                """).setParameter("o", operation).getSingleResult());
    }

    private static Instant instant(Object value) {
        return switch (value) {
            case Instant time -> time;
            case OffsetDateTime time -> time.toInstant();
            case java.sql.Timestamp time -> time.toInstant();
            default -> throw new IllegalStateException("Unsupported database timestamp: " + value.getClass());
        };
    }
}
