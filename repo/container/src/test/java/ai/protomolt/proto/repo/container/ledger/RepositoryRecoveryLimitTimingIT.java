package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.RepositoryCaller;
import java.time.Duration;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.context;
import static ai.protomolt.proto.repo.container.ledger.DocumentCaptureAdmissionClosureIT.*;
import static ai.protomolt.proto.repo.container.ledger.RepositoryHistoricalRecoveryBoundIT.*;
import static ai.protomolt.proto.repo.container.ledger.RepositoryHistoricalSuccessorActivationIT.count;
import static org.assertj.core.api.Assertions.*;

/** Real lease expiry and PostgreSQL constraint timing, with no clock or fence overrides. */
@Testcontainers
class RepositoryRecoveryLimitTimingIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    @ParameterizedTest @ValueSource(strings = {"before-rejection", "deferred", "early-constraints"})
    void leaseLivenessIsCheckedAtDecisionAndConstraintExecution(String phase) throws Exception {
        try (var c = context(POSTGRES); var rig = historicalInitial(c, Duration.ofSeconds(10))) {
            for (int i = 0; i < 15; i++) append(c, rig);
            assertThat(count(c, "repository_preparation_pin_batches")).isEqualTo(16);
            var plan = installedHistoricalSuccessor(c, rig, Duration.ofSeconds(2));
            var before = RepositoryHistoricalSuccessorActivationIT.leases(c, rig);
            org.assertj.core.api.ThrowableAssert.ThrowingCallable decision = () -> c.tx().inTransaction(em -> {
                insertDecision(em, rig, plan, "CAPTURES", 16);
                if (!phase.equals("before-rejection")) insertRejection(em, rig, plan);
                if (phase.equals("early-constraints")) em.createNativeQuery("SET CONSTRAINTS ALL IMMEDIATE").executeUpdate();
                em.createNativeQuery("""
                        SELECT pg_sleep(GREATEST(0,EXTRACT(EPOCH FROM (GREATEST(c.lease_until,o.lease_until)-clock_timestamp())))+0.05)
                        FROM repository_execution_claims c JOIN repository_operation_owners o USING(account_id,principal,operation_id)
                        WHERE c.operation_id=:id
                        """).setParameter("id", rig.record().key().operationId()).getSingleResult();
                boolean expired = (Boolean) em.createNativeQuery("""
                        SELECT clock_timestamp()>GREATEST(c.lease_until,o.lease_until)
                        FROM repository_execution_claims c JOIN repository_operation_owners o USING(account_id,principal,operation_id)
                        WHERE c.operation_id=:id
                        """).setParameter("id", rig.record().key().operationId()).getSingleResult();
                assertThat(expired).as("both database leases expired before the next action").isTrue();
                if (phase.equals("before-rejection")) insertRejection(em, rig, plan);
            });
            if (phase.equals("early-constraints")) {
                assertThatCode(decision).doesNotThrowAnyException();
                var receipt = new DocumentPublicationReplay(c.tx()).observe(new RepositoryCaller("principal", true), rig.record().command());
                assertThat(receipt.state()).isEqualTo(DocumentPublicationReplay.State.TERMINATED);
                assertThat(receipt.rejection().orElseThrow().getReasonValue()).isEqualTo(4);
            } else {
                assertThatThrownBy(decision).hasStackTraceContaining("Recovery limit decision requires exact live installed identity");
            }
            assertThat(count(c, "repository_recovery_limit_decisions")).isEqualTo(phase.equals("early-constraints") ? 1 : 0);
            assertThat(count(c, "repository_operation_rejection")).isEqualTo(phase.equals("early-constraints") ? 1 : 0);
            assertThat(count(c, "repository_successor_executions")).isZero();
            assertThat(count(c, "repository_historical_activations")).isZero();
            assertThat(count(c, "repository_preparation_pin_batches")).isEqualTo(16);
            assertThat(count(c, "repository_preparation_capture_drains")).isZero();
            assertThat(RepositoryHistoricalSuccessorActivationIT.leases(c, rig)).containsExactly(before);
        }
    }
}
