package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import java.time.Duration;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.context;
import static ai.protomolt.proto.repo.container.ledger.DocumentCaptureAdmissionClosureIT.*;
import static ai.protomolt.proto.repo.container.ledger.RepositoryHistoricalSuccessorActivationIT.*;
import static org.assertj.core.api.Assertions.*;

/** Administrative corruption only in an isolated test database; runtime guards are restored before checking. */
@Testcontainers
class RepositoryHistoricalLimitCorruptionIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static final RepositoryCaller CALLER=new RepositoryCaller("principal",true);
    private static final RepositoryReadControl NONE=RepositoryReadControl.NONE;

    @ParameterizedTest
    @CsvSource({
            "root_digest,Preparation history projection differs from its canonical command",
            "missing_root,Preparation history projection differs from its canonical command",
            "pin,Recovery capture contents differ",
            "owner,Recovery limit requires original capture ownership",
            "initial,Recovery limit requires original capture ownership"})
    void corruptedEvidenceCannotBecomeALimitReceipt(String kind,String expected) throws Exception {
        try (var c=context(POSTGRES); var rig=historicalInitial(c,Duration.ofSeconds(10))) {
            for (int i=0;i<15;i++) append(c,rig);
            var plan=installedHistoricalSuccessor(c,rig,Duration.ofMinutes(5));
            var before=leases(c,rig);
            var successes=count(c,"repository_operation_success");
            var table=switch(kind) {
                case "root_digest" -> "repository_preparation_history_sets";
                case "missing_root" -> "repository_preparation_history_roots";
                case "pin" -> "repository_preparation_source_pins";
                case "owner" -> "repository_preparation_pin_owners";
                case "initial" -> "repository_preparation_pin_batches";
                default -> throw new IllegalArgumentException(kind);
            };
            var guard=switch(kind) {
                case "root_digest" -> "repository_preparation_history_set_guard";
                case "missing_root" -> "repository_preparation_history_root_guard";
                case "pin" -> "repository_preparation_source_pin_guard";
                case "owner" -> "repository_preparation_pin_owner_guard";
                case "initial" -> "repository_preparation_pin_batch_guard";
                default -> throw new IllegalArgumentException(kind);
            };
            var mutation=switch(kind) {
                case "root_digest" -> "UPDATE repository_preparation_history_sets SET roots_sha256=sha256('corrupt'::bytea) WHERE operation_id=:id";
                case "missing_root" -> "DELETE FROM repository_preparation_history_roots WHERE operation_id=:id";
                case "pin" -> "UPDATE repository_preparation_source_pins SET publication_revision=publication_revision+1 WHERE operation_id=:id";
                case "owner" -> "UPDATE repository_preparation_pin_owners SET claim_token=gen_random_uuid() WHERE operation_id=:id";
                case "initial" -> "UPDATE repository_preparation_pin_batches SET initial_capture=false WHERE operation_id=:id AND initial_capture";
                default -> throw new IllegalArgumentException(kind);
            };
            c.tx().inTransaction(em -> {
                em.createNativeQuery("ALTER TABLE "+table+" DISABLE TRIGGER "+guard).executeUpdate();
                assertThat(em.createNativeQuery(mutation).setParameter("id",rig.record().key().operationId()).executeUpdate()).isPositive();
                em.createNativeQuery("ALTER TABLE "+table+" ENABLE TRIGGER "+guard).executeUpdate();
            });
            var enabled=c.tx().readOnly(em -> em.createNativeQuery("""
                    SELECT tgenabled::text FROM pg_trigger WHERE tgrelid=CAST(:table AS regclass) AND tgname=:guard
                    """).setParameter("table",table).setParameter("guard",guard).getSingleResult());
            assertThat(enabled).as("normal guard restored before runtime decision").isEqualTo("O");
            var decisions=new RepositoryHistoricalLimitDecisions(c.tx(),rig.budget());
            assertThatThrownBy(() -> decisions.decide(CALLER,CALLER,plan,rig.record(),NONE)).hasStackTraceContaining(expected);
            assertThat(count(c,"repository_recovery_limit_decisions")).isZero();
            assertThat(count(c,"repository_operation_rejection")).isZero();
            assertThat(count(c,"repository_operation_success")).isEqualTo(successes);
            long targetSuccesses=c.tx().readOnly(em -> ((Number)em.createNativeQuery(
                    "SELECT count(*) FROM repository_operation_success WHERE operation_id=:id")
                    .setParameter("id",rig.record().key().operationId()).getSingleResult()).longValue());
            assertThat(targetSuccesses).isZero();
            assertThat(count(c,"repository_successor_executions")).isZero();
            assertThat(count(c,"repository_historical_activations")).isZero();
            assertThat(count(c,"repository_preparation_pin_batches")).isEqualTo(16);
            assertThat(count(c,"repository_preparation_capture_drains")).isZero();
            assertThat(leases(c,rig)).containsExactly(before);
            assertThat(rig.budget().reservedBytes()).isZero();
        }
    }
}
