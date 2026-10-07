package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.context;
import static ai.protomolt.proto.repo.container.ledger.DocumentCaptureAdmissionClosureIT.*;
import static ai.protomolt.proto.repo.container.ledger.RepositoryHistoricalSuccessorActivationIT.*;
import static org.assertj.core.api.Assertions.*;

/** Corrupt an older edge while the newest installed plan still confirms exactly. */
@Testcontainers
class RepositoryHistoricalLimitAncestryIT {
    @Container static final PostgreSQLContainer POSTGRES=new PostgreSQLContainer("postgres:18-alpine");
    private static final RepositoryCaller CALLER=new RepositoryCaller("principal",true);
    private static final RepositoryReadControl NONE=RepositoryReadControl.NONE;

    @ParameterizedTest @ValueSource(strings={"predecessor_preparation_sha256","command_sha256"})
    void olderEdgeCorruptionCannotBecomeCaptureExhaustion(String column) throws Exception {
        try (var c=context(POSTGRES); var rig=historicalInitial(c,Duration.ofSeconds(10))) {
            for (int i=0;i<15;i++) append(c,rig);
            var first=installedHistoricalSuccessor(c,rig,Duration.ofSeconds(1));
            c.tx().inTransaction(em -> {
                em.createNativeQuery("""
                        SELECT pg_sleep(GREATEST(0,EXTRACT(EPOCH FROM
                          (GREATEST(c.lease_until,o.lease_until)-clock_timestamp())))+0.05)
                        FROM repository_execution_claims c JOIN repository_operation_owners o USING(account_id,principal,operation_id)
                        WHERE c.operation_id=:id
                        """).setParameter("id",rig.record().key().operationId()).getSingleResult();
            });
            var candidate=new RepositoryCoordinatorRecoveryDiscovery(c.tx(),new SqlTimeouts(Duration.ofSeconds(2),Duration.ofSeconds(10)))
                    .inspect(CALLER,rig.record().key(),rig.record().command().sha256(),NONE).unactivated().orElseThrow();
            var lease=Duration.ofMinutes(5);
            var reservation=new RepositoryCoordinatorReservation.SupersededUnactivated(candidate.predecessor(),UUID.randomUUID(),
                    UUID.randomUUID(),lease,candidate.owner(),candidate.preparationSha256(),candidate.installation());
            RepositoryCoordinatorSupersession.reserve(c.tx(),CALLER,reservation,NONE);
            var plan=RepositorySuccessorInstall.prepare(reservation,first.next(),lease,first.modes());
            RepositorySuccessorInstall.install(c.tx(),rig.budget(),CALLER,plan,NONE);
            var before=leases(c,rig);
            c.tx().inTransaction(em -> {
                em.createNativeQuery("ALTER TABLE repository_successor_installs DISABLE TRIGGER repository_successor_install_guard").executeUpdate();
                assertThat(em.createNativeQuery("UPDATE repository_successor_installs SET "+column+
                        "=sha256('corrupt'::bytea) WHERE operation_id=:id AND predecessor_generation=:generation")
                        .setParameter("id",rig.record().key().operationId())
                        .setParameter("generation",first.next().predecessorGeneration()).executeUpdate()).isEqualTo(1);
                em.createNativeQuery("ALTER TABLE repository_successor_installs ENABLE TRIGGER repository_successor_install_guard").executeUpdate();
            });
            var previousSha=DocumentPublicationPreparationJournal.digest(DocumentPublicationPreparationCodec.encode(plan.previous()));
            var nextSha=DocumentPublicationPreparationJournal.digest(DocumentPublicationPreparationCodec.encode(plan.next()));
            assertThat(RepositorySuccessorInstall.confirm(c.tx(),CALLER,plan,previousSha,nextSha,
                    RepositorySuccessorInstall.encodeModes(plan),NONE)).isTrue();
            assertThatThrownBy(() -> new RepositoryHistoricalLimitDecisions(c.tx(),rig.budget())
                    .decide(CALLER,CALLER,plan,rig.record(),NONE))
                    .isInstanceOfSatisfying(RepositoryException.class,e -> {
                        assertThat(e.code()).isEqualTo(RepositoryException.Code.FAILED_PRECONDITION);
                        assertThat(e).hasMessageContaining("ancestry is incomplete or differs");
                    });
            assertThat(count(c,"repository_recovery_limit_decisions")).isZero();
            assertThat(count(c,"repository_operation_rejection")).isZero();
            assertThat(count(c,"repository_successor_executions")).isZero();
            assertThat(count(c,"repository_historical_activations")).isZero();
            assertThat(count(c,"repository_preparation_pin_batches")).isEqualTo(16);
            assertThat(count(c,"repository_preparation_capture_drains")).isZero();
            assertThat(leases(c,rig)).containsExactly(before);
            assertThat(rig.budget().reservedBytes()).isZero();
        }
    }
}
