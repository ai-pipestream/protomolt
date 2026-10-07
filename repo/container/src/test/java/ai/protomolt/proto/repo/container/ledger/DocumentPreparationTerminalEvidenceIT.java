package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.context;
import static ai.protomolt.proto.repo.container.ledger.DocumentCaptureAdmissionClosureIT.*;
import static ai.protomolt.proto.repo.container.ledger.RepositoryHistoricalSuccessorActivationIT.*;
import static org.assertj.core.api.Assertions.*;

@Testcontainers
class DocumentPreparationTerminalEvidenceIT {
    @Container static final PostgreSQLContainer POSTGRES=new PostgreSQLContainer("postgres:18-alpine");
    private static final RepositoryCaller CALLER=new RepositoryCaller("principal",true);
    private static final RepositoryReadControl NONE=RepositoryReadControl.NONE;
    private static final Duration LEASE=Duration.ofMinutes(5);

    @Test void pendingIsNotTerminalButExactInitialAbandonmentQualifies() throws Exception {
        try (var c=context(POSTGRES); var rig=historicalInitial(c,LEASE);
             var memory=rig.budget().reserve(32L*1024*1024)) {
            var check=new DocumentPreparationTerminalEvidence(rig.record());
            var before=c.tx().readOnly(em -> em.createNativeQuery("SELECT lease_until FROM repository_execution_claims WHERE operation_id=:id")
                    .setParameter("id",rig.record().key().operationId()).getSingleResult());
            assertThatThrownBy(() -> c.tx().inTransaction(em -> { return check.lockAndRequire(em,CALLER,NONE); }))
                    .isInstanceOfSatisfying(RepositoryException.class,e -> assertThat(e.code()).isEqualTo(RepositoryException.Code.FAILED_PRECONDITION));
            var scoped=new RepositoryCaller("principal",false,Set.of("account"),Set.of());
            assertThatThrownBy(() -> c.tx().inTransaction(em -> { return check.lockAndRequire(em,scoped,NONE); }))
                    .hasMessageContaining("private process authority");
            DocumentPublicationAbandonment.abandon(c.tx(),rig.budget(),CALLER,rig.claim(),rig.record(),NONE);
            var evidence=c.tx().inTransaction(em -> { return check.lockAndRequire(em,CALLER,NONE); });
            assertThat(evidence).isEqualTo(new DocumentPreparationTerminalEvidence.Abandonment(rig.claim().token(),rig.record().seeds().ownerNonce()));
            var changed=new DocumentPublicationPreparationRecord(rig.record().key(),rig.record().command(),rig.record().seeds(),
                    rig.record().placements(),Duration.ofMinutes(4),0);
            var wrong=new DocumentPreparationTerminalEvidence(changed);
            assertThatThrownBy(() -> c.tx().inTransaction(em -> { return wrong.lockAndRequire(em,CALLER,NONE); }))
                    .isInstanceOfSatisfying(RepositoryException.class,e -> assertThat(e.code()).isEqualTo(RepositoryException.Code.DATA_LOSS));
            assertThat(count(c,"repository_preparation_history_roots")).isEqualTo(1);
            assertThat(count(c,"repository_preparation_capture_drains")).isZero();
            var after=c.tx().readOnly(em -> em.createNativeQuery("SELECT lease_until FROM repository_execution_claims WHERE operation_id=:id")
                    .setParameter("id",rig.record().key().operationId()).getSingleResult());
            assertThat(after).isEqualTo(before);
        }
    }

    @Test void canonicalRejectionQualifiesWithoutGrantingRootRelease() throws Exception {
        try (var c=context(POSTGRES); var rig=historicalInitial(c,LEASE); var work=rig.sources().work();
             var memory=rig.budget().reserve(32L*1024*1024)) {
            var command=rig.record().command();
            var modes=DocumentPublicationModesJournal.encode(command,
                    Map.of(command.intent().getMembers(0).getMemberId(),DocumentPublicationCandidate.Mode.TYPED));
            var admission=RepositoryOperationLedger.prepareHistoricalAdmission(rig.record().key(),command,rig.record().seeds().ownerNonce(),LEASE,work);
            var owner=c.tx().inTransaction(em -> {
                RepositoryExecutionClaimLedger.lockLive(em,rig.claim());
                DocumentPublicationModesJournal.insert(em,rig.claim(),rig.record(),modes);
                return admission.apply(em,rig.claim()).owner().orElseThrow();
            });
            var receipt=new DocumentPublicationRejections(c.tx()).cancel(CALLER,owner,command,NONE).rejection().orElseThrow();
            var before=leases(c,rig);
            var check=new DocumentPreparationTerminalEvidence(rig.record());
            var evidence=c.tx().inTransaction(em -> { return check.lockAndRequire(em,CALLER,NONE); });
            assertThat(evidence).isInstanceOfSatisfying(DocumentPreparationTerminalEvidence.Outcome.class,e -> {
                assertThat(e.kind()).isEqualTo("REJECTION");
                assertThat(e.generation()).isEqualTo(owner.generation());
                assertThat(e.resultSha256()).isEqualTo(ai.protomolt.proto.repo.spi.DocumentPublicationRejectionCodec
                        .encode(command,receipt,"principal",owner.generation()).sha256());
            });
            var repeated=c.tx().inTransaction(em -> { return check.lockAndRequire(em,CALLER,NONE); });
            assertThat(repeated).isEqualTo(evidence);
            assertThatThrownBy(() -> c.tx().inTransaction((java.util.function.Consumer<jakarta.persistence.EntityManager>)em -> {
                em.createNativeQuery("ALTER TABLE repository_operation_rejection DISABLE TRIGGER repository_operation_rejection_guard").executeUpdate();
                em.createNativeQuery("UPDATE repository_operation_rejection SET recorded_at_epoch_micros=recorded_at_epoch_micros+1 WHERE operation_id=:id")
                        .setParameter("id",command.operationId()).executeUpdate();
                assertThatThrownBy(() -> check.lockAndRequire(em,CALLER,NONE))
                        .isInstanceOfSatisfying(RepositoryException.class,e -> assertThat(e.code()).isEqualTo(RepositoryException.Code.DATA_LOSS));
                throw new CorruptionRollback();
            })).isInstanceOf(CorruptionRollback.class);
            assertThat(count(c,"repository_preparation_history_roots")).isEqualTo(1);
            assertThat(count(c,"repository_preparation_capture_drains")).isZero();
            assertThat(leases(c,rig)).containsExactly(before);
        }
    }
    private static final class CorruptionRollback extends RuntimeException {}
}
