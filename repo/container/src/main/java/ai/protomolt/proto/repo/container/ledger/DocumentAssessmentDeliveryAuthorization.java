package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.DocumentPublicationCommand;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Objects;

/** Current delivery gate only; callers retain their Use across provider I/O and this short transaction. */
final class DocumentAssessmentDeliveryAuthorization {
    private DocumentAssessmentDeliveryAuthorization() {}

    static void check(Tx tx, RepositoryCaller caller, RepositoryOperationLedger.Owner owner,
            DocumentPublicationCommand command, DocumentAssessmentReadProtection<DocumentAssessmentReadPlan> captured,
            RepositoryReadControl control) {
        Objects.requireNonNull(control).check();
        DocumentAdmissionAuthorization.requireCaller(caller, owner, command.intent().getAccountId());
        var stage = captured.plan().stage();
        tx.inTransaction(em -> {
            em.createNativeQuery("SELECT require_active_repository_reader(:reader)")
                    .setParameter("reader", captured.reader()).getSingleResult();
            // Delivery performs no guarded DML. Shared ownership permits concurrent
            // deliveries; capture still requires the separate transaction write fence.
            var owners = em.createNativeQuery("""
                    SELECT owner_token FROM repository_operation_owners
                    WHERE account_id=:account AND principal=:principal AND operation_id=:operation
                        AND owner_generation=:generation AND owner_token=:token FOR SHARE
                    """).setParameter("account", owner.key().account()).setParameter("principal", owner.key().principal())
                    .setParameter("operation", owner.key().operationId()).setParameter("generation", owner.generation())
                    .setParameter("token", owner.token()).getResultList();
            if (owners.size() != 1) throw new RepositoryOperationLedger.OwnerFencedException();
            requireLiveReadOwner(em, owner); // Fresh statement after a possible lock wait.
            RepositoryOperationLedger.requireCommand(em, owner.key(), command);
            DocumentAdmissionAuthorization.authorizeRejection(em, caller, command);
            var rows = em.createNativeQuery("""
                    SELECT o.assessment_id FROM document_assessment_owners o
                    WHERE o.assessment_id=:assessment AND o.account_id=:account AND o.principal=:principal
                        AND o.operation_id=:operation AND o.owner_generation=:generation
                        AND o.command_codec=:codec AND o.command_version=:version
                        AND encode(o.command_sha256,'hex')=:command AND encode(o.manifest_sha256,'hex')=:manifest
                        AND o.sealed AND o.release_xid IS NULL AND o.retain_until=:deadline
                        AND o.retain_until>clock_timestamp()
                    FOR SHARE OF o
                    """).setParameter("assessment", captured.assessment()).setParameter("account", owner.key().account())
                    .setParameter("principal", owner.key().principal()).setParameter("operation", owner.key().operationId())
                    .setParameter("generation", owner.generation()).setParameter("codec", DocumentPublicationCommand.CODEC)
                    .setParameter("version", DocumentPublicationCommand.ENCODING_VERSION).setParameter("command", command.sha256())
                    .setParameter("manifest", stage.manifestSha256())
                    .setParameter("deadline", OffsetDateTime.ofInstant(stage.retainUntil(), ZoneOffset.UTC)).getResultList();
            if (rows.size() != 1) throw new IllegalStateException("Assessment delivery requires a live exact reader session and stage");
            // Keep owner-before-session ordering explicit; a joined row-lock order is not guaranteed.
            var sessions = em.createNativeQuery("""
                    SELECT session_id FROM document_assessment_read_sessions
                    WHERE session_id=:session AND reader_incarnation=:reader AND assessment_id=:assessment
                        AND release_xid IS NULL FOR SHARE
                    """).setParameter("session", captured.session()).setParameter("reader", captured.reader())
                    .setParameter("assessment", captured.assessment()).getResultList();
            if (sessions.size() != 1) throw new IllegalStateException("Assessment delivery requires a live exact reader session and stage");
            requireLiveReadOwner(em, owner);
            if (!Boolean.TRUE.equals(em.createNativeQuery("""
                    SELECT retain_until>clock_timestamp() FROM document_assessment_owners WHERE assessment_id=:assessment
                    """).setParameter("assessment", captured.assessment()).getSingleResult()))
                throw new IllegalStateException("Assessment delivery deadline expired");
            control.check();
        });
        control.check();
    }

    private static void requireLiveReadOwner(jakarta.persistence.EntityManager em, RepositoryOperationLedger.Owner owner) {
        Object[] state = (Object[]) em.createNativeQuery("""
                SELECT EXISTS(SELECT 1 FROM repository_operation_owners
                    WHERE account_id=:account AND principal=:principal AND operation_id=:operation
                        AND owner_generation=:generation AND owner_token=:token AND lease_until>clock_timestamp()),
                    EXISTS(SELECT 1 FROM repository_operation_success
                        WHERE account_id=:account AND principal=:principal AND operation_id=:operation)
                    OR EXISTS(SELECT 1 FROM repository_operation_rejection
                        WHERE account_id=:account AND principal=:principal AND operation_id=:operation)
                """).setParameter("account", owner.key().account()).setParameter("principal", owner.key().principal())
                .setParameter("operation", owner.key().operationId()).setParameter("generation", owner.generation())
                .setParameter("token", owner.token()).getSingleResult();
        if (!Boolean.TRUE.equals(state[0])) throw new RepositoryOperationLedger.OwnerFencedException();
        if (Boolean.TRUE.equals(state[1])) throw new RepositoryOperationLedger.TerminalOperationException();
    }
}
