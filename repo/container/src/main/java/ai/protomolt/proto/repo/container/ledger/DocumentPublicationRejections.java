package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.DocumentPublicationCommand;
import ai.protomolt.proto.repo.spi.DocumentPublicationRejectionCodec;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import ai.protomolt.proto.repo.v1.DocumentPublicationDisposition;
import ai.protomolt.proto.repo.v1.DocumentPublicationRejection;
import ai.protomolt.proto.repo.v1.DocumentPublicationRejectionReason;
import java.util.HexFormat;
import java.util.Objects;
import java.util.Optional;

/** Explicit terminal decisions in fresh transactions; never an exception classifier. */
final class DocumentPublicationRejections {
    private enum Decision { CANCEL, PRECONDITIONS }
    private final Tx tx;
    DocumentPublicationRejections(Tx tx) { this.tx = Objects.requireNonNull(tx); }

    /**
     * The host explicitly requests operation cancellation after its failed mutation
     * transaction has rolled back. A cancelled transport control is not that request.
     * Existing success wins; an already recorded decision replays exactly. No provider
     * cleanup or semantic rejection is inferred here.
     */
    DocumentPublicationReplay.Observation cancel(RepositoryCaller caller, RepositoryOperationLedger.Owner owner,
            DocumentPublicationCommand command, RepositoryReadControl control) {
        return decide(caller, owner, command, control, Decision.CANCEL);
    }

    /** Recheck all command conditions under current locks; a prior exception is not decision evidence. */
    DocumentPublicationReplay.Observation rejectRevisionPreconditions(RepositoryCaller caller, RepositoryOperationLedger.Owner owner,
            DocumentPublicationCommand command, RepositoryReadControl control) {
        return decide(caller, owner, command, control, Decision.PRECONDITIONS);
    }

    private DocumentPublicationReplay.Observation decide(RepositoryCaller caller, RepositoryOperationLedger.Owner owner,
            DocumentPublicationCommand command, RepositoryReadControl control, Decision decision) {
        Objects.requireNonNull(owner); Objects.requireNonNull(command); Objects.requireNonNull(control).check();
        var key = owner.key();
        DocumentAdmissionAuthorization.requireCaller(caller, key, command.intent().getAccountId());
        if (!key.account().equals(command.intent().getAccountId()) || !key.operationId().equals(command.operationId()))
            throw new IllegalArgumentException("Decision owner differs from command scope");
        return tx.inTransaction(em -> {
            var rows = em.createNativeQuery("""
                    SELECT owner_generation FROM repository_operation_owners
                    WHERE account_id=:account AND principal=:principal AND operation_id=:operation FOR UPDATE
                    """).setParameter("account", key.account()).setParameter("principal", key.principal())
                    .setParameter("operation", key.operationId()).getResultList();
            if (rows.isEmpty()) throw new RepositoryOperationLedger.OwnerFencedException();
            var observed = DocumentPublicationReplay.observe(em, caller, command, key);
            control.check();
            if (observed.state() == DocumentPublicationReplay.State.COMMITTED
                    || observed.state() == DocumentPublicationReplay.State.TERMINATED) return observed;
            RepositoryOperationLedger.lockLiveOwner(em, owner);
            RepositoryOperationLedger.fenceLiveOwner(em, owner);
            if (decision == Decision.PRECONDITIONS) {
                boolean matches = DocumentAdmissionAuthorization.revisionPreconditionsMatch(em, caller, command);
                control.check();
                if (matches) return new DocumentPublicationReplay.Observation(DocumentPublicationReplay.State.PENDING, Optional.empty());
            } else {
                DocumentAdmissionAuthorization.authorizeRejection(em, caller, command);
            }
            control.check();
            long recordedAt = ((Number) em.createNativeQuery(
                    "SELECT floor(extract(epoch FROM clock_timestamp())*1000000)").getSingleResult()).longValue();
            var receipt = DocumentPublicationRejection.newBuilder().setOperationId(key.operationId().toString())
                    .setAccountId(key.account()).setPrincipal(key.principal()).setOwnerGeneration(owner.generation())
                    .setCommandCodec(DocumentPublicationCommand.CODEC).setCommandEncodingVersion(DocumentPublicationCommand.ENCODING_VERSION)
                    .setCommandSha256(command.sha256()).setRecordedAtEpochMicros(recordedAt)
                    .setDisposition(decision == Decision.CANCEL ? DocumentPublicationDisposition.DOCUMENT_PUBLICATION_DISPOSITION_ABORTED
                            : DocumentPublicationDisposition.DOCUMENT_PUBLICATION_DISPOSITION_REJECTED)
                    .setReason(decision == Decision.CANCEL ? DocumentPublicationRejectionReason.DOCUMENT_PUBLICATION_REJECTION_REASON_EXPLICIT_CANCELLATION
                            : DocumentPublicationRejectionReason.DOCUMENT_PUBLICATION_REJECTION_REASON_PRECONDITION_NOT_MET).build();
            var encoded = DocumentPublicationRejectionCodec.encode(command, receipt, key.principal(), owner.generation());
            em.createNativeQuery("""
                    INSERT INTO repository_operation_rejection(account_id,principal,operation_id,owner_generation,
                        command_codec,command_version,command_sha256,result_codec,result_version,result_bytes,result_sha256,
                        recorded_at_epoch_micros,disposition,reason)
                    VALUES(:account,:principal,:operation,:generation,:commandCodec,:commandVersion,:commandSha,
                        :resultCodec,:resultVersion,:bytes,:sha,:recordedAt,:disposition,:reason)
                    """).setParameter("account", key.account()).setParameter("principal", key.principal())
                    .setParameter("operation", key.operationId()).setParameter("generation", owner.generation())
                    .setParameter("commandCodec", DocumentPublicationCommand.CODEC).setParameter("commandVersion", DocumentPublicationCommand.ENCODING_VERSION)
                    .setParameter("commandSha", HexFormat.of().parseHex(command.sha256()))
                    .setParameter("resultCodec", DocumentPublicationRejectionCodec.CODEC).setParameter("resultVersion", DocumentPublicationRejectionCodec.VERSION)
                    .setParameter("bytes", encoded.bytes().toByteArray()).setParameter("sha", HexFormat.of().parseHex(encoded.sha256()))
                    .setParameter("recordedAt", recordedAt).setParameter("disposition", receipt.getDispositionValue())
                    .setParameter("reason", receipt.getReasonValue()).executeUpdate();
            // A returned receipt is committed. A failed SQL acknowledgement must be reconciled;
            // a control check after commit must never turn this into a rollback claim.
            return new DocumentPublicationReplay.Observation(DocumentPublicationReplay.State.TERMINATED,
                    Optional.empty(), Optional.of(receipt));
        });
    }
}
