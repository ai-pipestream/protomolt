package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.container.blob.DocumentIds;
import ai.protomolt.proto.repo.spi.DocumentPublicationCommand;
import ai.protomolt.proto.repo.spi.DocumentPublicationResultCodec;
import ai.protomolt.proto.repo.spi.DocumentPublicationRejectionCodec;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import ai.protomolt.proto.repo.v1.DocumentPublicationResult;
import ai.protomolt.proto.repo.v1.DocumentPublicationRejection;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;
import java.util.Objects;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/** Internal authorized observation of an immutable outcome; never creates or resumes work. */
final class DocumentPublicationReplay {
    enum State { NOT_OBSERVED, PENDING, COMMITTED, TERMINATED, ABANDONED }
    record Observation(State state, Optional<DocumentPublicationResult> result, Optional<DocumentPublicationRejection> rejection) {
        Observation(State state, Optional<DocumentPublicationResult> result) { this(state, result, Optional.empty()); }
        Observation {
            Objects.requireNonNull(state); Objects.requireNonNull(result); Objects.requireNonNull(rejection);
            if ((state == State.COMMITTED) != result.isPresent() || (state == State.TERMINATED) != rejection.isPresent())
                throw new IllegalArgumentException("Terminal observations must carry exactly their stored outcome");
        }
        void requireNotTerminated() {
            if (state == State.ABANDONED) throw new RepositoryException(RepositoryException.Code.FAILED_PRECONDITION,
                    "Publication registration was abandoned");
            rejection.ifPresent(receipt -> { throw new Terminated(receipt); });
        }
    }

    /** Internal success-only execution surfaces preserve the authorized terminal receipt in this signal. */
    static final class Terminated extends RuntimeException {
        private final DocumentPublicationRejection receipt;
        Terminated(DocumentPublicationRejection receipt) { super("Publication has a durable terminal rejection"); this.receipt = receipt; }
        DocumentPublicationRejection receipt() { return receipt; }
    }

    private final Tx tx;
    DocumentPublicationReplay(Tx tx) { this.tx = Objects.requireNonNull(tx); }

    /**
     * NOT_OBSERVED and PENDING are observations, never proof of rollback or permission
     * to start a new operation. Only admission/recovery can grant executable ownership.
     * Admission state and command conflicts are private to the authenticated operation
     * principal and account; they contain no document result. Pending and rejected
     * outcomes require the current destination/source read set; only explicit creation
     * authority covers an absent ifAbsent destination. Successful outcomes require current
     * target access. Observation never reapplies expected revision conditions.
     * No lease renewal, provider I/O, registry lookup or outbox mutation occurs here.
     */
    Observation observe(RepositoryCaller caller, DocumentPublicationCommand command) {
        Objects.requireNonNull(command);
        if (caller == null) throw new RepositoryException(RepositoryException.Code.UNAUTHENTICATED,
                "Authenticated repository caller is required");
        var key = new RepositoryOperationLedger.Key(command.intent().getAccountId(), caller.principalName(), command.operationId());
        DocumentAdmissionAuthorization.requireCaller(caller, key, key.account());
        return tx.inTransaction(em -> { return observe(em, caller, command, key); });
    }

    /** Managed publication replay additionally binds the submitted modes to the terminal journal. */
    Observation observe(RepositoryCaller caller, DocumentPublicationCommand command,
            Map<String, DocumentPublicationCandidate.Mode> modes, RepositoryReadControl control) {
        Objects.requireNonNull(control).check();
        Objects.requireNonNull(command);
        if (caller == null) throw new RepositoryException(RepositoryException.Code.UNAUTHENTICATED,
                "Authenticated repository caller is required");
        var key = new RepositoryOperationLedger.Key(command.intent().getAccountId(), caller.principalName(), command.operationId());
        DocumentAdmissionAuthorization.requireCaller(caller, key, key.account());
        String encoded = DocumentPublicationModesJournal.encode(command, modes);
        var observed = tx.inTransaction(em -> {
            control.check();
            var result = observe(em, caller, command, key);
            if (result.state() == State.COMMITTED || result.state() == State.TERMINATED) {
                long generation = result.result().map(DocumentPublicationResult::getOwnerGeneration)
                        .orElseGet(() -> result.rejection().orElseThrow().getOwnerGeneration());
                DocumentPublicationModesJournal.requireTerminalModes(em, key, command, generation, encoded);
            }
            control.check();
            return result;
        });
        control.check();
        return observed;
    }

    static Observation observe(EntityManager em, RepositoryCaller caller, DocumentPublicationCommand command,
            RepositoryOperationLedger.Key key) {
            // Wait for a committing owner before inspecting success in a fresh READ COMMITTED
            // statement. Lease expiry is irrelevant to replay; this grants no write fence.
            var owner = bind(em.createNativeQuery("""
                    SELECT owner_generation,require_repository_read_committed() FROM repository_operation_owners
                    WHERE account_id=:account AND principal=:principal AND operation_id=:operation FOR SHARE
                    """), key).getResultList();
            if (owner.isEmpty()) {
                var abandoned = bind(em.createNativeQuery("""
                        SELECT p.command_sha256,a.owner_nonce=p.owner_nonce AND a.preparation_sha256=p.preparation_sha256,
                          CASE WHEN octet_length(p.command_bytes)<=1048576 THEN p.command_bytes END
                        FROM repository_publication_abandonments a
                        JOIN repository_publication_preparations p USING(account_id,principal,operation_id,predecessor_generation)
                        WHERE a.account_id=:account AND a.principal=:principal AND a.operation_id=:operation
                        """), key).getResultList();
                if (abandoned.isEmpty()) return new Observation(State.NOT_OBSERVED, Optional.empty());
                var row = (Object[]) abandoned.getFirst();
                if (!Boolean.TRUE.equals(row[1])) throw invalidBinding();
                // Authorize the stored read set, not an alternative command whose
                // documents happen to be accessible to the same principal.
                DocumentPublicationCommand stored;
                try {
                    if (!(row[2] instanceof byte[] bytes)) throw invalidBinding();
                    stored = new DocumentPublicationCommand(ai.protomolt.proto.repo.v1.DocumentPublicationIntent.parseFrom(bytes)
                            .toBuilder().setOperationId(key.operationId().toString()).build());
                    if (!stored.canonical().equals(ByteString.copyFrom(bytes))
                            || !stored.sha256().equals(java.util.HexFormat.of().formatHex((byte[]) row[0]))) throw invalidBinding();
                } catch (InvalidProtocolBufferException | IllegalArgumentException malformed) { throw invalidBinding(); }
                DocumentAdmissionAuthorization.authorizeRejection(em, caller, stored);
                if (!command.sha256().equals(java.util.HexFormat.of().formatHex((byte[]) row[0])))
                    throw new RepositoryOperationLedger.CommandConflictException();
                return new Observation(State.ABANDONED, Optional.empty());
            }
            RepositoryOperationLedger.requireCommand(em, key, command);
            var outcomes = bind(em.createNativeQuery("""
                    SELECT owner_generation,result_codec,result_version,result_bytes,encode(result_sha256,'hex')
                    FROM repository_operation_success
                    WHERE account_id=:account AND principal=:principal AND operation_id=:operation
                    """), key).getResultList();
            var rejections = bind(em.createNativeQuery("""
                    SELECT owner_generation,result_codec,result_version,result_bytes,encode(result_sha256,'hex'),
                        recorded_at_epoch_micros,disposition,reason,command_codec,command_version,encode(command_sha256,'hex'),
                        assessment_id,manifest_codec,manifest_version,encode(manifest_sha256,'hex'),retain_until_epoch_micros
                    FROM repository_operation_rejection
                    WHERE account_id=:account AND principal=:principal AND operation_id=:operation
                    """), key).getResultList();
            if (!outcomes.isEmpty() && !rejections.isEmpty()) throw new RepositoryException(RepositoryException.Code.DATA_LOSS,
                    "Operation has conflicting terminal outcomes");
            if (!rejections.isEmpty()) {
                DocumentAdmissionAuthorization.authorizeRejection(em, caller, command);
                var row = (Object[]) rejections.getFirst();
                try {
                    long generation = ((Number) row[0]).longValue();
                    var receipt = DocumentPublicationRejectionCodec.decode(command, key.principal(), generation,
                            (String) row[1], ((Number) row[2]).intValue(), ByteString.copyFrom((byte[]) row[3]), (String) row[4]);
                    long currentGeneration = ((Number) ((Object[]) owner.getFirst())[0]).longValue();
                    if (generation != currentGeneration) throw new IllegalArgumentException("Rejection owner differs from receipt");
                    requireRejectionProjection(receipt, row);
                    return new Observation(State.TERMINATED, Optional.empty(), Optional.of(receipt));
                } catch (IllegalArgumentException | InvalidProtocolBufferException failure) {
                    throw new RepositoryException(RepositoryException.Code.DATA_LOSS, "Stored publication rejection is invalid", failure);
                }
            }
            if (outcomes.isEmpty()) {
                DocumentAdmissionAuthorization.authorizePending(em, caller, command);
                return new Observation(State.PENDING, Optional.empty());
            }
            DocumentAdmissionAuthorization.authorizeReplay(em, caller, command);
            var row = (Object[]) outcomes.getFirst();
            long generation = ((Number) row[0]).longValue();
            final DocumentPublicationResult result;
            try {
                result = DocumentPublicationResultCodec.decode(command, key.principal(), generation,
                        (String) row[1], ((Number) row[2]).intValue(), ByteString.copyFrom((byte[]) row[3]), (String) row[4]);
            } catch (IllegalArgumentException | InvalidProtocolBufferException failure) {
                throw new RepositoryException(RepositoryException.Code.DATA_LOSS, "Stored publication result is invalid", failure);
            }
            requireRevisions(em, key, result, generation);
            return new Observation(State.COMMITTED, Optional.of(result));
    }

    /** Shared with private cleanup; this validates stored projection, not caller authorization. */
    static void requireRejectionProjection(DocumentPublicationRejection receipt, Object[] row) {
        if (receipt.getOwnerGeneration() != ((Number)row[0]).longValue()
                || receipt.getRecordedAtEpochMicros() != ((Number)row[5]).longValue()
                || receipt.getDispositionValue() != ((Number)row[6]).intValue()
                || receipt.getReasonValue() != ((Number)row[7]).intValue()
                || !receipt.getCommandCodec().equals(row[8])
                || receipt.getCommandEncodingVersion() != ((Number)row[9]).intValue()
                || !receipt.getCommandSha256().equals(row[10]))
            throw new IllegalArgumentException("Rejection header differs from receipt");
        requireAssessmentBinding(receipt,row);
    }

    private static void requireAssessmentBinding(DocumentPublicationRejection receipt, Object[] row) {
        if (!receipt.hasAssessment()) {
            for (int i = 11; i <= 15; i++)
                if (row[i] != null) throw new IllegalArgumentException("Unexpected stored assessment binding");
            return;
        }
        var binding = receipt.getAssessment();
        if (!UUID.fromString(binding.getAssessmentId()).equals(row[11]) || !binding.getManifestCodec().equals(row[12])
                || !(row[13] instanceof Number version) || binding.getManifestEncodingVersion() != version.intValue()
                || !binding.getManifestSha256().equals(row[14]) || !(row[15] instanceof Number deadline)
                || binding.getRetainUntilEpochMicros() != deadline.longValue())
            throw new IllegalArgumentException("Stored assessment binding differs from rejection receipt");
    }

    private static void requireRevisions(EntityManager em, RepositoryOperationLedger.Key key,
            DocumentPublicationResult result, long generation) {
        var rows = bind(em.createNativeQuery("""
                SELECT c.member_id,c.member_ordinal,c.node_id,c.revision_id,c.publication_revision,c.owner_generation,
                    r.projection_sealed,r.native_binding
                FROM document_revision_commits c JOIN document_revision_publications r USING(revision_id)
                WHERE c.account_id=:account AND c.principal=:principal AND c.operation_id=:operation
                ORDER BY c.member_ordinal LIMIT 65
                """), key).getResultList();
        if (rows.size() != result.getMembersCount()) throw invalidBinding();
        for (int i = 0; i < rows.size(); i++) {
            var row = (Object[]) rows.get(i);
            var member = result.getMembers(i);
            if (!member.getMemberId().equals(row[0]) || ((Number) row[1]).intValue() != i
                    || !DocumentIds.nodeId(member.getAddress()).equals(row[2])
                    || !UUID.fromString(member.getRevisionId()).equals(row[3])
                    || member.getMutationRevision() != ((Number) row[4]).longValue()
                    || generation != ((Number) row[5]).longValue()
                    || !Boolean.TRUE.equals(row[6]) || !row[3].equals(row[7])) throw invalidBinding();
        }
    }

    private static RepositoryException invalidBinding() {
        return new RepositoryException(RepositoryException.Code.DATA_LOSS,
                "Stored publication result differs from retained revisions");
    }

    private static Query bind(Query query, RepositoryOperationLedger.Key key) {
        return query.setParameter("account", key.account()).setParameter("principal", key.principal())
                .setParameter("operation", key.operationId());
    }
}
