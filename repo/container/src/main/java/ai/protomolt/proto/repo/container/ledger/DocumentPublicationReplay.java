package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.container.blob.DocumentIds;
import ai.protomolt.proto.repo.spi.DocumentPublicationCommand;
import ai.protomolt.proto.repo.spi.DocumentPublicationResultCodec;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.v1.DocumentPublicationResult;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/** Internal authorized observation of an immutable outcome; never creates or resumes work. */
final class DocumentPublicationReplay {
    enum State { NOT_OBSERVED, PENDING, COMMITTED }
    record Observation(State state, Optional<DocumentPublicationResult> result) {
        Observation {
            Objects.requireNonNull(state); Objects.requireNonNull(result);
            if ((state == State.COMMITTED) != result.isPresent())
                throw new IllegalArgumentException("Only committed observations carry a result");
        }
    }

    private final Tx tx;
    DocumentPublicationReplay(Tx tx) { this.tx = Objects.requireNonNull(tx); }

    /**
     * NOT_OBSERVED and PENDING are observations, never proof of rollback or permission
     * to start a new operation. Only admission/recovery can grant executable ownership.
     * Admission state and command conflicts are private to the authenticated operation
     * principal and account; they contain no document result. Pending creation may have
     * no destination to authorize yet. Committed results require current document access.
     * No lease renewal, provider I/O, registry lookup or outbox mutation occurs here.
     */
    Observation observe(RepositoryCaller caller, DocumentPublicationCommand command) {
        Objects.requireNonNull(command);
        if (caller == null) throw new RepositoryException(RepositoryException.Code.UNAUTHENTICATED,
                "Authenticated repository caller is required");
        var key = new RepositoryOperationLedger.Key(command.intent().getAccountId(), caller.principalName(), command.operationId());
        DocumentAdmissionAuthorization.requireCaller(caller, key, key.account());
        return tx.inTransaction(em -> {
            // Wait for a committing owner before inspecting success in a fresh READ COMMITTED
            // statement. Lease expiry is irrelevant to replay; this grants no write fence.
            var owner = bind(em.createNativeQuery("""
                    SELECT owner_generation,require_repository_read_committed() FROM repository_operation_owners
                    WHERE account_id=:account AND principal=:principal AND operation_id=:operation FOR SHARE
                    """), key).getResultList();
            if (owner.isEmpty()) return new Observation(State.NOT_OBSERVED, Optional.empty());
            RepositoryOperationLedger.requireCommand(em, key, command);
            var outcomes = bind(em.createNativeQuery("""
                    SELECT owner_generation,result_codec,result_version,result_bytes,encode(result_sha256,'hex')
                    FROM repository_operation_success
                    WHERE account_id=:account AND principal=:principal AND operation_id=:operation
                    """), key).getResultList();
            if (outcomes.isEmpty()) return new Observation(State.PENDING, Optional.empty());
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
        });
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
