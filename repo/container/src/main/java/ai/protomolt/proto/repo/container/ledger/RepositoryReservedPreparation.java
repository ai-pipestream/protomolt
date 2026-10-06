package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.*;
import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;
import java.util.*;

/** Non-executing preparation read for an exact reserved successor, including after V91 local drain. */
final class RepositoryReservedPreparation {
    private final Tx tx;
    private final PayloadBudget budget;
    RepositoryReservedPreparation(Tx tx, PayloadBudget budget, SqlTimeouts timeouts) {
        this.tx = Objects.requireNonNull(tx).withTimeouts(Objects.requireNonNull(timeouts));
        this.budget = Objects.requireNonNull(budget);
    }

    DocumentPublicationPreparationJournal.Loaded load(RepositoryCaller authority, RepositoryCaller executionCaller,
            RepositoryCoordinatorReservation.Proposal reservation, RepositoryCoordinatorReservation.OwnerIdentity owner,
            RepositoryReadControl control) {
        RepositoryCoordinatorReservation.require(authority,reservation,control); Objects.requireNonNull(owner);
        var key = reservation.predecessor().key();
        DocumentAdmissionAuthorization.requireCaller(executionCaller,key,key.account());
        if (RepositoryCoordinatorReservation.owner(reservation).filter(expected -> !expected.equals(owner)).isPresent())
            throw new IllegalArgumentException("Reserved preparation owner differs from reservation");
        PayloadBudget.Lease[] lease = {null}; boolean transferred = false;
        try {
            var row = tx.inTransaction(em -> {
                requireState(em,reservation,owner); control.check();
                int size = ((Number) single(preparation(em,"octet_length(preparation_bytes)",key,owner))).intValue();
                if (size < 1 || size > DocumentPublicationPreparationCodec.MAX_BYTES)
                    throw new RepositoryException(RepositoryException.Code.DATA_LOSS,"Reserved preparation exceeds byte bound");
                lease[0] = budget.reserve(size); control.check();
                return (Object[]) single(preparation(em,"preparation_bytes,preparation_sha256,owner_nonce,command_sha256",key,owner));
            });
            control.check();
            if (reservation instanceof RepositoryCoordinatorReservation.SupersededUnactivated superseded
                    && !superseded.preparationSha256().equals(HexFormat.of().formatHex((byte[]) row[1])))
                throw new RepositoryException(RepositoryException.Code.DATA_LOSS,"Preparation differs from supersession");
            // Decode with no SQL locks held. The immutable hash is checked again before delivery.
            var record = DocumentPublicationPreparationJournal.decode(row,lease[0].bytes(),key,
                    reservation.predecessor().commandSha256(),owner.generation()-1);
            control.check();
            tx.inTransaction(em -> {
                requireState(em,reservation,owner); control.check();
                var digest = (byte[]) single(preparation(em,"preparation_sha256",key,owner));
                if (!Arrays.equals(digest,(byte[]) row[1]))
                    throw new RepositoryException(RepositoryException.Code.DATA_LOSS,"Reserved preparation changed during decode");
                DocumentAdmissionAuthorization.authorizeRejection(em,executionCaller,record.command());
                control.check(); return null;
            });
            control.check();
            var loaded = new DocumentPublicationPreparationJournal.Loaded(record,lease[0]);
            transferred = true; return loaded;
        } finally { if (!transferred && lease[0] != null) lease[0].close(); }
    }

    /** Claim then owner locks protect observation only; deliberately no write-fence stamp or lease renewal. */
    private static void requireState(EntityManager em, RepositoryCoordinatorReservation.Proposal reservation,
                                     RepositoryCoordinatorReservation.OwnerIdentity owner) {
        var p = reservation.predecessor(); var key = p.key();
        var claims = scope(em.createNativeQuery("""
                SELECT claim_epoch,claim_token,command_sha256 FROM repository_execution_claims
                WHERE account_id=:a AND principal=:p AND operation_id=:o FOR UPDATE
                """),key).getResultList();
        if (claims.size()!=1) throw new RepositoryExecutionClaimLedger.Fenced();
        var claim = (Object[]) claims.getFirst();
        if (((Number) claim[0]).longValue()!=p.epoch()+1 || !reservation.successorToken().equals(claim[1])
                || !p.commandSha256().equals(HexFormat.of().formatHex((byte[]) claim[2])))
            throw new RepositoryExecutionClaimLedger.Fenced();
        var owners = scope(em.createNativeQuery("""
                SELECT owner_generation,owner_token FROM repository_operation_owners
                WHERE account_id=:a AND principal=:p AND operation_id=:o FOR UPDATE
                """),key).getResultList();
        if (owners.size()!=1) throw new RepositoryExecutionClaimLedger.Fenced();
        var retained = (Object[]) owners.getFirst();
        if (((Number) retained[0]).longValue()!=owner.generation() || !owner.nonce().equals(retained[1]))
            throw new RepositoryExecutionClaimLedger.Fenced();
        if (RepositoryCoordinatorReservation.read(em,reservation).isEmpty())
            throw new IllegalArgumentException("Reserved preparation requires committed reservation");
        boolean valid = (Boolean) scope(em.createNativeQuery("""
                SELECT c.lease_until>clock_timestamp() AND o.lease_until<=clock_timestamp()
                  AND NOT EXISTS(SELECT 1 FROM repository_coordinator_bindings b WHERE (b.account_id,b.principal,b.operation_id,b.claim_epoch)=(c.account_id,c.principal,c.operation_id,c.claim_epoch))
                  AND NOT EXISTS(SELECT 1 FROM repository_successor_executions e WHERE (e.account_id,e.principal,e.operation_id,e.claim_epoch)=(c.account_id,c.principal,c.operation_id,c.claim_epoch))
                  AND NOT EXISTS(SELECT 1 FROM repository_successor_installs i WHERE (i.account_id,i.principal,i.operation_id,i.successor_epoch)=(c.account_id,c.principal,c.operation_id,c.claim_epoch))
                  AND NOT EXISTS(SELECT 1 FROM repository_operation_success s WHERE (s.account_id,s.principal,s.operation_id)=(c.account_id,c.principal,c.operation_id))
                  AND NOT EXISTS(SELECT 1 FROM repository_operation_rejection r WHERE (r.account_id,r.principal,r.operation_id)=(c.account_id,c.principal,c.operation_id))
                FROM repository_execution_claims c JOIN repository_operation_owners o USING(account_id,principal,operation_id)
                WHERE c.account_id=:a AND c.principal=:p AND c.operation_id=:o
                """),key).getSingleResult();
        if (!valid) throw new RepositoryExecutionClaimLedger.Fenced();
    }

    private static Query preparation(EntityManager em, String columns, RepositoryOperationLedger.Key key,
                                     RepositoryCoordinatorReservation.OwnerIdentity owner) {
        return scope(em.createNativeQuery("SELECT " + columns + " FROM repository_publication_preparations"
                + " WHERE account_id=:a AND principal=:p AND operation_id=:o AND predecessor_generation=:g AND owner_nonce=:n"),key)
                .setParameter("g",owner.generation()-1).setParameter("n",owner.nonce());
    }
    private static Query scope(Query query, RepositoryOperationLedger.Key key) {
        return query.setParameter("a",key.account()).setParameter("p",key.principal()).setParameter("o",key.operationId());
    }
    private static Object single(Query query) {
        var rows = query.getResultList();
        if (rows.isEmpty()) throw new RepositoryException(RepositoryException.Code.FAILED_PRECONDITION,"Reserved preparation is absent");
        if (rows.size()!=1) throw new RepositoryException(RepositoryException.Code.DATA_LOSS,"Reserved preparation is not unique");
        return rows.getFirst();
    }
}
