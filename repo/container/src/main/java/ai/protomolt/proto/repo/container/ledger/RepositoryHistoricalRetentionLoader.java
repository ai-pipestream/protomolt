package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.*;
import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;
import java.security.MessageDigest;
import java.util.*;

/** Private pre-install recovery metadata. Loading never grants execution or read pins. */
final class RepositoryHistoricalRetentionLoader {
    private final Tx tx;
    private final PayloadBudget budget;
    RepositoryHistoricalRetentionLoader(Tx tx, PayloadBudget budget, SqlTimeouts timeouts) {
        this.tx = Objects.requireNonNull(tx).withTimeouts(Objects.requireNonNull(timeouts));
        this.budget = Objects.requireNonNull(budget);
    }

    Loaded load(RepositoryCaller authority, RepositoryCaller caller,
            RepositoryCoordinatorReservation.Proposal reservation,
            RepositoryCoordinatorReservation.OwnerIdentity owner,
            DocumentPublicationPreparationRecord previous, RepositoryReadControl control) {
        RepositoryCoordinatorReservation.require(authority, reservation, control);
        var key = reservation.predecessor().key();
        DocumentAdmissionAuthorization.requireCaller(caller, key, key.account());
        if (!key.equals(previous.key()) || !reservation.predecessor().commandSha256().equals(previous.command().sha256())
                || owner.generation() - 1 != previous.predecessorGeneration()
                || !owner.nonce().equals(previous.seeds().ownerNonce())) throw inconsistent();
        var lease = budget.reserve(3L * DocumentPublicationPreparationCodec.MAX_BYTES);
        try {
            byte[] previousSha = DocumentPublicationPreparationJournal.digest(DocumentPublicationPreparationCodec.encode(previous));
            var captured = tx.inTransaction(em -> {
                RepositoryReservedPreparation.requireState(em, reservation, owner);
                DocumentAdmissionAuthorization.authorizePending(em, caller, previous.command());
                var anchor = discover(em, previous, previousSha, control);
                int size = ((Number) single(row(em, key, anchor.generation(), "octet_length(preparation_bytes)"))).intValue();
                if (size < 1 || size > DocumentPublicationPreparationCodec.MAX_BYTES) throw inconsistent();
                var bytes = (Object[]) single(row(em, key, anchor.generation(),
                        "preparation_bytes,preparation_sha256,owner_nonce,command_sha256"));
                control.check();
                return new Captured(anchor, bytes, size);
            });
            control.check();
            if (!captured.anchor().sha256().equals(HexFormat.of().formatHex((byte[]) captured.row()[1]))
                    || !captured.anchor().nonce().equals(captured.row()[2])) throw inconsistent();
            var record = DocumentPublicationPreparationJournal.decode(captured.row(), captured.size(), key,
                    previous.command().sha256(), captured.anchor().generation());
            tx.inTransaction(em -> {
                RepositoryReservedPreparation.requireState(em, reservation, owner);
                var current = discover(em, previous, previousSha, control);
                if (!current.equals(captured.anchor())) throw inconsistent();
                DocumentHistoricalRetentionBinding.require(em, record,
                        HexFormat.of().parseHex(current.sha256()), true);
                DocumentAdmissionAuthorization.authorizePending(em, caller, previous.command());
                control.check();
                return null;
            });
            control.check();
            // Decode scratch is temporary. Retain only the bounded encoded record size,
            // with both reservations held during the ownership transfer.
            var retained = budget.reserve(captured.size());
            try {
                return new Loaded(record, retained);
            } catch (RuntimeException | Error failure) { retained.close(); throw failure; }
        } finally { lease.close(); }
    }

    private record Anchor(long generation, String sha256, UUID nonce) {}
    private record Captured(Anchor anchor, Object[] row, int size) {}

    /** Immutable metadata reads only; no descending row locks or provider work. */
    private static Anchor discover(EntityManager em, DocumentPublicationPreparationRecord previous,
            byte[] previousSha, RepositoryReadControl control) {
        var key = previous.key();
        long generation = previous.predecessorGeneration();
        byte[] digest = previousSha;
        UUID nonce = previous.seeds().ownerNonce();
        Anchor activationAnchor = null;
        for (int edges = 0; edges < 64; edges++) {
            control.check();
            var identity = (Object[]) single(row(em, key, generation, "preparation_sha256,owner_nonce,command_sha256"));
            if (!MessageDigest.isEqual(digest, (byte[]) identity[0]) || !nonce.equals(identity[1])
                    || !previous.command().sha256().equals(HexFormat.of().formatHex((byte[]) identity[2]))) throw inconsistent();
            var activations = scope(em.createNativeQuery("""
                    SELECT retention_generation,retention_sha256,preparation_sha256,command_sha256
                    FROM repository_historical_activations
                    WHERE account_id=:a AND principal=:p AND operation_id=:o AND predecessor_generation=:g
                    """), key).setParameter("g", generation).setMaxResults(2).getResultList();
            if (activations.size() > 1) throw inconsistent();
            if (!activations.isEmpty()) {
                var activation = (Object[]) activations.getFirst();
                if (!MessageDigest.isEqual(digest, (byte[]) activation[2])
                        || !previous.command().sha256().equals(HexFormat.of().formatHex((byte[]) activation[3]))) throw inconsistent();
                var observed = new Anchor(((Number) activation[0]).longValue(), HexFormat.of().formatHex((byte[]) activation[1]), null);
                if (observed.generation() >= generation || (activationAnchor != null && !activationAnchor.equals(observed))) throw inconsistent();
                activationAnchor = observed;
            }
            boolean initial = (Boolean) scope(em.createNativeQuery("""
                    SELECT EXISTS(SELECT 1 FROM repository_preparation_history_sets h
                    JOIN repository_preparation_pin_batches b USING(account_id,principal,operation_id,predecessor_generation)
                    JOIN repository_preparation_pin_owners o USING(account_id,principal,operation_id,predecessor_generation,pins_sha256)
                    WHERE h.account_id=:a AND h.principal=:p AND h.operation_id=:o AND h.predecessor_generation=:g
                      AND h.sealed AND h.expected_count>0 AND h.preparation_sha256=:sha
                      AND h.command_sha256=:command AND b.initial_capture AND b.sealed AND b.creation_xid=h.creation_xid)
                    """), key).setParameter("g", generation).setParameter("sha", digest)
                    .setParameter("command", HexFormat.of().parseHex(previous.command().sha256())).getSingleResult();
            if (initial) {
                requireInitialCapture(em, key, generation);
                var anchor = new Anchor(generation, HexFormat.of().formatHex(digest), nonce);
                if (activationAnchor != null && (activationAnchor.generation() != generation
                        || !activationAnchor.sha256().equals(anchor.sha256()))) throw inconsistent();
                return anchor;
            }
            if (generation == 0) throw unavailable();
            // One additional edge will be installed for the planned successor.
            if (edges == 63) throw new RepositoryException(RepositoryException.Code.FAILED_PRECONDITION,
                    "Historical recovery exceeds the 64-link activation limit including the planned successor");
            var edge = (Object[]) single(scope(em.createNativeQuery("""
                    SELECT preparation_sha256,owner_nonce,command_sha256,predecessor_preparation_sha256,predecessor_nonce
                    FROM repository_successor_installs
                    WHERE account_id=:a AND principal=:p AND operation_id=:o AND predecessor_generation=:g
                    """), key).setParameter("g", generation));
            if (!MessageDigest.isEqual(digest, (byte[]) edge[0]) || !nonce.equals(edge[1])
                    || !previous.command().sha256().equals(HexFormat.of().formatHex((byte[]) edge[2]))) throw inconsistent();
            digest = (byte[]) edge[3]; nonce = (UUID) edge[4]; generation--;
        }
        throw new AssertionError("Ancestry loop exceeded its explicit limit");
    }

    private static void requireInitialCapture(EntityManager em, RepositoryOperationLedger.Key key, long generation) {
        var rows = scope(em.createNativeQuery("""
                SELECT b.pins_sha256,b.expected_count,o.claim_token,o.incarnation,i.claim_token,i.incarnation,
                  (SELECT count(*) FROM repository_preparation_source_pins p
                   WHERE (p.account_id,p.principal,p.operation_id,p.predecessor_generation,p.pins_sha256)=
                         (b.account_id,b.principal,b.operation_id,b.predecessor_generation,b.pins_sha256)),
                  (SELECT sha256(convert_to('protomolt/preparation-pins/v1' || chr(10) ||
                    coalesce(string_agg(p.reader_incarnation::text || '/' || p.pin_id::text || '/' || p.object_id::text || '/' ||
                      p.node_id::text || '/' || p.revision_id::text || '/' || p.publication_revision::text || chr(10),
                      '' ORDER BY p.pin_id),''),'UTF8'))
                   FROM repository_preparation_source_pins p
                   WHERE (p.account_id,p.principal,p.operation_id,p.predecessor_generation,p.pins_sha256)=
                         (b.account_id,b.principal,b.operation_id,b.predecessor_generation,b.pins_sha256)),
                  CASE WHEN b.predecessor_generation=0 THEN o.claim_epoch=1 ELSE EXISTS(
                    SELECT 1 FROM repository_successor_installs edge
                    JOIN repository_publication_preparations p USING(account_id,principal,operation_id,predecessor_generation)
                    WHERE edge.account_id=b.account_id AND edge.principal=b.principal AND edge.operation_id=b.operation_id
                      AND edge.predecessor_generation=b.predecessor_generation
                      AND edge.preparation_sha256=p.preparation_sha256 AND edge.owner_nonce=p.owner_nonce
                      AND edge.command_sha256=p.command_sha256 AND edge.successor_epoch=o.claim_epoch
                      AND edge.install_xid=h.creation_xid
                      AND edge.successor_token=o.claim_token AND edge.successor_incarnation=o.incarnation) END
                FROM repository_preparation_pin_batches b
                JOIN repository_preparation_pin_owners o USING(account_id,principal,operation_id,predecessor_generation,pins_sha256)
                JOIN repository_coordinator_bindings i USING(account_id,principal,operation_id,claim_epoch)
                JOIN repository_preparation_history_sets h USING(account_id,principal,operation_id,predecessor_generation)
                WHERE b.account_id=:a AND b.principal=:p AND b.operation_id=:o AND b.predecessor_generation=:g
                  AND b.initial_capture AND b.sealed AND b.creation_xid=h.creation_xid
                """), key).setParameter("g", generation).setMaxResults(2).getResultList();
        if (rows.size() != 1) throw inconsistent();
        var row = (Object[]) rows.getFirst();
        if (!Boolean.TRUE.equals(row[8]) || !row[2].equals(row[4]) || !row[3].equals(row[5])
                || ((Number) row[1]).longValue() != ((Number) row[6]).longValue()
                || !MessageDigest.isEqual((byte[]) row[0], (byte[]) row[7])) throw inconsistent();
    }

    static final class Loaded implements AutoCloseable {
        private DocumentPublicationPreparationRecord record;
        private final PayloadBudget.Lease lease;
        private Loaded(DocumentPublicationPreparationRecord record, PayloadBudget.Lease lease) { this.record = record; this.lease = lease; }
        synchronized DocumentPublicationPreparationRecord record() {
            if (record == null) throw new IllegalStateException("Historical retention load is closed");
            return record;
        }
        @Override public synchronized void close() { record = null; lease.close(); }
        @Override public String toString() { return "HistoricalRetention[private]"; }
    }
    private static Query row(EntityManager em, RepositoryOperationLedger.Key key, long generation, String columns) {
        return scope(em.createNativeQuery("SELECT " + columns + " FROM repository_publication_preparations"
                + " WHERE account_id=:a AND principal=:p AND operation_id=:o AND predecessor_generation=:g"), key)
                .setParameter("g", generation);
    }
    private static Query scope(Query query, RepositoryOperationLedger.Key key) {
        return query.setParameter("a", key.account()).setParameter("p", key.principal()).setParameter("o", key.operationId());
    }
    private static Object single(Query query) {
        var rows = query.setMaxResults(2).getResultList();
        if (rows.isEmpty()) throw unavailable();
        if (rows.size() != 1) throw inconsistent();
        return rows.getFirst();
    }
    private static RepositoryException unavailable() {
        return new RepositoryException(RepositoryException.Code.FAILED_PRECONDITION, "Historical retention ancestry is unavailable");
    }
    private static RepositoryException inconsistent() {
        return new RepositoryException(RepositoryException.Code.DATA_LOSS, "Historical retention ancestry differs from its saved identity");
    }
}
