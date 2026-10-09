package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.DocumentPublicationCommand;
import ai.protomolt.proto.repo.spi.RepositoryException;
import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

/** Append-only capture identity; not proof of drained work or permission to release retention. */
final class DocumentPreparationSourcePins {
    static final long MAX_BYTES = 4L * 1024 * 1024;
    private DocumentPreparationSourcePins() {}
    record Prepared(List<DocumentHistoricalSourcePin> pins, byte[] digest, String json) {
        Prepared { pins = List.copyOf(pins); digest = digest.clone(); }
        @Override public byte[] digest() { return digest.clone(); }
    }

    static Prepared prepare(DocumentPublicationCommand command,
            List<DocumentHistoricalReferenceAdmission.Prepared> sources, Runnable control) {
        sources = DocumentHistoricalReferenceAdmission.requireComplete(command, sources, control);
        var unique = new TreeMap<String, DocumentHistoricalSourcePin>();
        int count = 0;
        for (var source : sources) for (var pin : source.pins()) {
            control.run();
            if (++count > DocumentPublicationCommand.MAX_PARTS)
                throw new IllegalArgumentException("Preparation pin count exceeds bounds");
            var previous = unique.putIfAbsent(pin.pin().toString(), pin);
            if (previous != null && !previous.equals(pin)) throw corrupt();
        }
        var pins = List.copyOf(unique.values());
        if (pins.isEmpty()) throw new IllegalArgumentException("Historical preparation requires selected pins");
        var rows = new ArrayList<String>(pins.size());
        for (var pin : pins) {
            control.run();
            // All interpolated fields are typed UUIDs or positive captured revision numbers.
            rows.add("{\"reader\":\"" + pin.reader() + "\",\"pin\":\"" + pin.pin() + "\",\"object\":\""
                    + pin.object() + "\",\"node\":\"" + pin.node() + "\",\"revision\":\"" + pin.revision()
                    + "\",\"publication\":" + pin.publicationRevision() + "}");
        }
        String json = "[" + String.join(",", rows) + "]";
        if (json.length() > MAX_BYTES) throw new IllegalArgumentException("Preparation pin encoding exceeds bounds");
        return new Prepared(pins, digest(pins, control), json);
    }

    static byte[] digest(List<DocumentHistoricalSourcePin> pins, Runnable control) {
        try {
            var hash = MessageDigest.getInstance("SHA-256");
            hash.update("protomolt/preparation-pins/v1\n".getBytes(StandardCharsets.UTF_8));
            for (var pin : pins) {
                control.run();
                hash.update((pin.reader() + "/" + pin.pin() + "/" + pin.object() + "/" + pin.node()
                        + "/" + pin.revision() + "/" + pin.publicationRevision() + "\n").getBytes(StandardCharsets.UTF_8));
            }
            return hash.digest();
        } catch (NoSuchAlgorithmException failure) { throw new IllegalStateException("SHA-256 unavailable", failure); }
    }

    /**
     * Caller already holds the claim row before complete origin/retention locks and source Uses.
     * Resume validation rechecks that existing lock; it must not acquire a new claim lock after pins.
     */
    static void insert(EntityManager em, DocumentPublicationPreparationRecord record, Prepared prepared,
            RepositoryExecutionClaimLedger.Claim claim, java.util.UUID coordinator, Runnable control) {
        control.run();
        if (!record.key().equals(claim.key()) || !record.command().sha256().equals(claim.commandSha256()))
            throw new IllegalArgumentException("Capture owner differs from preparation");
        java.util.Objects.requireNonNull(coordinator);
        RepositoryCoordinatorBinding.requireResume(em, claim, coordinator);
        var existing = scope(em.createNativeQuery("""
                SELECT expected_count,sealed FROM repository_preparation_pin_batches
                WHERE account_id=:a AND principal=:p AND operation_id=:o AND predecessor_generation=:g AND pins_sha256=:digest
                """), record, prepared).getResultList();
        if (!existing.isEmpty()) {
            Object[] header = (Object[]) existing.getFirst();
            if (((Number) header[0]).intValue() != prepared.pins().size() || !Boolean.TRUE.equals(header[1])) throw corrupt();
            var owners = scope(em.createNativeQuery("""
                    SELECT claim_epoch,claim_token,incarnation FROM repository_preparation_pin_owners
                    WHERE account_id=:a AND principal=:p AND operation_id=:o AND predecessor_generation=:g AND pins_sha256=:digest
                    """), record, prepared).getResultList();
            if (owners.isEmpty()) throw new RepositoryException(RepositoryException.Code.FAILED_PRECONDITION,
                    "Preparation capture ownership is unknown");
            Object[] owner = (Object[]) owners.getFirst();
            if (((Number) owner[0]).longValue() != claim.epoch() || !claim.token().equals(owner[1])
                    || !coordinator.equals(owner[2]))
                throw new RepositoryException(RepositoryException.Code.FAILED_PRECONDITION,
                        "Preparation capture belongs to another execution; a fresh capture is required");
            // Confirmation does not depend on pins that may already have been released.
            long matches = ((Number) scope(em.createNativeQuery("""
                    SELECT count(*) FROM repository_preparation_source_pins p JOIN jsonb_to_recordset(CAST(:rows AS jsonb))
                     q(reader uuid,pin uuid,object uuid,node uuid,revision uuid,publication bigint)
                     ON p.pin_id=q.pin AND p.reader_incarnation=q.reader AND p.object_id=q.object
                      AND p.node_id=q.node AND p.revision_id=q.revision AND p.publication_revision=q.publication
                    WHERE account_id=:a AND principal=:p AND operation_id=:o AND predecessor_generation=:g AND pins_sha256=:digest
                    """), record, prepared).setParameter("rows", prepared.json()).getSingleResult()).longValue();
            if (matches != prepared.pins().size()) throw corrupt();
            control.run(); return;
        }
        var locked = em.createNativeQuery("""
                SELECT p.pin_id FROM document_read_pins p JOIN jsonb_to_recordset(CAST(:rows AS jsonb))
                 q(reader uuid,pin uuid,object uuid,node uuid,revision uuid,publication bigint)
                 ON p.pin_id=q.pin AND p.reader_incarnation=q.reader AND p.object_id=q.object
                  AND p.source_node=q.node AND p.source_revision=q.revision AND p.publication_revision=q.publication
                WHERE p.read_scope='HISTORICAL' ORDER BY p.pin_id FOR SHARE OF p
                """).setParameter("rows", prepared.json()).getResultList();
        if (locked.size() != prepared.pins().size()) throw new DocumentPartAttemptLedger.FenceException(
                "Preparation source pins differ from live historical capture");
        scope(em.createNativeQuery("""
                INSERT INTO repository_preparation_pin_batches(account_id,principal,operation_id,predecessor_generation,
                 pins_sha256,expected_count,initial_capture)
                SELECT account_id,principal,operation_id,predecessor_generation,:digest,:count,creation_xid=pg_current_xact_id()
                FROM repository_preparation_history_sets
                WHERE account_id=:a AND principal=:p AND operation_id=:o AND predecessor_generation=:g
                """), record, prepared).setParameter("count", prepared.pins().size()).executeUpdate();
        scope(em.createNativeQuery("""
                INSERT INTO repository_preparation_pin_owners(account_id,principal,operation_id,predecessor_generation,
                 pins_sha256,claim_epoch,claim_token,incarnation)
                VALUES(:a,:p,:o,:g,:digest,:epoch,:token,:incarnation)
                """), record, prepared).setParameter("epoch", claim.epoch()).setParameter("token", claim.token())
                .setParameter("incarnation", coordinator).executeUpdate();
        control.run();
        scope(em.createNativeQuery("""
                INSERT INTO repository_preparation_source_pins(account_id,principal,operation_id,predecessor_generation,
                 pins_sha256,pin_id,reader_incarnation,object_id,node_id,revision_id,publication_revision)
                SELECT :a,:p,:o,:g,:digest,q.pin,q.reader,q.object,q.node,q.revision,q.publication
                FROM jsonb_to_recordset(CAST(:rows AS jsonb))
                 q(reader uuid,pin uuid,object uuid,node uuid,revision uuid,publication bigint) ORDER BY q.pin
                """), record, prepared).setParameter("rows", prepared.json()).executeUpdate();
        scope(em.createNativeQuery("""
                UPDATE repository_preparation_pin_batches SET sealed=true
                WHERE account_id=:a AND principal=:p AND operation_id=:o AND predecessor_generation=:g AND pins_sha256=:digest
                """), record, prepared).executeUpdate();
        control.run();
    }

    /**
     * Confirms an existing initial capture without repairing or creating one. Caller owns
     * live source Work and holds the claim before ordered origin/retention locks. This is
     * capture evidence only: canonical preparation/root verification, current document
     * authority and operation ownership are separate caller obligations.
     */
    static void requireInitial(EntityManager em, DocumentPublicationPreparationRecord record, Prepared prepared,
            RepositoryExecutionClaimLedger.Claim claim, java.util.UUID coordinator, Runnable control) {
        requireInitial(em, record, prepared, claim, coordinator, control, true);
    }

    /** Only for a handle that already confirmed the full immutable batch at construction. */
    static void requireActiveInitial(EntityManager em, DocumentPublicationPreparationRecord record, Prepared prepared,
            RepositoryExecutionClaimLedger.Claim claim, java.util.UUID coordinator, Runnable control) {
        requireInitial(em, record, prepared, claim, coordinator, control, false);
    }

    private static void requireInitial(EntityManager em, DocumentPublicationPreparationRecord record, Prepared prepared,
            RepositoryExecutionClaimLedger.Claim claim, java.util.UUID coordinator, Runnable control, boolean full) {
        control.run();
        if (record.predecessorGeneration() != 0 || claim.epoch() != 1
                || !record.key().equals(claim.key()) || !record.command().sha256().equals(claim.commandSha256()))
            throw new IllegalArgumentException("Initial capture differs from preparation claim");
        requireCapture(em, record, prepared, claim, coordinator, control, full, true, "");
    }

    /**
     * Verify a fresh successor batch against already verified V109 activation evidence.
     * Caller holds its live claim before origin/retention locks and owns exact source Work.
     * The retained record is the original root anchor, not the successor's preparation.
     * This does not verify execution preparation, current document authorization or grant execution.
     */
    static void requireSuccessor(EntityManager em, DocumentPublicationPreparationRecord retention, Prepared prepared,
            RepositoryExecutionClaimLedger.Claim claim, java.util.UUID coordinator, String activationTransaction,
            Runnable control) {
        requireSuccessor(em, retention, prepared, claim, coordinator, activationTransaction, control, true);
    }

    /** Only for a handle whose full attachment verification and authorization completed. */
    static void requireActiveSuccessor(EntityManager em, DocumentPublicationPreparationRecord retention, Prepared prepared,
            RepositoryExecutionClaimLedger.Claim claim, java.util.UUID coordinator, String activationTransaction,
            Runnable control) {
        requireSuccessor(em, retention, prepared, claim, coordinator, activationTransaction, control, false);
    }

    private static void requireSuccessor(EntityManager em, DocumentPublicationPreparationRecord retention, Prepared prepared,
            RepositoryExecutionClaimLedger.Claim claim, java.util.UUID coordinator, String activationTransaction,
            Runnable control, boolean full) {
        if (claim.epoch() <= 1 || !retention.key().equals(claim.key())
                || !retention.command().sha256().equals(claim.commandSha256()))
            throw new IllegalArgumentException("Successor capture differs from preparation claim");
        if (activationTransaction == null || !activationTransaction.matches("[1-9][0-9]*"))
            throw new IllegalArgumentException("Successor capture requires an activation transaction");
        requireCapture(em, retention, prepared, claim, coordinator, control, full, false, activationTransaction);
    }

    private static void requireCapture(EntityManager em, DocumentPublicationPreparationRecord record, Prepared prepared,
            RepositoryExecutionClaimLedger.Claim claim, java.util.UUID coordinator, Runnable control,
            boolean full, boolean initial, String activationTransaction) {
        control.run();
        String kind = initial ? "Initial" : "Successor";
        java.util.Objects.requireNonNull(coordinator);
        RepositoryCoordinatorBinding.requireResume(em, claim, coordinator);
        boolean closed = (Boolean) em.createNativeQuery("""
                SELECT EXISTS(SELECT 1 FROM repository_publication_abandonments WHERE account_id=:a AND principal=:p AND operation_id=:o)
                  OR EXISTS(SELECT 1 FROM repository_operation_success WHERE account_id=:a AND principal=:p AND operation_id=:o)
                  OR EXISTS(SELECT 1 FROM repository_operation_rejection WHERE account_id=:a AND principal=:p AND operation_id=:o)
                  OR EXISTS(SELECT 1 FROM repository_coordinator_drains WHERE account_id=:a AND principal=:p AND operation_id=:o AND claim_epoch=:epoch)
                """).setParameter("a", record.key().account()).setParameter("p", record.key().principal())
                .setParameter("o", record.key().operationId()).setParameter("epoch", claim.epoch()).getSingleResult();
        if (closed) throw new RepositoryException(RepositoryException.Code.FAILED_PRECONDITION,
                kind + " preparation capture admission is closed");
        var rows = scope(em.createNativeQuery("""
                SELECT b.expected_count,b.sealed,b.initial_capture,
                  CASE WHEN :initial THEN b.creation_xid=h.creation_xid
                    ELSE b.creation_xid::text=CAST(:activation AS text) END,
                  own.claim_epoch,own.claim_token,own.incarnation,
                  CASE WHEN :full THEN (SELECT count(*) FROM repository_preparation_source_pins p
                    WHERE p.account_id=b.account_id AND p.principal=b.principal AND p.operation_id=b.operation_id
                      AND p.predecessor_generation=b.predecessor_generation AND p.pins_sha256=b.pins_sha256)
                    ELSE b.expected_count END,
                  EXISTS(SELECT 1 FROM repository_preparation_capture_drains d
                    WHERE d.account_id=b.account_id AND d.principal=b.principal AND d.operation_id=b.operation_id
                      AND d.predecessor_generation=b.predecessor_generation AND d.pins_sha256=b.pins_sha256),
                  EXISTS(SELECT 1 FROM repository_preparation_root_releases r
                    WHERE r.account_id=b.account_id AND r.principal=b.principal AND r.operation_id=b.operation_id
                      AND r.predecessor_generation=b.predecessor_generation)
                FROM repository_preparation_pin_batches b
                JOIN repository_preparation_history_sets h USING(account_id,principal,operation_id,predecessor_generation)
                JOIN repository_preparation_pin_owners own USING(account_id,principal,operation_id,predecessor_generation,pins_sha256)
                WHERE b.account_id=:a AND b.principal=:p AND b.operation_id=:o AND b.predecessor_generation=:g AND b.pins_sha256=:digest
                """), record, prepared).setParameter("full", full).setParameter("initial", initial)
                .setParameter("activation", activationTransaction).getResultList();
        if (rows.isEmpty()) throw new RepositoryException(RepositoryException.Code.FAILED_PRECONDITION,
                kind + " preparation capture is absent");
        Object[] row = (Object[]) rows.getFirst();
        if (((Number) row[0]).intValue() != prepared.pins().size() || !Boolean.TRUE.equals(row[1])
                || !Boolean.valueOf(initial).equals(row[2]) || !Boolean.TRUE.equals(row[3])
                || ((Number) row[7]).longValue() != prepared.pins().size()) throw corrupt();
        if (((Number) row[4]).longValue() != claim.epoch() || !claim.token().equals(row[5]) || !coordinator.equals(row[6])
                || Boolean.TRUE.equals(row[8]) || Boolean.TRUE.equals(row[9]))
            throw new RepositoryException(RepositoryException.Code.FAILED_PRECONDITION,
                    kind + " preparation capture is no longer executable");
        if (full) {
            long matches = ((Number) scope(em.createNativeQuery("""
                SELECT count(*) FROM repository_preparation_source_pins p
                JOIN jsonb_to_recordset(CAST(:rows AS jsonb))
                  q(reader uuid,pin uuid,object uuid,node uuid,revision uuid,publication bigint)
                  ON p.pin_id=q.pin AND p.reader_incarnation=q.reader AND p.object_id=q.object
                    AND p.node_id=q.node AND p.revision_id=q.revision AND p.publication_revision=q.publication
                WHERE p.account_id=:a AND p.principal=:p AND p.operation_id=:o AND p.predecessor_generation=:g AND p.pins_sha256=:digest
                """), record, prepared).setParameter("rows", prepared.json()).getSingleResult()).longValue();
            if (matches != prepared.pins().size()) throw corrupt();
        }
        var live = scope(em.createNativeQuery("""
                SELECT p.pin_id FROM document_read_pins p JOIN repository_preparation_source_pins q
                  ON p.pin_id=q.pin_id AND p.reader_incarnation=q.reader_incarnation AND p.object_id=q.object_id
                    AND p.source_node=q.node_id AND p.source_revision=q.revision_id AND p.publication_revision=q.publication_revision
                WHERE q.account_id=:a AND q.principal=:p AND q.operation_id=:o AND q.predecessor_generation=:g
                  AND q.pins_sha256=:digest AND p.read_scope='HISTORICAL' ORDER BY p.pin_id FOR SHARE OF p
                """), record, prepared).getResultList();
        if (live.size() != prepared.pins().size()) throw new DocumentPartAttemptLedger.FenceException(
                kind + " preparation capture no longer has its live historical pins");
        control.run();
    }

    private static Query scope(Query query, DocumentPublicationPreparationRecord record, Prepared prepared) {
        return query.setParameter("a", record.key().account()).setParameter("p", record.key().principal())
                .setParameter("o", record.key().operationId()).setParameter("g", record.predecessorGeneration())
                .setParameter("digest", prepared.digest());
    }
    private static RepositoryException corrupt() {
        return new RepositoryException(RepositoryException.Code.DATA_LOSS,
                "Preparation source pin batch differs from its captured identities");
    }
}
