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
