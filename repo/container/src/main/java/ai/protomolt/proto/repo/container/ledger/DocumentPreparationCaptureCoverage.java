package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.container.blob.DocumentIds;
import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;
import java.security.MessageDigest;
import java.util.*;

/** Private transaction-local capture qualification. This neither releases roots nor proves terminality. */
final class DocumentPreparationCaptureCoverage {
    private record ObjectKey(UUID node, UUID revision, UUID object) {}
    private record Selection(String json, int count) {}
    private final DocumentPublicationPreparationRecord record;
    private final byte[] preparationDigest;
    private final List<Selection> selections;

    /** Bounded canonical encoding happens before SQL locks. No live source handle is required. */
    static DocumentPreparationCaptureCoverage prepare(DocumentPublicationPreparationRecord record, RepositoryReadControl control) {
        control.check();
        var digest = DocumentPublicationPreparationJournal.digest(DocumentPublicationPreparationCodec.encode(record));
        var selectors = record.command().intent().getMembersList().stream().flatMap(m -> m.getPartsList().stream())
                .filter(p -> p.hasHistoricalReuse()).map(p -> p.getHistoricalReuse()).distinct().toList();
        if (selectors.isEmpty()) throw incomplete("Historical capture coverage requires historical selectors");
        var batches = new ArrayList<Selection>();
        for (int start = 0; start < selectors.size(); start += 256) {
            var rows = new JsonArray();
            for (int i = start; i < Math.min(start + 256, selectors.size()); i++) {
                control.check(); var selector = selectors.get(i); var row = new JsonObject();
                row.addProperty("node", DocumentIds.nodeId(selector.getSource()).toString());
                row.addProperty("revision", selector.getRevisionId());
                row.addProperty("object", selector.getObject().getObjectId());
                row.addProperty("ordinal", selector.getRevisionOrdinal());
                row.addProperty("part", selector.getSourceSlot().getPartValue());
                row.addProperty("sub_key", selector.getSourceSlot().getSubKey());
                rows.add(row);
            }
            batches.add(new Selection(rows.toString(), rows.size()));
        }
        control.check();
        return new DocumentPreparationCaptureCoverage(record, digest, List.copyOf(batches));
    }

    private DocumentPreparationCaptureCoverage(DocumentPublicationPreparationRecord record, byte[] digest, List<Selection> selections) {
        this.record = record; this.preparationDigest = digest; this.selections = selections;
    }

    /** Lock order: claim, preparation, root header, digest-ordered batches. Caller retains this transaction. */
    int lockAndRequireDrained(EntityManager em, RepositoryReadControl control) {
        control.check();
        var claims = operation(em.createNativeQuery("""
                SELECT command_sha256,CAST(require_repository_read_committed() AS text) FROM repository_execution_claims
                WHERE account_id=:a AND principal=:p AND operation_id=:o FOR UPDATE
                """)).getResultList();
        if (claims.isEmpty()) throw incomplete("Preparation claim is absent");
        if (!MessageDigest.isEqual((byte[]) ((Object[]) claims.getFirst())[0], HexFormat.of().parseHex(record.command().sha256()))) throw corrupt();
        var preparations = scope(em.createNativeQuery("""
                SELECT preparation_sha256 FROM repository_publication_preparations
                WHERE account_id=:a AND principal=:p AND operation_id=:o AND predecessor_generation=:g FOR UPDATE
                """)).getResultList();
        if (preparations.isEmpty()) throw incomplete("Preparation is absent");
        if (!MessageDigest.isEqual((byte[]) preparations.getFirst(), preparationDigest)) throw corrupt();
        var headers = scope(em.createNativeQuery("""
                SELECT creation_xid::text FROM repository_preparation_history_sets
                WHERE account_id=:a AND principal=:p AND operation_id=:o AND predecessor_generation=:g FOR UPDATE
                """)).getResultList();
        if (headers.isEmpty()) throw incomplete("Preparation history coverage is unknown");
        if (DocumentPreparationHistoryRoots.coverage(em, record, preparationDigest) != DocumentPreparationHistoryRoots.Coverage.EXACT)
            throw incomplete("Preparation history coverage is unknown");
        var expected = selectedObjects(em, control);
        var batches = scope(em.createNativeQuery("""
                SELECT b.pins_sha256,b.expected_count,b.sealed,b.initial_capture,b.creation_xid::text,
                  o.claim_epoch,o.claim_token,o.incarnation,
                  d.claim_epoch,d.claim_token,d.incarnation,d.command_sha256
                FROM repository_preparation_pin_batches b
                LEFT JOIN repository_preparation_pin_owners o USING(account_id,principal,operation_id,predecessor_generation,pins_sha256)
                LEFT JOIN repository_preparation_capture_drains d USING(account_id,principal,operation_id,predecessor_generation,pins_sha256)
                WHERE b.account_id=:a AND b.principal=:p AND b.operation_id=:o AND b.predecessor_generation=:g
                ORDER BY b.pins_sha256 FOR UPDATE OF b
                """)).setMaxResults(17).getResultList();
        if (batches.isEmpty()) throw incomplete("Preparation capture coverage is unknown");
        if (batches.size() > 16) throw corrupt();
        boolean initial = false;
        for (var value : batches) {
            control.check(); var batch = (Object[]) value;
            boolean original = headers.getFirst().equals(batch[4]);
            if (!Boolean.TRUE.equals(batch[2]) || !Boolean.valueOf(original).equals(batch[3])) throw corrupt();
            initial |= original;
            if (batch[5] == null) throw incomplete("Preparation capture ownership is unknown");
            if (batch[8] == null) throw incomplete("Preparation capture has not drained");
            if (!batch[5].equals(batch[8]) || !batch[6].equals(batch[9]) || !batch[7].equals(batch[10])
                    || !MessageDigest.isEqual((byte[]) batch[11], HexFormat.of().parseHex(record.command().sha256()))) throw corrupt();
            var rows = scope(em.createNativeQuery("""
                    SELECT s.reader_incarnation,s.pin_id,s.object_id,s.node_id,s.revision_id,s.publication_revision,
                      EXISTS(SELECT 1 FROM document_read_pins p WHERE p.pin_id=s.pin_id)
                       OR EXISTS(SELECT 1 FROM repository_object_references r WHERE r.owner_kind='DOCUMENT_READER' AND r.owner_id=s.pin_id)
                    FROM repository_preparation_source_pins s
                    WHERE account_id=:a AND principal=:p AND operation_id=:o AND predecessor_generation=:g AND pins_sha256=:digest
                    ORDER BY pin_id
                    """)).setParameter("digest", batch[0]).setMaxResults(10001).getResultList();
            if (rows.isEmpty() || rows.size() > 10000 || rows.size() != ((Number) batch[1]).intValue()) throw corrupt();
            var pins = new ArrayList<DocumentHistoricalSourcePin>(rows.size());
            var actual = new HashMap<ObjectKey, Long>();
            for (var raw : rows) {
                control.check(); var row = (Object[]) raw;
                if (Boolean.TRUE.equals(row[6])) throw incomplete("Preparation capture still has native pins or mirrors");
                var pin = new DocumentHistoricalSourcePin((UUID) row[0], (UUID) row[1], (UUID) row[2], (UUID) row[3],
                        (UUID) row[4], ((Number) row[5]).longValue());
                pins.add(pin);
                var key = new ObjectKey(pin.node(), pin.revision(), pin.object());
                var prior = actual.putIfAbsent(key, pin.publicationRevision());
                if (prior != null) throw corrupt();
            }
            if (!actual.equals(expected) || !MessageDigest.isEqual((byte[]) batch[0], DocumentPreparationSourcePins.digest(pins, control::check)))
                throw corrupt();
        }
        if (!initial) throw incomplete("Preparation initial capture coverage is unknown");
        control.check(); return batches.size();
    }

    private Map<ObjectKey, Long> selectedObjects(EntityManager em, RepositoryReadControl control) {
        var expected = new HashMap<ObjectKey, Long>();
        for (var selection : selections) {
            control.check();
            var rows = em.createNativeQuery("""
                    SELECT h.node_id,h.revision_id,p.object_id,h.publication_revision
                    FROM jsonb_to_recordset(CAST(:rows AS jsonb)) q(node uuid,revision uuid,object uuid,ordinal integer,part integer,sub_key text)
                    JOIN document_revision_publications h ON h.node_id=q.node AND h.revision_id=q.revision AND h.projection_sealed
                    JOIN document_revision_parts p ON p.revision_id=h.revision_id AND p.revision_ordinal=q.ordinal
                      AND p.part=q.part AND p.sub_key=q.sub_key AND p.object_id=q.object
                    WHERE h.body->>'account_id'=:account
                    """).setParameter("rows", selection.json()).setParameter("account", record.key().account()).getResultList();
            if (rows.size() != selection.count()) throw corrupt();
            for (var raw : rows) {
                control.check(); var row = (Object[]) raw;
                expected.put(new ObjectKey((UUID) row[0], (UUID) row[1], (UUID) row[2]), ((Number) row[3]).longValue());
            }
        }
        return expected;
    }

    private Query operation(Query q) {
        return q.setParameter("a", record.key().account()).setParameter("p", record.key().principal()).setParameter("o", record.key().operationId());
    }
    private Query scope(Query q) { return operation(q).setParameter("g", record.predecessorGeneration()); }
    private static RepositoryException corrupt() { return new RepositoryException(RepositoryException.Code.DATA_LOSS, "Preparation capture coverage differs from canonical history"); }
    private static RepositoryException incomplete(String message) { return new RepositoryException(RepositoryException.Code.FAILED_PRECONDITION, message); }
}
