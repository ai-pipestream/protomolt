package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.container.blob.DocumentIds;
import ai.protomolt.proto.repo.spi.DocumentPublicationCommand;
import com.google.protobuf.ListValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import com.google.protobuf.util.JsonFormat;
import jakarta.persistence.EntityManager;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.UUID;

/** Internal whole-plan acquisition and release; DocumentReadLedger owns local read lifetimes. */
final class DocumentReadPins {
    private DocumentReadPins() {}
    record Pin(UUID id, UUID object) {}
    record Captured<P>(P plan, UUID reader, List<Pin> pins) {
        Captured { pins = List.copyOf(pins); }
    }
    static final class Prepared {
        private final DocumentPublicationCommand command;
        private final String rows;
        private final List<Pin> pins;
        private Prepared(DocumentPublicationCommand command, String rows, List<Pin> pins) {
            this.command = command; this.rows = rows; this.pins = List.copyOf(pins);
        }
    }

    /** Deduplicate physical protection only; admission must still validate every canonical claim. */
    static Prepared prepare(DocumentPublicationCommand command) {
        var witnesses = new LinkedHashMap<UUID, UUID>();
        for (var member : command.intent().getMembersList()) for (var part : member.getPartsList()) {
            if (part.hasReuse()) witnesses.putIfAbsent(UUID.fromString(part.getReuse().getObject().getObjectId()),
                    DocumentIds.nodeId(part.getReuse().getSource().getAddress()));
        }
        var pins = new ArrayList<Pin>(witnesses.size());
        var rows = ListValue.newBuilder();
        witnesses.forEach((object, source) -> {
            var pin = new Pin(UUID.randomUUID(), object); pins.add(pin);
            rows.addValues(Value.newBuilder().setStructValue(Struct.newBuilder()
                    .putFields("pin", text(pin.id())).putFields("object", text(object)).putFields("source", text(source))));
        });
        try { return new Prepared(command, JsonFormat.printer().omittingInsignificantWhitespace().print(rows), pins); }
        catch (com.google.protobuf.InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Cannot encode document read pin plan", failure);
        }
    }
    private static Value text(UUID value) { return Value.newBuilder().setStringValue(value.toString()).build(); }

    /** Caller must drain all provider work and batch owners first; failures retain the complete set. */
    static void release(Tx tx, Captured<?> captured) {
        finish(tx, captured, false);
    }

    /** Recovery consumes durable QUIESCED evidence; it cannot establish local or remote drain. */
    static void recover(Tx tx, Captured<?> captured) {
        finish(tx, captured, true);
    }

    private static void finish(Tx tx, Captured<?> captured, boolean recovery) {
        java.util.Objects.requireNonNull(captured.reader(), "Pinned reader identity");
        if (captured.pins().size() > DocumentPublicationCommand.MAX_PARTS)
            throw new IllegalArgumentException("Read pin release exceeds command bounds");
        if (captured.pins().isEmpty()) return;
        var rows = ListValue.newBuilder();
        for (var pin : captured.pins()) rows.addValues(Value.newBuilder().setStructValue(Struct.newBuilder()
                .putFields("pin", text(pin.id())).putFields("object", text(pin.object()))));
        final String encoded;
        try { encoded = JsonFormat.printer().omittingInsignificantWhitespace().print(rows); }
        catch (com.google.protobuf.InvalidProtocolBufferException failure) {
            throw new IllegalArgumentException("Cannot encode read pin release", failure);
        }
        tx.inTransaction(em -> {
            String query = recovery ? "SELECT recover_quiesced_document_read_pins(:reader,CAST(:claims AS jsonb))"
                    : "SELECT release_document_read_pins(:reader,CAST(:claims AS jsonb))";
            if (!Boolean.TRUE.equals(em.createNativeQuery(query)
                    .setParameter("reader", captured.reader()).setParameter("claims", encoded).getSingleResult()))
                throw new DocumentPartAttemptLedger.FenceException("Read pin release did not complete");
        });
    }

    /** Caller holds owner, active reader and authorized source/destination revision locks. */
    static Captured<DocumentRetainedReadPlan> acquire(EntityManager em, Prepared prepared, DocumentRetainedReadPlan plan, UUID reader,
            DocumentReuseAdmission.Prepared reuse) {
        if (prepared.command != plan.command()) throw new IllegalArgumentException("Read pin plan differs from captured command");
        if (prepared.pins.isEmpty()) return new Captured<>(plan, reader, List.of());
        // Lock complete distinct origin set before touching ANY retention row. PostgreSQL
        // UUID ordering is authoritative, including UUIDs whose high bit is set.
        em.createNativeQuery("""
                SELECT a.attempt_id FROM document_part_attempts a
                WHERE a.attempt_id IN (
                  SELECT l.source_id FROM jsonb_to_recordset(CAST(:rows AS jsonb)) q(object uuid)
                  JOIN repository_physical_locations l ON l.object_id=q.object AND l.source_kind='DOCUMENT_PART')
                ORDER BY a.attempt_id FOR SHARE OF a
                """).setParameter("rows", prepared.rows).getResultList();
        // Recheck every claim after locking origins, including claims sharing a
        // physical pin but naming different sources once mixed revisions activate.
        DocumentReuseAdmission.requireBoundSources(em, reuse);
        var retained = em.createNativeQuery("""
                SELECT r.object_id FROM repository_object_retention r
                WHERE r.object_id IN (SELECT q.object FROM jsonb_to_recordset(CAST(:rows AS jsonb)) q(object uuid))
                ORDER BY r.object_id FOR SHARE OF r
                """).setParameter("rows", prepared.rows).getResultList();
        if (retained.size() != prepared.pins.size()) throw new DocumentPartAttemptLedger.FenceException("Read pin retention is incomplete");
        int inserted = em.createNativeQuery("""
                INSERT INTO document_read_pins(pin_id,reader_incarnation,object_id,source_node,source_revision,publication_revision)
                SELECT q.pin,:reader,q.object,q.source,r.revision_id,r.publication_revision
                FROM jsonb_to_recordset(CAST(:rows AS jsonb)) q(pin uuid,object uuid,source uuid)
                JOIN document_revision_current c ON c.node_id=q.source
                JOIN document_revision_publications r USING(revision_id)
                ORDER BY q.object
                """).setParameter("rows", prepared.rows).setParameter("reader", reader).executeUpdate();
        if (inserted != prepared.pins.size()) throw new DocumentPartAttemptLedger.FenceException("Read pin source is incomplete");
        return new Captured<>(plan, reader, prepared.pins);
    }
}
