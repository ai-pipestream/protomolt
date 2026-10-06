package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.container.blob.DocumentIds;
import ai.protomolt.proto.repo.spi.DocumentPublicationCommand;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import ai.protomolt.proto.repo.v1.DocumentPublicationSlot;
import com.google.protobuf.ListValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import com.google.protobuf.util.JsonFormat;
import jakarta.persistence.EntityManager;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/** Exact candidate associations for a new assessment; not a replay or authorization API. */
final class DocumentAssessmentSlots {
    private static final int BATCH_SIZE = 256;
    private record Source(UUID node, DocumentPublicationSlot slot) {}
    private record Origin(UUID revision, int ordinal) {}
    private record Batch(String json, int count) {}
    record Slot(String member, int ordinal, long selection, UUID object, String declaration,
                UUID sourceRevision, Integer sourceOrdinal, UUID sourceNode) {
        Slot(String member, int ordinal, long selection, UUID object, String declaration,
                UUID sourceRevision, Integer sourceOrdinal) {
            this(member, ordinal, selection, object, declaration, sourceRevision, sourceOrdinal, null);
        }
    }
    static final class Prepared {
        private final DocumentPublicationCommand command;
        private final Map<Source, UUID> expected;
        private final List<Batch> batches;
        private final List<DocumentHistoricalReferenceAdmission.Prepared> historical;
        private Prepared(DocumentPublicationCommand command, Map<Source, UUID> expected, List<Batch> batches,
                List<DocumentHistoricalReferenceAdmission.Prepared> historical) {
            this.command = command; this.expected = Map.copyOf(expected); this.batches = List.copyOf(batches);
            this.historical = List.copyOf(historical);
        }
    }
    private DocumentAssessmentSlots() {}

    /** Encode only bounded identities before any database locks; no fragment bytes or manifests. */
    static Prepared prepare(DocumentPublicationCommand command, Runnable control) {
        command.requireExecutionSupported();
        return prepare(command, List.of(), control);
    }

    /** Explicit internal historical preparation. The caller owns every source Use through staging/commit. */
    static Prepared prepare(DocumentPublicationCommand command,
            List<DocumentHistoricalReferenceAdmission.Prepared> historical, Runnable control) {
        historical = DocumentHistoricalReferenceAdmission.requireComplete(command, historical, control);
        var expected = new LinkedHashMap<Source, UUID>();
        for (var member : command.intent().getMembersList()) for (var part : member.getPartsList()) {
            control.run();
            if (!part.hasReuse()) continue;
            var reuse = part.getReuse();
            var source = new Source(DocumentIds.nodeId(reuse.getSource().getAddress()), reuse.getSourceSlot());
            var object = UUID.fromString(reuse.getObject().getObjectId());
            var prior = expected.putIfAbsent(source, object);
            if (prior != null && !prior.equals(object)) throw conflict();
        }
        if (expected.size() > 10000) throw conflict();
        var batches = new ArrayList<Batch>();
        var values = ListValue.newBuilder();
        for (var entry : expected.entrySet()) {
            control.run();
            values.addValues(Value.newBuilder().setStructValue(Struct.newBuilder()
                    .putFields("node", text(entry.getKey().node().toString()))
                    .putFields("part", text(Integer.toString(entry.getKey().slot().getPartValue())))
                    .putFields("sub_key", text(entry.getKey().slot().getSubKey()))
                    .putFields("object_id", text(entry.getValue().toString()))));
            if (values.getValuesCount() == BATCH_SIZE) { batches.add(encode(values)); values.clear(); }
        }
        if (values.getValuesCount() > 0) batches.add(encode(values));
        control.run();
        historical.forEach(source -> source.selectors());
        return new Prepared(command, expected, batches, historical);
    }

    /**
     * Caller has authorized the full command and holds logical/source, assessment
     * owner and physical locks. Run after bindAssessment; never use this on a
     * retained stage whose original sources may have retired or advanced.
     */
    static List<Slot> bind(EntityManager em, Prepared prepared, DocumentCommitParts.Bound physical, Runnable control) {
        if (!prepared.historical.isEmpty()) throw conflict();
        return bind(em, prepared, physical, null, control);
    }

    /** Historical binding additionally requires the complete transaction-local origin/retention lock proof. */
    static List<Slot> bind(EntityManager em, Prepared prepared, DocumentCommitParts.Bound physical,
            DocumentPublicationLocks.IndependentOrigins locks, Runnable control) {
        if (!prepared.historical.isEmpty()) {
            java.util.Objects.requireNonNull(locks, "Historical binding requires origin/retention locks");
            var readControl = new RepositoryReadControl() {
                @Override public boolean isCancelled() { control.run(); return Thread.currentThread().isInterrupted(); }
                @Override public long remainingNanos() { return Long.MAX_VALUE; }
            };
            for (var source : prepared.historical)
                DocumentHistoricalReferenceAdmission.requireBoundSources(em, source, locks, readControl);
        }
        var origins = new HashMap<Source, Origin>();
        for (var batch : prepared.batches) {
            control.run();
            var rows = em.createNativeQuery("""
                    SELECT q.node,q.part,q.sub_key,p.object_id,c.revision_id,p.revision_ordinal
                    FROM jsonb_to_recordset(CAST(:rows AS jsonb)) q(node uuid,part integer,sub_key text,object_id uuid)
                    JOIN document_revision_current c ON c.node_id=q.node
                    JOIN document_revision_publications r ON r.revision_id=c.revision_id AND r.node_id=q.node AND r.projection_sealed
                    JOIN document_revision_parts p ON p.revision_id=r.revision_id
                        AND p.part=q.part AND p.sub_key=q.sub_key AND p.object_id=q.object_id
                    """).setParameter("rows", batch.json()).getResultList();
            if (rows.size() != batch.count()) throw conflict();
            for (Object value : rows) {
                control.run();
                var row = (Object[]) value;
                var source = new Source((UUID) row[0], DocumentPublicationSlot.newBuilder()
                        .setPartValue(((Number) row[1]).intValue()).setSubKey((String) row[2]).build());
                if (!java.util.Objects.equals(prepared.expected.get(source), row[3])
                        || origins.put(source, new Origin((UUID) row[4], ((Number) row[5]).intValue())) != null) throw conflict();
            }
        }
        if (!origins.keySet().equals(prepared.expected.keySet())) throw conflict();
        var result = new ArrayList<Slot>();
        var expectedSlots = new java.util.HashSet<DocumentCommitParts.Slot>();
        var expectedMembers = new java.util.HashSet<String>();
        for (var member : prepared.command.intent().getMembersList()) {
            String id = member.getMemberId();
            expectedMembers.add(id);
            var selection = physical.selections().get(id);
            if (selection == null || selection < 1) throw conflict();
            for (int ordinal = 0; ordinal < member.getPartsCount(); ordinal++) {
                control.run();
                var part = member.getParts(ordinal);
                if (part.hasEmpty()) continue;
                var slot = new DocumentCommitParts.Slot(id, ordinal);
                expectedSlots.add(slot);
                var object = physical.parts().get(slot);
                if (object == null || object.part() != part.getSlot().getPartValue()
                        || !object.subKey().equals(part.getSlot().getSubKey())) throw conflict();
                if (part.hasUpload()) {
                    if (object.size() != part.getUpload().getSizeBytes() || !object.sha256().equals(part.getUpload().getSha256())) throw conflict();
                    result.add(new Slot(id, ordinal, selection, object.id(), "NEW_CONTENT", null, null));
                } else if (part.hasReuse()) {
                    var reuse = part.getReuse();
                    var origin = origins.get(new Source(DocumentIds.nodeId(reuse.getSource().getAddress()), reuse.getSourceSlot()));
                    if (origin == null || !object.id().toString().equals(reuse.getObject().getObjectId())
                            || object.size() != reuse.getObject().getSizeBytes() || !object.sha256().equals(reuse.getObject().getSha256())) throw conflict();
                    result.add(new Slot(id, ordinal, selection, object.id(), "REUSE", origin.revision(), origin.ordinal()));
                } else if (part.hasHistoricalReuse()) {
                    var historical = part.getHistoricalReuse(); var expected = historical.getObject();
                    if (!object.id().toString().equals(expected.getObjectId()) || object.size() != expected.getSizeBytes()
                            || !object.sha256().equals(expected.getSha256()) || !object.contentType().equals(expected.getContentType())
                            || !object.generation().equals(expected.getBackendGeneration()) || !object.realm().equals(expected.getStorageRealm())
                            || !object.namespace().equals(expected.getNamespace()) || !object.key().equals(expected.getObjectKey())
                            || !java.util.Objects.equals(object.version(), expected.hasProviderVersion() ? expected.getProviderVersion() : null))
                        throw conflict();
                    result.add(new Slot(id, ordinal, selection, object.id(), "HISTORICAL_REUSE",
                            UUID.fromString(historical.getRevisionId()), historical.getRevisionOrdinal(),
                            DocumentIds.nodeId(historical.getSource())));
                } else throw conflict();
            }
        }
        if (result.isEmpty() || result.size() > 10000 || !expectedSlots.equals(physical.parts().keySet())
                || !expectedMembers.equals(physical.selections().keySet())) throw conflict();
        control.run();
        prepared.historical.forEach(source -> source.selectors());
        return List.copyOf(result);
    }
    private static Value text(String value) { return Value.newBuilder().setStringValue(value).build(); }
    private static Batch encode(ListValue.Builder values) {
        try { return new Batch(JsonFormat.printer().omittingInsignificantWhitespace().print(values), values.getValuesCount()); }
        catch (com.google.protobuf.InvalidProtocolBufferException invalid) {
            throw new IllegalArgumentException("Cannot encode assessment source identities", invalid);
        }
    }
    private static DocumentPartAttemptLedger.FenceException conflict() {
        return new DocumentPartAttemptLedger.FenceException("Assessment slots differ from the complete bound candidate and current sources");
    }
}
