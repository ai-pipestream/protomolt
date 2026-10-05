package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import jakarta.persistence.EntityManager;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

/** Original staged associations; never follows current selection or source pointers. */
final class DocumentAssessmentRetainedSlots {
    private DocumentAssessmentRetainedSlots() {}

    /**
     * Caller holds the live operation fence, complete current read-authorization
     * locks and retained owner lock, in that order. This checks storage identity,
     * not authorization, runtime evidence, expiry, or permission to publish.
     */
    static void verify(EntityManager em, DocumentAssessmentSlotSnapshot.Identity identity,
            DocumentUploadPlan.Prepared plan, Map<String,DocumentSelectedAttemptLedger.Selected> selections,
            PayloadBudget budget, Runnable control) {
        var selected = Map.copyOf(selections);
        var members = plan.members().stream().collect(Collectors.toMap(m -> m.intent().getMemberId(), m -> m));
        var uploading = plan.members().stream().filter(m -> m.attempt().isPresent())
                .map(m -> m.intent().getMemberId()).collect(Collectors.toSet());
        if (!selected.keySet().equals(uploading) || !plan.command().sha256().equals(identity.commandSha256())
                || !plan.command().operationId().equals(identity.key().operationId())
                || !plan.command().intent().getAccountId().equals(identity.key().account())) throw conflict();
        control.run();
        var rows = em.createNativeQuery("""
                SELECT s.member_id,s.revision_ordinal,s.selection_revision,s.object_id,s.declaration,s.source_revision,s.source_ordinal,
                    h.attempt_id,o.attempt_id,o.revision_ordinal,o.part,o.sub_key,o.expected_size,o.expected_sha256,o.content_type,
                    l.object_key,l.backend_generation,l.storage_realm,l.storage_namespace,o.verified,
                    EXISTS(SELECT 1 FROM document_assessment_objects x WHERE x.assessment_id=s.assessment_id AND x.object_id=s.object_id),
                    EXISTS(SELECT 1 FROM repository_object_references r WHERE r.object_id=s.object_id
                        AND r.owner_kind='ASSESSMENT' AND r.owner_id=s.assessment_id AND r.owner_revision=1),o.provider_version
                FROM document_assessment_slots s
                JOIN document_operation_selection_attempts h ON h.account_id=:account AND h.principal=:principal
                    AND h.operation_id=:operation AND h.owner_generation=:generation
                    AND h.member_id=s.member_id AND h.selection_revision=s.selection_revision
                JOIN repository_physical_locations l ON l.object_id=s.object_id AND l.source_kind='DOCUMENT_PART'
                JOIN document_part_attempt_objects o ON o.attempt_id=l.source_id AND o.ordinal=l.source_ordinal
                    AND o.physical_object_id=l.object_id
                WHERE s.assessment_id=:id LIMIT 10001
                """).setParameter("account", identity.key().account()).setParameter("principal", identity.key().principal())
                .setParameter("operation", identity.key().operationId()).setParameter("generation", identity.generation())
                .setParameter("id", identity.assessment()).getResultList();
        if (rows.isEmpty() || rows.size() > 10000) throw conflict();
        var slots = new ArrayList<DocumentAssessmentSlots.Slot>();
        var actualKeys = new HashSet<DocumentCommitParts.Slot>();
        var objects = new HashSet<UUID>();
        var memberSelections = new HashMap<String, Long>();
        var uploads = new HashMap<DocumentCommitParts.Slot, DocumentPartAttemptLedger.PlannedObject>();
        for (var member : plan.members()) member.attempt().ifPresent(attempt -> {
            for (var upload : attempt.uploads()) uploads.put(new DocumentCommitParts.Slot(member.intent().getMemberId(), upload.revisionOrdinal()), upload.object());
        });
        for (Object value : rows) {
            control.run(); var row = (Object[]) value;
            String memberId = (String) row[0]; int ordinal = ((Number) row[1]).intValue();
            var member = members.get(memberId);
            if (member == null || ordinal < 0 || ordinal >= member.intent().getPartsCount()) throw conflict();
            var key = new DocumentCommitParts.Slot(memberId, ordinal);
            long revision = ((Number) row[2]).longValue();
            var previous = memberSelections.putIfAbsent(memberId, revision);
            if (!actualKeys.add(key) || previous != null && previous != revision) throw conflict();
            var part = member.intent().getParts(ordinal);
            if (part.hasEmpty() || ((Number) row[10]).intValue() != part.getSlot().getPartValue()
                    || !row[11].equals(part.getSlot().getSubKey()) || !Boolean.TRUE.equals(row[19])
                    || !Boolean.TRUE.equals(row[20]) || !Boolean.TRUE.equals(row[21])) throw conflict();
            if (member.attempt().isPresent()) {
                var selection = selected.get(memberId);
                if (!selection.member().equals(memberId) || selection.revision() != revision
                        || !selection.attempt().equals(row[7]) || !member.attempt().orElseThrow().id().equals(row[7])) throw conflict();
            } else if (row[7] != null) throw conflict();
            if (part.hasUpload()) {
                var upload = uploads.get(key);
                if (!"NEW_CONTENT".equals(row[4]) || row[5] != null || row[6] != null || !row[7].equals(row[8])
                        || ordinal != ((Number) row[9]).intValue() || upload == null
                        || upload.size() != ((Number) row[12]).longValue() || !upload.sha256().equals(row[13])
                        || !upload.contentType().equals(row[14]) || !upload.objectKey().equals(row[15])
                        || !member.placement().generation().equals(row[16])
                        || !member.placement().profile().storageRealm().equals(row[17])
                        || !member.placement().drive().namespace().equals(row[18])) throw conflict();
            } else if (part.hasReuse()) {
                var object = part.getReuse().getObject();
                if (!"REUSE".equals(row[4]) || row[5] == null || row[6] == null
                        || !UUID.fromString(object.getObjectId()).equals(row[3])
                        || object.getSizeBytes() != ((Number) row[12]).longValue() || !object.getSha256().equals(row[13])
                        || !object.getContentType().equals(row[14]) || !object.getObjectKey().equals(row[15])
                        || !object.getBackendGeneration().equals(row[16]) || !object.getStorageRealm().equals(row[17])
                        || !object.getNamespace().equals(row[18])
                        || !java.util.Objects.equals(object.hasProviderVersion() ? object.getProviderVersion() : null, row[22])) throw conflict();
            } else throw conflict();
            objects.add((UUID) row[3]);
            slots.add(new DocumentAssessmentSlots.Slot(memberId, ordinal, revision, (UUID) row[3], (String) row[4],
                    (UUID) row[5], row[6] == null ? null : ((Number) row[6]).intValue()));
        }
        var expectedKeys = new HashSet<DocumentCommitParts.Slot>();
        for (var member : plan.members()) for (int i = 0; i < member.intent().getPartsCount(); i++) {
            control.run();
            if (!member.intent().getParts(i).hasEmpty()) expectedKeys.add(new DocumentCommitParts.Slot(member.intent().getMemberId(), i));
        }
        if (!expectedKeys.equals(actualKeys)) throw conflict();
        // Check base counts too: joins must not hide orphaned or extraneous associations.
        Object[] counts = (Object[]) em.createNativeQuery("""
                SELECT (SELECT count(*) FROM document_assessment_slots WHERE assessment_id=:id),
                    (SELECT count(*) FROM document_assessment_objects WHERE assessment_id=:id),
                    (SELECT count(*) FROM repository_object_references WHERE owner_kind='ASSESSMENT' AND owner_id=:id)
                """).setParameter("id", identity.assessment()).getSingleResult();
        if (((Number) counts[0]).longValue() != slots.size() || ((Number) counts[1]).longValue() != objects.size()
                || ((Number) counts[2]).longValue() != objects.size()) throw conflict();
        try (var readBudget = budget.reserve(2L * DocumentAssessmentSlotSnapshot.MAX_BYTES);
             var expected = DocumentAssessmentSlotSnapshot.encode(identity, slots, budget, control)) {
            var snapshots = em.createNativeQuery("""
                    SELECT snapshot_codec,snapshot_version,
                        CASE WHEN octet_length(snapshot_bytes)<=4194304 THEN snapshot_bytes END,encode(snapshot_sha256,'hex')
                    FROM document_assessment_slot_snapshots WHERE assessment_id=:id
                    """).setParameter("id", identity.assessment()).getResultList();
            if (snapshots.size() != 1) throw conflict();
            var row = (Object[]) snapshots.getFirst();
            if (!DocumentAssessmentSlotSnapshot.CODEC.equals(row[0]) || ((Number) row[1]).intValue() != DocumentAssessmentSlotSnapshot.VERSION
                    || !(row[2] instanceof byte[] stored) || !expected.bytes().asReadOnlyByteBuffer().equals(ByteBuffer.wrap(stored))
                    || !expected.sha256().equals(row[3])) throw conflict();
            control.run();
        }
    }

    private static IllegalStateException conflict() { return new IllegalStateException("Retained assessment slots differ from original staging identity"); }
}
