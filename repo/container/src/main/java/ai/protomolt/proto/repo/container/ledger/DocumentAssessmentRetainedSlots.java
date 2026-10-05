package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.container.blob.DocumentIds;
import ai.protomolt.proto.repo.spi.DocumentPublicationCommand;
import jakarta.persistence.EntityManager;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

/** Original staged associations; never follows current selection or source pointers. */
final class DocumentAssessmentRetainedSlots {
    private DocumentAssessmentRetainedSlots() {}

    /** Durable selection identity; an upload lease token is not acknowledgement authority. */
    record UploadSelection(String member, long revision, UUID attempt) {
        UploadSelection {
            if (member == null || !member.matches("[A-Za-z0-9][A-Za-z0-9._-]{0,127}") || revision < 1 || attempt == null)
                throw new IllegalArgumentException("Invalid retained upload selection");
        }
    }
    static Map<String,UploadSelection> uploadSelections(Map<String,DocumentSelectedAttemptLedger.Selected> selected) {
        return selected.entrySet().stream().collect(Collectors.toUnmodifiableMap(Map.Entry::getKey,
                entry -> new UploadSelection(entry.getValue().member(), entry.getValue().revision(), entry.getValue().attempt())));
    }

    /** Original immutable selection history for a receipt-authorized read, never the current pointer. */
    static Map<String,UploadSelection> retainedSelections(EntityManager em, DocumentAssessmentSlotSnapshot.Identity identity,
            Runnable control) {
        control.run();
        var rows = em.createNativeQuery("""
                SELECT DISTINCT s.member_id,s.selection_revision,h.attempt_id
                FROM document_assessment_slots s JOIN document_operation_selection_attempts h
                  ON h.account_id=:account AND h.principal=:principal AND h.operation_id=:op AND h.owner_generation=:generation
                  AND h.member_id=s.member_id AND h.selection_revision=s.selection_revision
                WHERE s.assessment_id=:id AND h.attempt_id IS NOT NULL LIMIT 65
                """).setParameter("account", identity.key().account()).setParameter("principal", identity.key().principal())
                .setParameter("op", identity.key().operationId()).setParameter("generation", identity.generation())
                .setParameter("id", identity.assessment()).getResultList();
        if (rows.size() > 64) throw conflict();
        var selections = new HashMap<String,UploadSelection>();
        for (var value : rows) {
            control.run(); var row = (Object[]) value;
            var selection = new UploadSelection((String) row[0], ((Number) row[1]).longValue(), (UUID) row[2]);
            if (selections.put(selection.member(), selection) != null) throw conflict();
        }
        return Map.copyOf(selections);
    }

    /**
     * Caller holds the live operation fence or verified terminal receipt, complete
     * current read-authorization locks and retained owner lock, in that order. This checks storage identity,
     * not authorization, runtime evidence, expiry, or permission to publish.
     */
    static void verify(EntityManager em, DocumentAssessmentSlotSnapshot.Identity identity,
            DocumentPublicationCommand command, Map<String,UploadSelection> selections,
            PayloadBudget budget, Runnable control) {
        var selected = Map.copyOf(selections);
        var members = command.intent().getMembersList().stream().collect(Collectors.toMap(m -> m.getMemberId(), m -> m));
        var uploadCounts = command.intent().getMembersList().stream().collect(Collectors.toMap(m -> m.getMemberId(),
                m -> m.getPartsList().stream().filter(p -> p.hasUpload()).count()));
        var uploading = uploadCounts.entrySet().stream().filter(e -> e.getValue() > 0).map(Map.Entry::getKey).collect(Collectors.toSet());
        if (!selected.keySet().equals(uploading) || !command.sha256().equals(identity.commandSha256())
                || !command.operationId().equals(identity.key().operationId())
                || !command.intent().getAccountId().equals(identity.key().account())) throw conflict();
        control.run();
        var rows = em.createNativeQuery("""
                SELECT s.member_id,s.revision_ordinal,s.selection_revision,s.object_id,s.declaration,s.source_revision,s.source_ordinal,
                    h.attempt_id,o.attempt_id,o.revision_ordinal,o.part,o.sub_key,o.expected_size,o.expected_sha256,o.content_type,
                    l.object_key,l.backend_generation,l.storage_realm,l.storage_namespace,o.verified,
                    EXISTS(SELECT 1 FROM document_assessment_objects x WHERE x.assessment_id=s.assessment_id AND x.object_id=s.object_id),
                    EXISTS(SELECT 1 FROM repository_object_references r WHERE r.object_id=s.object_id
                        AND r.owner_kind='ASSESSMENT' AND r.owner_id=s.assessment_id AND r.owner_revision=1),o.provider_version,
                    a.operation_principal=:principal AND a.operation_id=:operation AND a.operation_generation=:generation
                        AND a.member_id=s.member_id AND a.plan_kind='NEW_CONTENT'
                        AND a.backend_generation=anchor.backend_generation AND a.storage_realm=anchor.storage_realm
                        AND a.storage_namespace=anchor.storage_namespace,
                    anchor.drive_id,anchor.node_id,anchor.sampled_revision,anchor.upload_count
                FROM document_assessment_slots s
                JOIN document_operation_selection_attempts h ON h.account_id=:account AND h.principal=:principal
                    AND h.operation_id=:operation AND h.owner_generation=:generation
                    AND h.member_id=s.member_id AND h.selection_revision=s.selection_revision
                JOIN document_operation_selections anchor ON anchor.account_id=h.account_id AND anchor.principal=h.principal
                    AND anchor.operation_id=h.operation_id AND anchor.owner_generation=h.owner_generation AND anchor.member_id=h.member_id
                JOIN repository_physical_locations l ON l.object_id=s.object_id AND l.source_kind='DOCUMENT_PART'
                JOIN document_part_attempt_objects o ON o.attempt_id=l.source_id AND o.ordinal=l.source_ordinal
                    AND o.physical_object_id=l.object_id
                    AND o.object_key=l.object_key AND o.storage_realm=l.storage_realm AND o.storage_namespace=l.storage_namespace
                JOIN document_part_attempts a ON a.attempt_id=o.attempt_id AND a.account_id=:account
                    AND a.backend_generation=l.backend_generation AND a.storage_realm=l.storage_realm AND a.storage_namespace=l.storage_namespace
                WHERE s.assessment_id=:id LIMIT 10001
                """).setParameter("account", identity.key().account()).setParameter("principal", identity.key().principal())
                .setParameter("operation", identity.key().operationId()).setParameter("generation", identity.generation())
                .setParameter("id", identity.assessment()).getResultList();
        if (rows.isEmpty() || rows.size() > 10000) throw conflict();
        var slots = new ArrayList<DocumentAssessmentSlots.Slot>();
        var actualKeys = new HashSet<DocumentCommitParts.Slot>();
        var objects = new HashSet<UUID>();
        var memberSelections = new HashMap<String, Long>();
        for (Object value : rows) {
            control.run(); var row = (Object[]) value;
            String memberId = (String) row[0]; int ordinal = ((Number) row[1]).intValue();
            var member = members.get(memberId);
            if (member == null || ordinal < 0 || ordinal >= member.getPartsCount()
                    || !UUID.fromString(member.getDriveId()).equals(row[24])
                    || !DocumentIds.nodeId(member.getDestination().getAddress()).equals(row[25])
                    || member.getDestination().getExpectedMutationRevision() != ((Number) row[26]).longValue()
                    || uploadCounts.get(memberId) != ((Number) row[27]).longValue()) throw conflict();
            var key = new DocumentCommitParts.Slot(memberId, ordinal);
            long revision = ((Number) row[2]).longValue();
            var previous = memberSelections.putIfAbsent(memberId, revision);
            if (!actualKeys.add(key) || previous != null && previous != revision) throw conflict();
            var part = member.getParts(ordinal);
            if (part.hasEmpty() || ((Number) row[10]).intValue() != part.getSlot().getPartValue()
                    || !row[11].equals(part.getSlot().getSubKey()) || !Boolean.TRUE.equals(row[19])
                    || !Boolean.TRUE.equals(row[20]) || !Boolean.TRUE.equals(row[21])) throw conflict();
            if (uploading.contains(memberId)) {
                var selection = selected.get(memberId);
                if (!selection.member().equals(memberId) || selection.revision() != revision
                        || !selection.attempt().equals(row[7])) throw conflict();
            } else if (row[7] != null) throw conflict();
            if (part.hasUpload()) {
                var upload = part.getUpload();
                if (!"NEW_CONTENT".equals(row[4]) || row[5] != null || row[6] != null || !row[7].equals(row[8])
                        || ordinal != ((Number) row[9]).intValue() || !Boolean.TRUE.equals(row[23])
                        || upload.getSizeBytes() != ((Number) row[12]).longValue() || !upload.getSha256().equals(row[13])
                        || !upload.getContentType().equals(row[14])) throw conflict();
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
        for (var member : command.intent().getMembersList()) for (int i = 0; i < member.getPartsCount(); i++) {
            control.run();
            if (!member.getParts(i).hasEmpty()) expectedKeys.add(new DocumentCommitParts.Slot(member.getMemberId(), i));
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
