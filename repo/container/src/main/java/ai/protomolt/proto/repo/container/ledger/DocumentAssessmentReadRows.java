package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.v1.DocumentPart;
import jakarta.persistence.EntityManager;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

/** Complete physical-set capture; caller holds reader, operation, authorization and assessment locks. */
final class DocumentAssessmentReadRows {
    private DocumentAssessmentReadRows() {}

    /** Slot/snapshot/evidence verification must still succeed in this same transaction. */
    static List<DocumentAssessmentReadPlan.Entry> capture(EntityManager em, UUID assessment, Runnable control) {
        control.run();
        int objects = ((Number) em.createNativeQuery("""
                SELECT share_repository_retention_set(ARRAY(
                    SELECT object_id FROM document_assessment_objects WHERE assessment_id=:id LIMIT 10001))
                """).setParameter("id", assessment).getSingleResult()).intValue();
        if (objects < 1) throw invalid();
        // All physical locks are held before these fresh READ COMMITTED observations.
        // Retiring is allowed for this existing owner; reclaiming is never readable.
        var rows = em.unwrap(org.hibernate.Session.class).createNativeQuery("""
                SELECT s.member_id,s.revision_ordinal,s.object_id,o.part,o.sub_key,
                    l.object_key,o.expected_size,o.expected_sha256,o.provider_version,o.etag,o.content_type,
                    l.backend_generation,l.storage_realm,l.storage_namespace
                FROM document_assessment_slots s
                JOIN document_assessment_objects n ON n.assessment_id=s.assessment_id AND n.object_id=s.object_id
                JOIN repository_object_references r ON r.object_id=s.object_id AND r.owner_kind='ASSESSMENT'
                    AND r.owner_id=s.assessment_id AND r.owner_revision=1
                JOIN repository_object_retention t ON t.object_id=s.object_id AND NOT t.reclaiming
                JOIN repository_physical_locations l ON l.object_id=s.object_id AND l.source_kind='DOCUMENT_PART'
                JOIN document_part_attempt_objects o ON o.physical_object_id=l.object_id
                    AND o.attempt_id=l.source_id AND o.ordinal=l.source_ordinal AND o.verified
                    AND o.object_key=l.object_key AND o.storage_realm=l.storage_realm AND o.storage_namespace=l.storage_namespace
                JOIN document_part_attempts a ON a.attempt_id=o.attempt_id AND a.state='VERIFIED'
                    AND a.backend_generation=l.backend_generation AND a.storage_realm=l.storage_realm
                    AND a.storage_namespace=l.storage_namespace
                WHERE s.assessment_id=:id ORDER BY s.member_id COLLATE "C",s.revision_ordinal LIMIT 10001
                """, Object[].class).setParameter("id", assessment).getResultList();
        long slots = ((Number) em.createNativeQuery("SELECT count(*) FROM document_assessment_slots WHERE assessment_id=:id")
                .setParameter("id", assessment).getSingleResult()).longValue();
        if (rows.isEmpty() || rows.size() > 10000 || rows.size() != slots) throw invalid();
        if (rows.stream().map(row -> row[2]).distinct().count() != objects) throw invalid();
        var profiles = ManagedBackendLedger.requireAll(em,
                rows.stream().map(row -> (String) row[11]).collect(Collectors.toSet()));
        var entries = new ArrayList<DocumentAssessmentReadPlan.Entry>(rows.size());
        for (Object[] row : rows) {
            control.run();
            var profile = profiles.get((String) row[11]);
            if (!profile.storageRealm().equals(row[12])) throw invalid();
            var part = new DocumentPublicationLedger.Part(DocumentPart.forNumber(((Number) row[3]).intValue()),
                    (String) row[4], (String) row[5], ((Number) row[6]).longValue(), (String) row[7],
                    (String) row[8], (String) row[9], (String) row[10]);
            entries.add(new DocumentAssessmentReadPlan.Entry((String) row[0], ((Number) row[1]).intValue(), (UUID) row[2],
                    new DocumentPublicationLedger.BoundPart(part,
                            new DocumentPublicationLedger.Binding((String) row[11], profile, (String) row[13]))));
        }
        return List.copyOf(entries);
    }

    private static IllegalStateException invalid() {
        return new IllegalStateException("Assessment physical read set is incomplete or unavailable");
    }
}
