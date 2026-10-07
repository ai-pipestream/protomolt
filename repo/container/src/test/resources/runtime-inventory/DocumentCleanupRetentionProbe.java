package ai.protomolt.proto.repo.container.ledger;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

/** Retained native revisions must be excluded from both cleanup entry points. */
public final class DocumentCleanupRetentionProbe {
    public static void run(Tx tx,String generation) {
        List<UUID> retained=tx.readOnly(em -> {
            @SuppressWarnings("unchecked")
            List<UUID> ids=em.createNativeQuery("""
                    SELECT DISTINCT a.attempt_id FROM document_part_attempts a
                    JOIN document_part_attempt_objects o ON o.attempt_id=a.attempt_id
                    JOIN repository_object_references r ON r.object_id=o.physical_object_id
                    WHERE a.backend_generation=:generation AND a.lease_until <= clock_timestamp()
                        AND r.owner_kind IN ('DOCUMENT_HISTORY','DOCUMENT_CURRENT')
                        AND NOT EXISTS(SELECT 1 FROM document_part_publication_history h WHERE h.attempt_id=a.attempt_id)
                    """).setParameter("generation",generation).getResultList();
            return ids;
        });
        if (retained.isEmpty()) throw new AssertionError("No expired native retained attempt exercised cleanup filtering");
        var cleanup=new DocumentAttemptCleanupLedger(tx);
        var candidates=cleanup.candidates(Duration.ZERO,100,generation);
        for (var id:retained) {
            if (candidates.contains(id)) throw new AssertionError("Retained native revision selected for cleanup");
            if (cleanup.claim(id,Duration.ofSeconds(10)).isPresent()) throw new AssertionError("Retained native revision claimed for cleanup");
        }
        long fenced=tx.readOnly(em -> ((Number)em.createNativeQuery("""
                SELECT count(*) FROM document_part_attempt_objects o
                JOIN document_part_attempts a ON a.attempt_id=o.attempt_id
                JOIN repository_object_retention r ON r.object_id=o.physical_object_id
                WHERE a.backend_generation=:generation AND (r.retiring OR r.reclaiming)
                    AND EXISTS(SELECT 1 FROM repository_object_references refs WHERE refs.object_id=r.object_id)
                """).setParameter("generation",generation).getSingleResult()).longValue());
        if (fenced!=0) throw new AssertionError("Retained native object was fenced for reclamation");
        System.out.println("DOCUMENT_CLEANUP_RETENTION_FILTER_OK");
    }
}
