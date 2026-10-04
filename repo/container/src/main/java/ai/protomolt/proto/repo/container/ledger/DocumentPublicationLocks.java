package ai.protomolt.proto.repo.container.ledger;

import jakarta.persistence.EntityManager;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Complete lock set for the existing FULL_REVISION batch path. Caller already
 * holds all document and drive locks. Mixed revisions must additionally include
 * reused origins before this can serve that path. No provider work belongs here.
 */
final class DocumentPublicationLocks {
    private DocumentPublicationLocks() {}
    static final class Origins {
        private final String ids;
        private Origins(Collection<UUID> ids) { this.ids=encode(ids); }
    }

    /** Stabilize current pins, then lock every origin in PostgreSQL UUID order. */
    static Origins lockOrigins(EntityManager em, Set<UUID> destinations, Set<UUID> newAttempts) {
        requireTransaction(em);
        if (destinations.isEmpty() || destinations.size()>64 || newAttempts.size()!=destinations.size())
            throw new IllegalArgumentException("Publication locks require 1 to 64 distinct destinations and attempts");
        var session=em.unwrap(org.hibernate.Session.class);
        var ids=new HashSet<>(newAttempts);
        ids.addAll(session.createNativeQuery("""
                SELECT p.attempt_id FROM unnest(CAST(:nodes AS uuid[])) requested(node_id)
                JOIN document_part_publications p ON p.node_id=requested.node_id
                ORDER BY p.node_id FOR UPDATE OF p
                """,UUID.class).setParameter("nodes",encode(destinations)).getResultList());
        var locked=session.createNativeQuery("""
                SELECT a.attempt_id FROM unnest(CAST(:ids AS uuid[])) requested(attempt_id)
                JOIN document_part_attempts a ON a.attempt_id=requested.attempt_id
                ORDER BY a.attempt_id FOR UPDATE OF a
                """,UUID.class).setParameter("ids",encode(ids)).getResultList();
        if (locked.size()!=ids.size()) throw new DocumentPartAttemptLedger.FenceException("Publication origin is missing");
        return new Origins(locked);
    }

    /** Run after content/lease validation, before the first publication mutation. */
    static void lockRetention(EntityManager em, Origins origins) {
        requireTransaction(em);
        // Return a count rather than transferring every locked physical UUID.
        var counts=(Object[])em.createNativeQuery("""
                WITH objects AS MATERIALIZED (
                    SELECT o.physical_object_id FROM unnest(CAST(:ids AS uuid[])) requested(attempt_id)
                    JOIN document_part_attempt_objects o ON o.attempt_id=requested.attempt_id
                ), locked AS MATERIALIZED (
                    SELECT r.object_id FROM objects o JOIN repository_object_retention r ON r.object_id=o.physical_object_id
                    ORDER BY r.object_id FOR UPDATE OF r
                )
                SELECT (SELECT count(*) FROM objects),(SELECT count(*) FROM locked)
                """).setParameter("ids",origins.ids).getSingleResult();
        if (((Number)counts[0]).longValue()!=((Number)counts[1]).longValue())
            throw new DocumentPartAttemptLedger.FenceException("Publication retention row is missing");
    }
    private static String encode(Collection<UUID> ids) {
        return ids.stream().map(UUID::toString).collect(Collectors.joining(",","{","}"));
    }
    private static void requireTransaction(EntityManager em) {
        if (!em.getTransaction().isActive() || em.getTransaction().getRollbackOnly())
            throw new IllegalStateException("Publication locks require an active writable transaction");
    }
}
