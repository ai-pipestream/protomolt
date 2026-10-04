package ai.protomolt.proto.repo.container.ledger;

import jakarta.persistence.EntityManager;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Lock sets for legacy FULL_REVISION and future independent publication paths.
 * Caller already holds all document/source and drive locks. The independent
 * methods include reused origins but do not enable publication. No provider work belongs here.
 */
final class DocumentPublicationLocks {
    private DocumentPublicationLocks() {}
    static final class Origins {
        private final String ids;
        private Origins(Collection<UUID> ids) { this.ids=encode(ids); }
    }

    /** Transaction-scoped lock evidence, not publication or physical verification authority. */
    static final class IndependentOrigins {
        private final EntityManager manager;
        private final String transaction;
        private final String objects;
        private final int count;
        private final Set<UUID> destinations;
        private final Set<UUID> proposed;
        private final Set<UUID> attempts;
        private boolean retentionLocked;
        private IndependentOrigins(EntityManager manager, String transaction, Set<UUID> objects,
                Set<UUID> destinations, Set<UUID> proposed, Set<UUID> attempts) {
            this.manager = manager; this.transaction = transaction;
            this.objects = encode(objects); this.count = objects.size();
            this.destinations = destinations; this.proposed = proposed; this.attempts = attempts;
        }
        void requirePlan(Set<UUID> destinations, Set<UUID> proposed, Set<UUID> attempts) {
            requireScope(manager);
            if (!retentionLocked) throw new IllegalStateException("Independent retention locks have not been acquired");
            if (!this.destinations.equals(destinations) || !this.proposed.equals(proposed) || !this.attempts.equals(attempts))
                throw new IllegalArgumentException("Independent publication plan differs from locked sets");
        }
        private void requireScope(EntityManager em) {
            requireTransaction(em);
            if (manager != em || !transaction.equals(em.createNativeQuery("SELECT pg_current_xact_id()::text").getSingleResult()))
                throw new IllegalStateException("Independent origin locks belong to another transaction");
        }
    }

    /**
     * Independent path: caller holds the operation, complete document/source and
     * drive lock sets. Stabilize current pointers, then lock the union of old and
     * proposed physical origins. Zero-upload members require no synthetic attempt.
     * Inputs must cover the full command; identity, authorization and verification
     * are separate checks before publication. No provider I/O belongs here.
     */
    static IndependentOrigins lockIndependentOrigins(EntityManager em, Set<UUID> destinations,
            Set<UUID> proposedObjects, Set<UUID> selectedAttempts) {
        requireTransaction(em);
        try {
            if (destinations.isEmpty() || destinations.size() > 64 || proposedObjects.size() > 10000
                    || selectedAttempts.size() > 64)
                throw new IllegalArgumentException("Independent publication lock set exceeds bounds");
            destinations = Set.copyOf(destinations);
            proposedObjects = Set.copyOf(proposedObjects);
            selectedAttempts = Set.copyOf(selectedAttempts);
            if (destinations.isEmpty() || destinations.size() > 64 || proposedObjects.size() > 10000
                    || selectedAttempts.size() > 64)
                throw new IllegalArgumentException("Independent publication lock set exceeds bounds");
            var session = em.unwrap(org.hibernate.Session.class);
            String transaction = (String) em.createNativeQuery("SELECT pg_current_xact_id()::text").getSingleResult();
            var revisions = session.createNativeQuery("""
                    SELECT c.revision_id FROM document_revision_current c
                    WHERE c.node_id=ANY(CAST(:nodes AS uuid[])) ORDER BY c.node_id FOR UPDATE OF c
                    """, UUID.class).setParameter("nodes", encode(destinations)).getResultList();
            var objects = new HashSet<>(proposedObjects);
            objects.addAll(session.createNativeQuery("""
                    SELECT DISTINCT object_id FROM document_revision_parts
                    WHERE revision_id=ANY(CAST(:revisions AS uuid[])) LIMIT 10001
                    """, UUID.class).setParameter("revisions", encode(revisions)).getResultList());
            if (objects.size() > 10000)
                throw new IllegalArgumentException("Independent publication exceeds old and new object bound");
            var locations = session.createNativeQuery("""
                    SELECT object_id,source_kind,source_id FROM repository_physical_locations
                    WHERE object_id=ANY(CAST(:objects AS uuid[]))
                    """, Object[].class).setParameter("objects", encode(objects)).getResultList();
            if (locations.size() != objects.size())
                throw new DocumentPartAttemptLedger.FenceException("Independent publication object is missing");
            var origins = new HashSet<>(selectedAttempts);
            for (var location : locations) {
                if (!"DOCUMENT_PART".equals(location[1]))
                    throw new DocumentPartAttemptLedger.FenceException("Independent document reference needs a document origin");
                origins.add((UUID) location[2]);
            }
            var locked = session.createNativeQuery("""
                    SELECT attempt_id FROM document_part_attempts
                    WHERE attempt_id=ANY(CAST(:origins AS uuid[])) ORDER BY attempt_id FOR UPDATE
                    """, UUID.class).setParameter("origins", encode(origins)).getResultList();
            if (locked.size() != origins.size())
                throw new DocumentPartAttemptLedger.FenceException("Independent publication origin is missing");
            return new IndependentOrigins(em, transaction, objects, destinations, proposedObjects, selectedAttempts);
        } catch (RuntimeException | Error failure) {
            rollbackOnly(em, failure);
            throw failure;
        }
    }

    /** Locks exact old/new objects after origin validation, before the first reference mutation. */
    static void lockIndependentRetention(EntityManager em, IndependentOrigins origins) {
        requireTransaction(em);
        try {
            java.util.Objects.requireNonNull(origins, "origins").requireScope(em);
            var counts = (Object[]) em.createNativeQuery("""
                    WITH locked AS MATERIALIZED (
                        SELECT object_id,retiring,reclaiming FROM repository_object_retention
                        WHERE object_id=ANY(CAST(:objects AS uuid[])) ORDER BY object_id FOR UPDATE
                    ) SELECT count(*),COALESCE(bool_or(retiring OR reclaiming),false) FROM locked
                    """).setParameter("objects", origins.objects).getSingleResult();
            if (((Number) counts[0]).intValue() != origins.count || Boolean.TRUE.equals(counts[1]))
                throw new DocumentPartAttemptLedger.FenceException("Independent retention object is missing or retiring");
            origins.retentionLocked = true;
        } catch (RuntimeException | Error failure) {
            rollbackOnly(em, failure);
            throw failure;
        }
    }

    private static void rollbackOnly(EntityManager em, Throwable failure) {
        try { em.getTransaction().setRollbackOnly(); }
        catch (RuntimeException rollbackFailure) { failure.addSuppressed(rollbackFailure); }
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
