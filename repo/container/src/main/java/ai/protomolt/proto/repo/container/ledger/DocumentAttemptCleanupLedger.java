package ai.protomolt.proto.repo.container.ledger;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/** Internal recovery claims for expired, never-published attempts. No provider I/O. */
final class DocumentAttemptCleanupLedger {
    record Claim(UUID attemptId, UUID token, String generation, ManagedBackendLedger.Profile profile,
            String namespace, List<String> keys) {
        Claim { keys = List.copyOf(keys); }
    }
    private final Tx tx;
    DocumentAttemptCleanupLedger(Tx tx) { this.tx = Objects.requireNonNull(tx); }

    Optional<Claim> claim(UUID id, Duration lease) {
        lease(lease);
        return tx.inTransaction(em -> {
            List<?> owners = em.createNativeQuery("""
                    SELECT backend_generation,storage_namespace,planned_count FROM document_part_attempts
                    WHERE attempt_id=:id FOR UPDATE
                    """).setParameter("id", id).getResultList();
            if (owners.isEmpty()) return Optional.empty();
            // Separate statement after the parent lock: see a publication that committed while we waited.
            boolean ineligible = (Boolean) em.createNativeQuery("""
                    SELECT lease_until > clock_timestamp() OR state='PLANNING'
                        OR EXISTS (SELECT 1 FROM document_part_publication_history WHERE attempt_id=:id)
                        OR EXISTS (SELECT 1 FROM document_part_attempt_objects o
                            JOIN repository_object_references r ON r.object_id=o.physical_object_id
                            WHERE o.attempt_id=:id AND r.owner_kind='ASSESSMENT')
                    FROM document_part_attempts WHERE attempt_id=:id
                    """).setParameter("id", id).getSingleResult();
            if (ineligible) return Optional.empty();
            List<?> busy = em.createNativeQuery("""
                    SELECT cleanup_token FROM document_part_attempt_cleanup
                    WHERE attempt_id=:id AND claim_until > clock_timestamp()
                    """).setParameter("id", id).getResultList();
            if (!busy.isEmpty()) return Optional.empty();
            Object[] owner = (Object[]) owners.getFirst();
            String generation = (String) owner[0];
            var profile = ManagedBackendLedger.find(em, generation)
                    .orElseThrow(() -> new IllegalStateException("Original document backend profile is missing"));
            @SuppressWarnings("unchecked")
            List<String> keys = em.createNativeQuery("""
                    SELECT object_key FROM document_part_attempt_objects WHERE attempt_id=:id ORDER BY ordinal
                    """).setParameter("id", id).getResultList();
            if (keys.size() != ((Number) owner[2]).intValue()) throw new IllegalStateException("Document cleanup plan is incomplete");
            UUID token = UUID.randomUUID();
            em.createNativeQuery("""
                    INSERT INTO document_part_attempt_cleanup(attempt_id,cleanup_token,claim_until,state)
                    VALUES (:id,:token,clock_timestamp()+(:millis * interval '1 millisecond'),'DELETING')
                    ON CONFLICT(attempt_id) DO UPDATE SET cleanup_token=EXCLUDED.cleanup_token,
                        claim_until=EXCLUDED.claim_until,state='DELETING',last_error=NULL,absence_observed_at=NULL
                    """).setParameter("id", id).setParameter("token", token).setParameter("millis", lease.toMillis()).executeUpdate();
            return Optional.of(new Claim(id, token, generation, profile, (String) owner[1], keys));
        });
    }

    boolean renew(Claim claim, Duration lease) {
        lease(lease);
        return tx.inTransaction(em -> {
            lock(em, claim.attemptId());
            return em.createNativeQuery("""
                    UPDATE document_part_attempt_cleanup SET claim_until=GREATEST(claim_until,
                        clock_timestamp()+(:millis * interval '1 millisecond'))
                    WHERE attempt_id=:id AND cleanup_token=:token AND claim_until > clock_timestamp()
                    """).setParameter("id", claim.attemptId()).setParameter("token", claim.token())
                    .setParameter("millis", lease.toMillis()).executeUpdate() == 1;
        });
    }

    /** False means ownership expired or changed; the caller must not report a recorded result. */
    boolean finish(Claim claim, boolean absent, String error) {
        if (absent && error != null) throw new IllegalArgumentException("Absence cannot also report failure");
        if (error != null && error.length() > 2048) throw new IllegalArgumentException("Cleanup error exceeds storage bound");
        return tx.inTransaction(em -> {
            lock(em, claim.attemptId());
            return em.createNativeQuery("""
                    UPDATE document_part_attempt_cleanup SET state=:state,claim_until=clock_timestamp(),last_error=:error,
                        absence_observed_at=CASE WHEN :absent THEN clock_timestamp() ELSE NULL END
                    WHERE attempt_id=:id AND cleanup_token=:token AND claim_until > clock_timestamp()
                    """).setParameter("id", claim.attemptId()).setParameter("token", claim.token())
                    .setParameter("state", absent ? "ABSENT" : "DELETING").setParameter("error", error)
                    .setParameter("absent", absent).executeUpdate() == 1;
        });
    }

    List<UUID> candidates(Duration recheckDelay, int limit) {
        return candidates(recheckDelay, limit, null);
    }

    List<UUID> candidates(Duration recheckDelay, int limit, String generation) {
        if (recheckDelay.isNegative() || recheckDelay.compareTo(Duration.ofDays(1)) > 0 || limit < 1 || limit > 1000)
            throw new IllegalArgumentException("Invalid document cleanup scan bounds");
        return tx.readOnly(em -> {
            @SuppressWarnings("unchecked")
            var query = em.createNativeQuery("""
                    SELECT a.attempt_id FROM document_part_attempts a
                    LEFT JOIN document_part_attempt_cleanup c ON c.attempt_id=a.attempt_id
                    WHERE a.lease_until <= clock_timestamp() AND a.state<>'PLANNING'
                        AND (CAST(:generation AS text) IS NULL OR a.backend_generation=CAST(:generation AS text))
                        AND NOT EXISTS (SELECT 1 FROM document_part_publication_history h WHERE h.attempt_id=a.attempt_id)
                        AND NOT EXISTS (SELECT 1 FROM document_part_attempt_objects o
                            JOIN repository_object_references r ON r.object_id=o.physical_object_id
                            WHERE o.attempt_id=a.attempt_id AND r.owner_kind='ASSESSMENT')
                        AND (c.attempt_id IS NULL OR (c.claim_until <= clock_timestamp()
                            AND c.last_checked_at <= clock_timestamp()-(:delay * interval '1 millisecond')))
                    ORDER BY COALESCE(c.last_checked_at,a.lease_until),a.attempt_id LIMIT :limit
                    """).setParameter("delay", recheckDelay.toMillis()).setParameter("limit", limit)
                    .setParameter("generation", generation);
            List<UUID> ids = query.getResultList();
            return List.copyOf(ids);
        });
    }

    private static void lock(jakarta.persistence.EntityManager em, UUID id) {
        em.createNativeQuery("SELECT attempt_id FROM document_part_attempts WHERE attempt_id=:id FOR UPDATE")
                .setParameter("id", id).getSingleResult();
    }
    private static void lease(Duration duration) {
        if (duration.compareTo(Duration.ofSeconds(1)) < 0 || duration.compareTo(Duration.ofDays(1)) > 0)
            throw new IllegalArgumentException("Cleanup lease must be between one second and one day");
    }
}
