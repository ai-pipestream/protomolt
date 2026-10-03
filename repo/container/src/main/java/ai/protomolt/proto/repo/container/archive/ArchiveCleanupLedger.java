package ai.protomolt.proto.repo.container.archive;

import ai.protomolt.proto.repo.container.ledger.Tx;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/** Durable reclamation accounting. The caller performs provider I/O outside SQL. */
public final class ArchiveCleanupLedger {
    private final Tx tx;
    public ArchiveCleanupLedger(Tx tx) { this.tx = Objects.requireNonNull(tx); }
    public record Claim(ArchiveObjectLedger.Binding binding, UUID token) {}

    /**
     * A cutoff schedules work, not cancellation. Expired uploads can still finish
     * provider I/O; DELETED tombstones therefore remain reconciliation candidates.
     */
    public Optional<Claim> claim(UUID objectId, Instant inactiveBefore) {
        return claim(objectId, inactiveBefore, inactiveBefore);
    }

    /** Separate failed-work retry age from abandonment of an in-flight claim. */
    public Optional<Claim> claim(UUID objectId, Instant inactiveBefore, Instant abandonedBefore) {
        Objects.requireNonNull(inactiveBefore);
        Objects.requireNonNull(abandonedBefore);
        return tx.inTransaction(em -> {
            var upload = ArchiveUploadLedger.lock(em, objectId);
            Instant now = em.unwrap(org.hibernate.Session.class)
                    .createNativeQuery("SELECT clock_timestamp()", Instant.class).getSingleResult();
            Number eligible = (Number) em.createNativeQuery("""
                    SELECT count(*) FROM archive_object_uploads WHERE object_id=:id AND updated_at<=:cutoff
                    AND (state<>'DELETING' OR cleanup_error IS NOT NULL OR updated_at<=:abandoned)
                    AND NOT EXISTS (SELECT 1 FROM archive_version_object_refs WHERE object_id=:id)
                    """).setParameter("id", objectId).setParameter("cutoff", inactiveBefore)
                    .setParameter("abandoned", abandonedBefore).getSingleResult();
            if (eligible.longValue() == 0 || (("STAGING".equals(upload.state()) || "VERIFIED".equals(upload.state()))
                    && upload.leaseUntil().isAfter(now))) return Optional.empty();
            UUID token = UUID.randomUUID();
            em.createNativeQuery("""
                    UPDATE archive_object_uploads SET state='DELETING',cleanup_token=:token,
                    cleanup_attempts=cleanup_attempts+1,cleanup_error=NULL WHERE object_id=:id
                    """).setParameter("id", objectId).setParameter("token", token).executeUpdate();
            return Optional.of(new Claim(ArchiveObjectLedger.find(em, objectId).orElseThrow(), token));
        });
    }

    public boolean succeeded(UUID objectId, UUID token) { return finish(objectId, token, null); }
    public boolean failed(UUID objectId, UUID token, String error) {
        if (error == null || !error.matches("[A-Z][A-Z0-9_]{0,127}"))
            throw new IllegalArgumentException("Cleanup error must be a bounded diagnostic code");
        return finish(objectId, token, error);
    }

    private boolean finish(UUID objectId, UUID token, String error) {
        Objects.requireNonNull(objectId);
        Objects.requireNonNull(token);
        return tx.inTransaction(em -> em.createNativeQuery("""
                UPDATE archive_object_uploads SET state=:state,cleanup_error=:error
                WHERE object_id=:id AND state='DELETING' AND cleanup_token=:token
                """).setParameter("state", error == null ? "DELETED" : "DELETING")
                .setParameter("error", error).setParameter("id", objectId).setParameter("token", token).executeUpdate() == 1);
    }

    public List<UUID> candidates(Instant inactiveBefore, int limit) {
        Objects.requireNonNull(inactiveBefore);
        if (limit < 1 || limit > 1000) throw new IllegalArgumentException("Cleanup limit must be between 1 and 1000");
        return tx.readOnly(em -> em.unwrap(org.hibernate.Session.class).createNativeQuery("""
                SELECT u.object_id FROM archive_object_uploads u WHERE u.updated_at<=:cutoff
                AND (u.state NOT IN ('STAGING','VERIFIED') OR u.lease_until<=clock_timestamp())
                AND NOT EXISTS (SELECT 1 FROM archive_version_object_refs r WHERE r.object_id=u.object_id)
                ORDER BY u.updated_at,u.object_id
                """, UUID.class).setParameter("cutoff", inactiveBefore).setMaxResults(limit).getResultList());
    }

    /** Fast lane for admitted logical mutations; completed tombstones use the aged scan. */
    public List<UUID> mutationCandidates(Instant inactiveBefore, Instant abandonedBefore, int limit) {
        Objects.requireNonNull(inactiveBefore);
        Objects.requireNonNull(abandonedBefore);
        if (limit < 1 || limit > 1000) throw new IllegalArgumentException("Cleanup limit must be between 1 and 1000");
        return tx.readOnly(em -> em.unwrap(org.hibernate.Session.class).createNativeQuery("""
                SELECT u.object_id FROM archive_object_uploads u WHERE u.updated_at<=:cutoff
                AND u.state IN ('LIVE','DELETING')
                AND (u.state<>'DELETING' OR u.cleanup_error IS NOT NULL OR u.updated_at<=:abandoned)
                AND EXISTS (SELECT 1 FROM archive_mutation_targets t WHERE t.object_id=u.object_id)
                AND NOT EXISTS (SELECT 1 FROM archive_version_object_refs r WHERE r.object_id=u.object_id)
                ORDER BY u.updated_at,u.object_id
                """, UUID.class).setParameter("cutoff", inactiveBefore).setParameter("abandoned", abandonedBefore)
                .setMaxResults(limit).getResultList());
    }
}
