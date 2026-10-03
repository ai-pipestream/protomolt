package ai.protomolt.proto.repo.container.ledger;

import jakarta.persistence.EntityManager;
import jakarta.persistence.LockModeType;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeSet;
import java.util.UUID;

/**
 * Managed raw-object publication and cleanup coordination. This is an internal
 * ledger API: callers must authorize documents and verify their actual payload
 * references before binding. A UUID or storage coordinate is not an access grant.
 * No provider I/O runs in a database transaction.
 */
public final class RawObjectLedger {
    private final Tx tx;

    public RawObjectLedger(Tx tx) { this.tx = Objects.requireNonNull(tx, "tx"); }

    /** Immutable physical coordinates; backendIdentity names configuration, never secrets. */
    public record Location(String accountId, String backendIdentity, UUID driveId,
            String driveName, String bucket, String objectKey) {
        public Location {
            requireText(accountId, "accountId");
            requireText(backendIdentity, "backendIdentity");
            Objects.requireNonNull(driveId, "driveId");
            requireText(driveName, "driveName");
            requireText(bucket, "bucket");
            requireText(objectKey, "objectKey");
        }
    }

    /** A token is required for first publication; null is allowed only for a LIVE object. */
    public record Binding(UUID rawId, UUID leaseToken) {
        public Binding { Objects.requireNonNull(rawId, "rawId"); }
    }

    public static final class FenceException extends RuntimeException {
        public FenceException(String message) { super(message); }
    }

    /** Persist before starting PUT. A duplicate location is an error, never an overwrite. */
    public RawObjectRecord begin(Location location, long expectedSize, String contentType, Duration lease) {
        Objects.requireNonNull(location, "location");
        requireText(contentType, "contentType");
        requireLease(lease);
        if (expectedSize < 0) throw new IllegalArgumentException("expectedSize must be nonnegative");
        return tx.inTransaction(em -> {
            Instant now = databaseNow(em);
            var row = new RawObjectRecord();
            row.rawId = UUID.randomUUID();
            row.accountId = location.accountId();
            row.backendIdentity = location.backendIdentity();
            row.driveId = location.driveId();
            row.driveName = location.driveName();
            row.bucket = location.bucket();
            row.objectKey = location.objectKey();
            row.expectedSize = expectedSize;
            row.contentType = contentType;
            row.state = RawObjectRecord.STAGING;
            row.leaseToken = UUID.randomUUID();
            row.leaseUntil = now.plus(lease);
            row.createdAt = now;
            row.updatedAt = now;
            em.persist(row);
            return row;
        });
    }

    /** Heartbeats never resurrect an expired lease, even if cleanup has not run yet. */
    public RawObjectRecord renew(UUID id, UUID token, Duration lease) {
        requireLease(lease);
        return tx.inTransaction(em -> {
            var row = lock(em, id);
            Instant now = databaseNow(em);
            requireLeaseOwner(row, token, now);
            row.leaseUntil = now.plus(lease);
            row.updatedAt = now;
            return row;
        });
    }

    /** Record the computed digest only after a successful complete provider write. */
    public RawObjectRecord verify(UUID id, UUID token, long actualSize, String sha256,
            String providerVersion, String etag) {
        if (sha256 == null || !sha256.matches("[0-9a-f]{64}"))
            throw new IllegalArgumentException("sha256 must be 64 lowercase hexadecimal characters");
        return tx.inTransaction(em -> {
            var row = lock(em, id);
            Instant now = databaseNow(em);
            requireLeaseOwner(row, token, now);
            if (actualSize != row.expectedSize) throw new FenceException("Upload length differs from admission");
            if (RawObjectRecord.VERIFIED.equals(row.state)) {
                if (!sha256.equals(row.sha256) || !Objects.equals(providerVersion, row.providerVersion)
                        || !Objects.equals(etag, row.etag))
                    throw new FenceException("Verified upload identity differs");
                return row;
            }
            row.sha256 = sha256;
            row.providerVersion = providerVersion;
            row.etag = etag;
            row.state = RawObjectRecord.VERIFIED;
            row.updatedAt = now;
            return row;
        });
    }

    public Optional<RawObjectRecord> find(UUID id) {
        return tx.readOnly(em -> Optional.ofNullable(em.find(RawObjectRecord.class, id)));
    }

    public List<UUID> references(UUID nodeId) {
        return tx.readOnly(em -> references(em, nodeId));
    }

    /**
     * Replace references in the document publication transaction. Caller has already
     * locked all document identities (sources included) in their canonical order.
     * Locks raw identities in sorted order after documents. Cleanup never acquires
     * document locks. All publication/copy paths must use this method, not direct
     * SQL reference insertion. Empty bindings explicitly release previous references.
     */
    public void replaceReferences(EntityManager em, DocumentRecord document, Collection<Binding> bindings) {
        if (!em.getTransaction().isActive() || !em.contains(document))
            throw new IllegalArgumentException("Binding requires the managed document publication transaction");
        em.lock(document, LockModeType.PESSIMISTIC_WRITE);
        if (!DocumentStatus.AVAILABLE.equals(document.status))
            throw new FenceException("Cannot bind raw content to an unavailable document");
        Map<UUID, Binding> desired = new HashMap<>();
        for (Binding binding : bindings) {
            if (desired.put(binding.rawId(), binding) != null)
                throw new IllegalArgumentException("Duplicate raw binding");
        }
        List<UUID> previous = references(em, document.nodeId);
        var ids = new TreeSet<>(previous);
        ids.addAll(desired.keySet());
        Map<UUID, RawObjectRecord> locked = new HashMap<>();
        for (UUID id : ids) locked.put(id, lock(em, id));
        Instant now = databaseNow(em);
        for (Binding binding : desired.values()) {
            var row = locked.get(binding.rawId());
            if (!row.accountId.equals(document.accountId))
                throw new FenceException("Raw content belongs to another account");
            if (!RawObjectRecord.LIVE.equals(row.state)) {
                requireLeaseOwner(row, binding.leaseToken(), now);
                if (!RawObjectRecord.VERIFIED.equals(row.state))
                    throw new FenceException("Raw content has not been verified");
                row.state = RawObjectRecord.LIVE;
            }
            row.updatedAt = now;
        }
        // Validate every binding before removing any reference. The surrounding
        // transaction also rolls back document publication on any failure here.
        em.flush();
        em.createNativeQuery("DELETE FROM document_raw_refs WHERE node_id = :node")
                .setParameter("node", document.nodeId).executeUpdate();
        for (UUID id : desired.keySet()) {
            em.createNativeQuery("INSERT INTO document_raw_refs(node_id, raw_id) VALUES (:node, :raw)")
                    .setParameter("node", document.nodeId).setParameter("raw", id).executeUpdate();
        }
        for (UUID id : previous) locked.get(id).updatedAt = now;
    }

    /**
     * Claim an unreferenced object. A returned token fences completion accounting;
     * only the latest claimant may record its result. DELETED rows remain eligible
     * for repeated reconciliation because an expired PUT may finish after deletion.
     * inactiveBefore is a scheduling cutoff, never proof of writer cancellation.
     */
    public Optional<RawObjectRecord> claimCleanup(UUID id, Instant inactiveBefore) {
        Objects.requireNonNull(inactiveBefore, "inactiveBefore");
        return tx.inTransaction(em -> {
            var row = lock(em, id);
            Instant now = databaseNow(em);
            if (row.updatedAt.isAfter(inactiveBefore)) return Optional.empty();
            if ((RawObjectRecord.STAGING.equals(row.state) || RawObjectRecord.VERIFIED.equals(row.state))
                    && row.leaseUntil.isAfter(now)) return Optional.empty();
            long refs = em.unwrap(org.hibernate.Session.class).createNativeQuery("SELECT count(*) FROM document_raw_refs WHERE raw_id = :raw", Long.class)
                    .setParameter("raw", id).getSingleResult();
            if (refs != 0) return Optional.empty();
            row.state = RawObjectRecord.DELETING;
            row.cleanupToken = UUID.randomUUID();
            row.cleanupAttempts = Math.addExact(row.cleanupAttempts, 1);
            row.updatedAt = now;
            return Optional.of(row);
        });
    }

    /** Returns false for a superseded claim; storage errors themselves belong in cleanupFailed. */
    public boolean cleanupSucceeded(UUID id, UUID token) {
        return finishCleanup(id, token, null);
    }

    public boolean cleanupFailed(UUID id, UUID token, String error) {
        requireText(error, "error");
        return finishCleanup(id, token, error);
    }

    /** Oldest first, including DELETED tombstones that still require late-writer reconciliation. */
    public List<RawObjectRecord> cleanupCandidates(Instant inactiveBefore, int limit) {
        Objects.requireNonNull(inactiveBefore, "inactiveBefore");
        if (limit < 1 || limit > 1000) throw new IllegalArgumentException("limit must be between 1 and 1000");
        return tx.readOnly(em -> em.unwrap(org.hibernate.Session.class).createNativeQuery("""
                SELECT r.* FROM raw_objects r
                WHERE r.updated_at <= :cutoff
                  AND (r.state NOT IN ('STAGING', 'VERIFIED') OR r.lease_until <= clock_timestamp())
                  AND NOT EXISTS (SELECT 1 FROM document_raw_refs d WHERE d.raw_id = r.raw_id)
                ORDER BY r.updated_at, r.raw_id
                """, RawObjectRecord.class)
                .setParameter("cutoff", inactiveBefore).setMaxResults(limit).getResultList());
    }

    private boolean finishCleanup(UUID id, UUID token, String error) {
        Objects.requireNonNull(token, "token");
        return tx.inTransaction(em -> {
            var row = lock(em, id);
            if (!RawObjectRecord.DELETING.equals(row.state) || !token.equals(row.cleanupToken)) return false;
            row.state = error == null ? RawObjectRecord.DELETED : RawObjectRecord.DELETING;
            row.cleanupError = error;
            row.updatedAt = databaseNow(em);
            return true;
        });
    }

    private static List<UUID> references(EntityManager em, UUID nodeId) {
        return em.unwrap(org.hibernate.Session.class).createNativeQuery("SELECT raw_id FROM document_raw_refs WHERE node_id = :node ORDER BY raw_id", UUID.class)
                .setParameter("node", nodeId).getResultList();
    }

    private static RawObjectRecord lock(EntityManager em, UUID id) {
        var row = em.find(RawObjectRecord.class, Objects.requireNonNull(id, "id"), LockModeType.PESSIMISTIC_WRITE);
        if (row == null) throw new FenceException("Managed raw object does not exist");
        return row;
    }

    private static void requireLeaseOwner(RawObjectRecord row, UUID token, Instant now) {
        if ((!RawObjectRecord.STAGING.equals(row.state) && !RawObjectRecord.VERIFIED.equals(row.state))
                || !row.leaseToken.equals(token) || !row.leaseUntil.isAfter(now))
            throw new FenceException("Upload lease is expired, superseded or no longer publishable");
    }

    private static Instant databaseNow(EntityManager em) {
        // Sample after locks: transaction-start time can predate a long lock wait.
        return em.unwrap(org.hibernate.Session.class).createNativeQuery("SELECT clock_timestamp()", Instant.class).getSingleResult();
    }

    private static void requireLease(Duration lease) {
        Objects.requireNonNull(lease, "lease");
        if (lease.compareTo(Duration.ofSeconds(1)) < 0 || lease.compareTo(Duration.ofDays(1)) > 0)
            throw new IllegalArgumentException("lease must be between one second and one day");
    }

    private static void requireText(String value, String field) {
        if (value == null || value.isBlank()) throw new IllegalArgumentException(field + " must not be blank");
    }
}
