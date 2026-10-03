package ai.protomolt.proto.repo.container.archive;

import ai.protomolt.proto.repo.container.ledger.Tx;
import jakarta.persistence.EntityManager;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/** Internal upload admission. Verification records observed bytes, not schema validity or publication. */
public final class ArchiveUploadLedger {
    private final Tx tx;

    public ArchiveUploadLedger(Tx tx) { this.tx = Objects.requireNonNull(tx, "tx"); }

    public record Upload(UUID objectId, UUID leaseToken, Instant leaseUntil, long expectedSize,
            String state, String sha256, String providerVersion, String etag) {}
    public record Admission(ArchiveObjectLedger.Binding binding, Upload upload) {}

    public static final class FenceException extends RuntimeException {
        public FenceException(String message) { super(message); }
    }

    /** Atomically reserve coordinates and a lease before starting provider I/O. */
    public Admission begin(ArchiveObjectLedger.Location location, long expectedSize, String contentType, Duration lease) {
        Objects.requireNonNull(location, "location");
        requireLease(lease);
        if (expectedSize < 0) throw new IllegalArgumentException("expectedSize must be nonnegative");
        if (contentType == null || contentType.isBlank()) throw new IllegalArgumentException("contentType must not be blank");
        return tx.inTransaction(em -> {
            var binding = ArchiveObjectLedger.register(em, location);
            UUID token = UUID.randomUUID();
            em.createNativeQuery("""
                    INSERT INTO archive_object_uploads(object_id,expected_size,content_type,lease_token,lease_until,state)
                    VALUES (:id,:size,:type,:token,clock_timestamp()+(:millis * interval '1 millisecond'),'STAGING')
                    """).setParameter("id", binding.objectId()).setParameter("size", expectedSize)
                    .setParameter("type", contentType).setParameter("token", token)
                    .setParameter("millis", lease.toMillis()).executeUpdate();
            return new Admission(binding, lock(em, binding.objectId()));
        });
    }

    public Upload renew(UUID objectId, UUID token, Duration lease) {
        requireLease(lease);
        return tx.inTransaction(em -> {
            var upload = lock(em, objectId);
            requireOwner(em, upload, token);
            em.createNativeQuery("""
                    UPDATE archive_object_uploads SET lease_until=clock_timestamp()+(:millis * interval '1 millisecond')
                    WHERE object_id=:id
                    """).setParameter("id", objectId).setParameter("millis", lease.toMillis()).executeUpdate();
            return lock(em, objectId);
        });
    }

    /** Caller supplies the measured identity after complete successful provider I/O. */
    public Upload verify(UUID objectId, UUID token, long actualSize, String sha256, String providerVersion, String etag) {
        if (sha256 == null || !sha256.matches("[0-9a-f]{64}"))
            throw new IllegalArgumentException("sha256 must be 64 lowercase hexadecimal characters");
        return tx.inTransaction(em -> {
            var upload = lock(em, objectId);
            requireOwner(em, upload, token);
            if (actualSize != upload.expectedSize()) throw new FenceException("Upload length differs from admission");
            if ("VERIFIED".equals(upload.state())) {
                if (!sha256.equals(upload.sha256()) || !Objects.equals(providerVersion, upload.providerVersion())
                        || !Objects.equals(etag, upload.etag())) throw new FenceException("Verified upload identity differs");
                return upload;
            }
            em.createNativeQuery("""
                    UPDATE archive_object_uploads SET state='VERIFIED',sha256=:sha,provider_version=:version,etag=:etag
                    WHERE object_id=:id
                    """).setParameter("id", objectId).setParameter("sha", sha256)
                    .setParameter("version", providerVersion).setParameter("etag", etag).executeUpdate();
            return lock(em, objectId);
        });
    }

    private static Upload lock(EntityManager em, UUID id) {
        Objects.requireNonNull(id, "objectId");
        List<?> rows = em.createNativeQuery("""
                SELECT object_id,lease_token,EXTRACT(EPOCH FROM lease_until),
                       expected_size,state,sha256,provider_version,etag
                FROM archive_object_uploads WHERE object_id=:id FOR UPDATE
                """).setParameter("id", id).getResultList();
        if (rows.isEmpty()) throw new FenceException("Archive upload admission is missing");
        var row = (Object[]) rows.getFirst();
        var epoch = (java.math.BigDecimal) row[2];
        long seconds = epoch.longValue();
        int nanos = epoch.subtract(java.math.BigDecimal.valueOf(seconds)).movePointRight(9).intValueExact();
        return new Upload((UUID) row[0], (UUID) row[1], Instant.ofEpochSecond(seconds, nanos),
                ((Number) row[3]).longValue(), (String) row[4], (String) row[5], (String) row[6], (String) row[7]);
    }

    private static void requireOwner(EntityManager em, Upload upload, UUID token) {
        Instant now = em.unwrap(org.hibernate.Session.class)
                .createNativeQuery("SELECT clock_timestamp()", Instant.class).getSingleResult();
        if (!upload.leaseToken().equals(token) || !upload.leaseUntil().isAfter(now))
            throw new FenceException("Archive upload lease is expired or belongs to another attempt");
    }

    private static void requireLease(Duration lease) {
        Objects.requireNonNull(lease, "lease");
        if (lease.compareTo(Duration.ofSeconds(1)) < 0 || lease.compareTo(Duration.ofDays(1)) > 0)
            throw new IllegalArgumentException("lease must be between one second and one day");
    }
}
