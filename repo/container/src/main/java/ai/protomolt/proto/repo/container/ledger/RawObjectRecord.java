package ai.protomolt.proto.repo.container.ledger;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;

/** Managed upload identity and its durable publication/cleanup fence. */
@Entity
@Table(name = "raw_objects")
public class RawObjectRecord {
    public static final String STAGING = "STAGING";
    public static final String VERIFIED = "VERIFIED";
    public static final String LIVE = "LIVE";
    public static final String DELETING = "DELETING";
    public static final String DELETED = "DELETED";

    @Id @Column(name = "raw_id", nullable = false) public UUID rawId;
    @Column(name = "account_id", nullable = false) public String accountId;
    /** Opaque, non-secret identity of the original provider configuration. */
    @Column(name = "backend_identity", nullable = false) public String backendIdentity;
    @Column(name = "drive_id", nullable = false) public UUID driveId;
    @Column(name = "drive_name", nullable = false) public String driveName;
    @Column(name = "bucket", nullable = false) public String bucket;
    @Column(name = "object_key", nullable = false) public String objectKey;
    @Column(name = "expected_size", nullable = false) public long expectedSize;
    @Column(name = "content_type", nullable = false) public String contentType;
    @Column(name = "state", nullable = false, length = 16) public String state;
    @Column(name = "lease_token", nullable = false) public UUID leaseToken;
    @Column(name = "lease_until", nullable = false) public Instant leaseUntil;
    @Column(name = "sha256", length = 64) public String sha256;
    @Column(name = "provider_version") public String providerVersion;
    @Column(name = "etag") public String etag;
    @Column(name = "cleanup_token") public UUID cleanupToken;
    @Column(name = "cleanup_attempts", nullable = false) public long cleanupAttempts;
    @Column(name = "cleanup_error") public String cleanupError;
    @Column(name = "created_at", nullable = false) public Instant createdAt;
    @Column(name = "updated_at", nullable = false) public Instant updatedAt;
}
