package ai.pipestream.proto.repo.container.ledger;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.PrePersist;
import jakarta.persistence.Table;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.Instant;
import java.util.UUID;

/**
 * A named object-storage drive: the bucket/prefix root that document part
 * objects live under.
 * <p>
 * Drives are account-scoped — {@code (account_id, name)} is unique — because
 * document rows reference their drive by bare name ({@code drive_name}) and
 * that name must resolve unambiguously within the owning account. The drive
 * row carries everything the storage adapter needs to reach the bucket
 * (provider, bucket, prefix, region, credentials reference) so document rows
 * stay storage-agnostic.
 */
@Entity
@Table(name = "drives", check = @jakarta.persistence.CheckConstraint(
        name = "chk_drives_type", constraint = "drive_type IN ('INTAKE', 'PIPELINE', 'CUSTOM')"))
public class DriveRecord {

    /** Default constructor required by the JPA/Hibernate persistence provider. */
    public DriveRecord() {
    }

    /** Repository's unique identifier (UUID) — primary key, caller-minted. */
    @Id
    @Column(name = "drive_id", nullable = false)
    public UUID driveId;

    /** Identifier of the account that owns this drive. */
    @Column(name = "account_id", nullable = false)
    public String accountId;

    /** Drive name, unique within the account; document rows reference it. */
    @Column(name = "name", nullable = false)
    public String name;

    /**
     * Drive flavor: {@code INTAKE} (staging area for raw intake blobs),
     * {@code PIPELINE} (working area for pipeline part objects) or
     * {@code CUSTOM} (caller-managed). Enforced by check constraint.
     */
    @Column(name = "drive_type", nullable = false)
    public String driveType;

    /** Storage provider implementation, e.g. {@code s3}. */
    @Column(name = "provider", nullable = false)
    public String provider = "s3";

    /** Bucket the drive's objects live in. */
    @Column(name = "bucket", nullable = false)
    public String bucket;

    /** Key prefix inside the bucket; empty string for the bucket root. */
    @Column(name = "prefix", nullable = false)
    public String prefix = "";

    /** Provider region of the bucket (nullable — provider default). */
    @Column(name = "region")
    public String region;

    /**
     * Opaque reference to the credentials the storage adapter should use
     * (e.g. a secret-store key), never the credentials themselves.
     */
    @Column(name = "credentials_ref")
    public String credentialsRef;

    /** Lifecycle status, e.g. {@code ACTIVE}; non-ACTIVE drives reject writes. */
    @Column(name = "status", nullable = false)
    public String status = "ACTIVE";

    /** Free-form provider/drive metadata as JSON ({@code jsonb}). */
    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "metadata")
    public String metadata;

    /** Timestamp when this record was first created. */
    @Column(name = "created_at", nullable = false)
    public Instant createdAt;

    /** Default the creation timestamp on insert. */
    @PrePersist
    void onPrePersist() {
        if (createdAt == null) {
            createdAt = Instant.now();
        }
    }
}
