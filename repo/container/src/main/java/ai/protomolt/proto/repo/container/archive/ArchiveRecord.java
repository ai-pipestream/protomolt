package ai.protomolt.proto.repo.container.archive;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.Instant;
import java.util.UUID;

/**
 * One row of the {@code archives} table: an account-scoped named collection
 * of entries, bound to a drive and carrying its versioning policy.
 */
@Entity
@Table(name = "archives")
public class ArchiveRecord {

    /** Versioning column value: one retained state per entry. */
    public static final String VERSIONING_NONE = "NONE";

    /** Versioning column value: every save lands a new immutable version. */
    public static final String VERSIONING_RETAINED = "RETAINED";

    /** Deterministic id over {@code account|name} (see {@link ArchiveIds}). */
    @Id
    @Column(name = "archive_id", nullable = false)
    public UUID archiveId;

    /** Owning account (tenant root). */
    @Column(name = "account_id", nullable = false)
    public String accountId;

    /** Archive name: an account-scoped slug (it appears in object keys). */
    @Column(name = "name", nullable = false)
    public String name;

    /** The drive whose backing store holds this archive's objects. */
    @Column(name = "drive_name", nullable = false)
    public String driveName;

    /** {@link #VERSIONING_NONE} or {@link #VERSIONING_RETAINED}. */
    @Column(name = "versioning", nullable = false)
    public String versioning;

    /** Human-readable description; null when none was given. */
    @Column(name = "description")
    public String description;

    /** Extension-metadata map as a JSON object; null when empty. */
    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "metadata")
    public String metadata;

    /** When the archive was created. */
    @Column(name = "created_at", nullable = false)
    public Instant createdAt;

    /** Whether the archive retains superseded versions. */
    public boolean retainsVersions() {
        return VERSIONING_RETAINED.equals(versioning);
    }
}
