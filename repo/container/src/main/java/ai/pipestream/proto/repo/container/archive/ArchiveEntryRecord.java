package ai.pipestream.proto.repo.container.archive;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.Instant;
import java.util.UUID;

/**
 * One row of the {@code archive_entries} table: one logical document inside
 * an archive. The row is the entry's metadata and its version cursor; the
 * stored states themselves are {@link ArchiveVersionRecord} rows.
 */
@Entity
@Table(name = "archive_entries")
public class ArchiveEntryRecord {

    /** Deterministic id over {@code account|archive|entry} (see {@link ArchiveIds}). */
    @Id
    @Column(name = "entry_uuid", nullable = false)
    public UUID entryUuid;

    /** Owning account (tenant root). */
    @Column(name = "account_id", nullable = false)
    public String accountId;

    /** The archive holding the entry. */
    @Column(name = "archive", nullable = false)
    public String archive;

    /** Caller-chosen entry identity, unique within the archive; free-form. */
    @Column(name = "entry_id", nullable = false)
    public String entryId;

    /** The latest version number; the optimistic-concurrency token. */
    @Column(name = "current_version", nullable = false)
    public long currentVersion;

    /** Display title; null when none was given. */
    @Column(name = "title")
    public String title;

    /** Original filename; null when the entry came from no file. */
    @Column(name = "filename")
    public String filename;

    /** Media type of the entry's primary content; null when unknown. */
    @Column(name = "content_type")
    public String contentType;

    /** Where the content came from; null when the source has no address. */
    @Column(name = "source_uri")
    public String sourceUri;

    /** The source's own last-modified instant; null when unreported. */
    @Column(name = "source_modified_at")
    public Instant sourceModifiedAt;

    /** Extension-metadata map as a JSON object; null when empty. */
    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "metadata")
    public String metadata;

    /** The stored Classification as protobuf-JSON; null = never classified. */
    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "classification")
    public String classification;

    /**
     * The classification state's own column (UNCLASSIFIED, DECLARED,
     * IDENTIFIED, VERIFIED, CONFLICTED), so filters and the exact per-state
     * counts are one indexed aggregate.
     */
    @Column(name = "classification_state", nullable = false)
    public String classificationState = "UNCLASSIFIED";

    /** When the entry was first created. */
    @Column(name = "created_at", nullable = false)
    public Instant createdAt;

    /** When the entry last changed. */
    @Column(name = "updated_at", nullable = false)
    public Instant updatedAt;
}
