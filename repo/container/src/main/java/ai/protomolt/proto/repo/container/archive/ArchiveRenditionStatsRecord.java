package ai.protomolt.proto.repo.container.archive;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.IdClass;
import jakarta.persistence.Table;

import java.io.Serializable;
import java.util.Objects;

/**
 * One row of the {@code archive_rendition_stats} table: the per-rendition-name
 * breakdown of an archive's retained objects.
 */
@Entity
@Table(name = "archive_rendition_stats")
@IdClass(ArchiveRenditionStatsRecord.Key.class)
public class ArchiveRenditionStatsRecord {

    /** The composite primary key: (account, archive, rendition name). */
    public static class Key implements Serializable {
        /** Owning account. */
        public String accountId;
        /** The archive. */
        public String archive;
        /** The rendition name the counters describe. */
        public String renditionName;

        /** JPA constructor. */
        public Key() {
        }

        /**
         * @param accountId owning account
         * @param archive the archive
         * @param renditionName the rendition name the counters describe
         */
        public Key(String accountId, String archive, String renditionName) {
            this.accountId = accountId;
            this.archive = archive;
            this.renditionName = renditionName;
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof Key k && Objects.equals(accountId, k.accountId)
                    && Objects.equals(archive, k.archive)
                    && Objects.equals(renditionName, k.renditionName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(accountId, archive, renditionName);
        }
    }

    /** Owning account. */
    @Id
    @Column(name = "account_id", nullable = false)
    public String accountId;

    /** The archive. */
    @Id
    @Column(name = "archive", nullable = false)
    public String archive;

    /** The rendition name the counters describe. */
    @Id
    @Column(name = "rendition_name", nullable = false)
    public String renditionName;

    /** Number of stored objects under this name (all retained versions). */
    @Column(name = "object_count", nullable = false)
    public long objectCount;

    /** Total bytes of those objects. */
    @Column(name = "total_bytes", nullable = false)
    public long totalBytes;
}
