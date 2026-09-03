package ai.protomolt.proto.repo.container.archive;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.IdClass;
import jakarta.persistence.Table;

import java.io.Serializable;
import java.util.Objects;

/**
 * One row of the {@code archive_stats} table: an archive's exact counters,
 * adjusted in the same transaction as the mutation they describe. What the
 * store claims, the store can prove from its own rows.
 */
@Entity
@Table(name = "archive_stats")
@IdClass(ArchiveStatsRecord.Key.class)
public class ArchiveStatsRecord {

    /** The composite primary key: (account, archive). */
    public static class Key implements Serializable {
        /** Owning account. */
        public String accountId;
        /** The archive the counters describe. */
        public String archive;

        /** JPA constructor. */
        public Key() {
        }

        /**
         * @param accountId owning account
         * @param archive the archive the counters describe
         */
        public Key(String accountId, String archive) {
            this.accountId = accountId;
            this.archive = archive;
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof Key k && Objects.equals(accountId, k.accountId)
                    && Objects.equals(archive, k.archive);
        }

        @Override
        public int hashCode() {
            return Objects.hash(accountId, archive);
        }
    }

    /** Owning account. */
    @Id
    @Column(name = "account_id", nullable = false)
    public String accountId;

    /** The archive the counters describe. */
    @Id
    @Column(name = "archive", nullable = false)
    public String archive;

    /** Number of live entries. */
    @Column(name = "entries", nullable = false)
    public long entries;

    /** Number of retained versions across all entries. */
    @Column(name = "versions", nullable = false)
    public long versions;

    /** Bytes of every retained object, each object counted once. */
    @Column(name = "retained_bytes", nullable = false)
    public long retainedBytes;

    /** Bytes of the current versions only (the logical archive size). */
    @Column(name = "current_bytes", nullable = false)
    public long currentBytes;
}
