package ai.protomolt.proto.repo.container.archive;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.IdClass;
import jakarta.persistence.Table;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.io.Serializable;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * One row of the {@code archive_versions} table: one immutable stored state
 * of one entry. The manifest JSONB is the authoritative list of the
 * version's objects; it lives here, not in object storage, because it must
 * be transactionally consistent with the version's existence.
 */
@Entity
@Table(name = "archive_versions")
@IdClass(ArchiveVersionRecord.Key.class)
public class ArchiveVersionRecord {

    /** The composite primary key: (entry, version). */
    public static class Key implements Serializable {
        /** The entry the version belongs to. */
        public UUID entryUuid;
        /** The version number. */
        public long version;

        /** JPA constructor. */
        public Key() {
        }

        /**
         * @param entryUuid the entry the version belongs to
         * @param version the version number
         */
        public Key(UUID entryUuid, long version) {
            this.entryUuid = entryUuid;
            this.version = version;
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof Key k && version == k.version
                    && Objects.equals(entryUuid, k.entryUuid);
        }

        @Override
        public int hashCode() {
            return Objects.hash(entryUuid, version);
        }
    }

    /** The entry the version belongs to. */
    @Id
    @Column(name = "entry_uuid", nullable = false)
    public UUID entryUuid;

    /** The version number: 1 for the first stored state, monotonic per entry. */
    @Id
    @Column(name = "version", nullable = false)
    public long version;

    /** protobuf-JSON of the {@code VersionManifest}. */
    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "manifest", nullable = false)
    public String manifest;

    /** SHA-256 over the ordered rendition hashes (the whole-save dedupe key). */
    @Column(name = "root_checksum", nullable = false)
    public String rootChecksum;

    /** Sum of PRESENT rendition sizes in this version. */
    @Column(name = "total_bytes", nullable = false)
    public long totalBytes;

    /** When this version was created. */
    @Column(name = "created_at", nullable = false)
    public Instant createdAt;
}
