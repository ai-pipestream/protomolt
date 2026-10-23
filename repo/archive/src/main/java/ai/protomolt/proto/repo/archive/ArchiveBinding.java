package ai.protomolt.proto.repo.archive;

import ai.protomolt.proto.repo.archive.v1.VersioningPolicy;
import java.util.Objects;

/**
 * What the archive over v2 documents needs to know about one archive: the
 * intake datasource its entries are owned by, the drive its saves name, and the
 * versioning policy that decides which revisions it serves.
 *
 * @param accountId the owning account
 * @param archive the archive name, the entries' {@code graph_address_id}
 * @param datasourceId the intake datasource every entry's ownership and
 *        {@code SaveOwner.intake} name
 * @param drive the drive saves name; blank uses the repository's default drive
 * @param versioning which revisions the archive serves
 */
public record ArchiveBinding(
        String accountId, String archive, String datasourceId, String drive, VersioningPolicy versioning) {

    /**
     * @throws IllegalArgumentException when a required value is blank or the
     *         versioning policy is unspecified
     */
    public ArchiveBinding {
        requireText(accountId, "accountId");
        requireText(archive, "archive");
        requireText(datasourceId, "datasourceId");
        Objects.requireNonNull(drive, "drive");
        Objects.requireNonNull(versioning, "versioning");
        if (versioning != VersioningPolicy.VERSIONING_POLICY_NONE
                && versioning != VersioningPolicy.VERSIONING_POLICY_RETAINED) {
            throw new IllegalArgumentException("versioning must be NONE or RETAINED, not " + versioning);
        }
    }

    private static void requireText(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
    }
}
