package ai.pipestream.proto.repo.container.archive;

import ai.pipestream.proto.repo.archive.v1.EntryAddress;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * Deterministic identity for the archive family: name-based UUIDs over
 * logical coordinates, so identity is never random. Re-saves are idempotent
 * by construction, the same address can never mint two rows, and the entry
 * UUID doubles as the object-key prefix segment — which is why the
 * free-form {@code entry_id} itself never reaches a key.
 */
public final class ArchiveIds {

    /** Separator used in composite keys to prevent segment collisions. */
    private static final String SEPARATOR = "|";

    private ArchiveIds() {
    }

    /**
     * The deterministic archive row id.
     *
     * @param accountId the owning account
     * @param name the archive name (account-scoped slug)
     * @return the name-based UUID over {@code account|name}
     */
    public static UUID archiveId(String accountId, String name) {
        requireSegment(accountId, "accountId");
        requireSegment(name, "name");
        return UUID.nameUUIDFromBytes(
                ("archive" + SEPARATOR + accountId + SEPARATOR + name)
                        .getBytes(StandardCharsets.UTF_8));
    }

    /**
     * The deterministic entry UUID for an address.
     *
     * @param address the entry's three logical segments
     * @return the name-based UUID over {@code account|archive|entry}
     */
    public static UUID entryUuid(EntryAddress address) {
        requireSegment(address.getAccountId(), "accountId");
        requireSegment(address.getArchive(), "archive");
        requireSegment(address.getEntryId(), "entryId");
        return UUID.nameUUIDFromBytes(
                ("archive-entry" + SEPARATOR + address.getAccountId()
                        + SEPARATOR + address.getArchive()
                        + SEPARATOR + address.getEntryId())
                        .getBytes(StandardCharsets.UTF_8));
    }

    private static void requireSegment(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " cannot be null or blank");
        }
    }
}
