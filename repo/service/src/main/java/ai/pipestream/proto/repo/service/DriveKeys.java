package ai.pipestream.proto.repo.service;

import ai.pipestream.proto.repo.container.ledger.DriveRecord;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * Object keys within a drive. A drive's configured prefix is operator-typed and arrives in
 * every shape a hand-edited setting does: absent, blank, with or without a trailing slash.
 * Normalizing it in one place is what keeps two callers from disagreeing about where a
 * drive's objects live, which is a disagreement nothing detects until something is missing.
 */
final class DriveKeys {

    private DriveKeys() {
    }

    /** {@code <drive.prefix>/<suffix>}, with the prefix normalized and omitted when empty. */
    static String under(DriveRecord drive, String suffix) {
        String prefix = drive.prefix == null ? "" : drive.prefix;
        if (prefix.endsWith("/")) {
            prefix = prefix.substring(0, prefix.length() - 1);
        }
        return (prefix.isBlank() ? "" : prefix + "/") + suffix;
    }

    /**
     * Content-addressed default blob key: {@code <drive.prefix>/blobs/<name-uuid of the
     * sha256>}. Identical puts land on the same object, so a retried upload is an
     * idempotent overwrite rather than a second randomly-keyed copy.
     */
    static String blob(DriveRecord drive, String sha256Hex) {
        UUID nameUuid = UUID.nameUUIDFromBytes(
                ("blob-content|" + sha256Hex).getBytes(StandardCharsets.UTF_8));
        return under(drive, "blobs/" + nameUuid);
    }
}
