package ai.protomolt.proto.repo.engine;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * Object keys within a drive. A drive's configured prefix is operator-typed and arrives in
 * every shape a hand-edited setting does: absent, blank, with or without a trailing slash.
 * Normalizing it in one place is what keeps two callers from disagreeing about where a
 * drive's objects live, which is a disagreement nothing detects until something is missing.
 */
public final class DriveKeys {

    private DriveKeys() {
    }

    /** {@code <drive.prefix>/<suffix>}, with the prefix normalized and omitted when empty. */
    public static String under(String drivePrefix, String suffix) {
        String prefix = drivePrefix == null ? "" : drivePrefix;
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
    public static String blob(String drivePrefix, String sha256Hex) {
        UUID nameUuid = UUID.nameUUIDFromBytes(
                ("blob-content|" + sha256Hex).getBytes(StandardCharsets.UTF_8));
        return under(drivePrefix, "blobs/" + nameUuid);
    }
}
