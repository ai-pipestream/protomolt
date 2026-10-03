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

    private static final String MANAGED_SEGMENT = ".protomolt-managed";

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

    /** A unique attempt key; its lifetime belongs to the managed raw-object ledger. */
    public static String managedRaw(String drivePrefix, UUID attemptId) {
        java.util.Objects.requireNonNull(attemptId, "attemptId");
        return under(drivePrefix, "blobs/" + MANAGED_SEGMENT + "/v1/" + attemptId + ".bin");
    }

    /** Keys are opaque: reserve the exact path segment without decoding or normalizing. */
    public static boolean isManaged(String objectKey) {
        return hasSegment(objectKey, MANAGED_SEGMENT);
    }

    /**
     * The archive namespace is reserved for archive publication and recovery.
     * This includes keys minted before durable object bindings were introduced.
     * Do not infer ownership from the current drive: aliases can share a namespace.
     */
    public static boolean isArchiveOwned(String objectKey) {
        return hasSegment(objectKey, "archive");
    }

    private static boolean hasSegment(String objectKey, String reserved) {
        if (objectKey == null) return false;
        int start = 0;
        while (start <= objectKey.length()) {
            int end = objectKey.indexOf('/', start);
            if (end < 0) end = objectKey.length();
            if (end - start == reserved.length() && objectKey.startsWith(reserved, start)) return true;
            if (end == objectKey.length()) break;
            start = end + 1;
        }
        return false;
    }
}
