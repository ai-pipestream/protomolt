package ai.protomolt.proto.repo.engine;

import ai.protomolt.proto.repo.engine.DriveKeys;

import ai.protomolt.proto.repo.container.ledger.DriveRecord;

import java.util.UUID;

/**
 * Physical archive objects use unique write identities under a drive's prefix.
 * Content hashes remain integrity/dedupe identities. Unchanged renditions share
 * objects by carrying retained manifest references, never by minting the same
 * key for another write or a recreated entry.
 *
 * <p>Every caller-influenced segment is either validated path-safe at the
 * contract (rendition name, sub key, archive slug) or sanitized here
 * (account id), because signature canonicalization must not diverge on
 * exotic bytes. The free-form entry id never appears — the deterministic
 * entry UUID stands in for it.
 */
final class ArchiveKeys {

    private ArchiveKeys() {
    }

    /**
     * A fresh rendition key for one physical write:
     * {@code <drive.prefix>/archive/<account>/<archive>/<entryUuid>/<name>[/<subKey>]/writes/<uuid>/<sha256>}.
     */
    static String rendition(DriveRecord drive, String accountId, String archive,
                            UUID entryUuid, String name, String subKey, String sha256) {
        String middle = subKey == null || subKey.isBlank() ? name : name + "/" + subKey;
        return DriveKeys.under(drive.prefix, "archive/" + sanitize(accountId) + "/" + archive
                + "/" + entryUuid + "/" + middle + "/writes/" + UUID.randomUUID() + "/" + sha256);
    }

    /**
     * A staging key for a streamed upload whose hash is not yet known:
     * {@code .../<entryUuid>/staging/<uploadId>}. A crashed upload leaves
     * this object with no owning manifest. Durable candidate registration and
     * orphan recovery are required before claiming automatic reclamation.
     */
    static String staging(DriveRecord drive, String accountId, String archive,
                          UUID entryUuid, UUID uploadId) {
        return DriveKeys.under(drive.prefix, "archive/" + sanitize(accountId) + "/" + archive
                + "/" + entryUuid + "/staging/" + uploadId);
    }

    /** Final immutable key allocated before a managed stream's digest is known. */
    static String streamed(DriveRecord drive, String accountId, String archive, UUID entryUuid) {
        return DriveKeys.under(drive.prefix, "archive/" + sanitize(accountId) + "/" + archive
                + "/" + entryUuid + "/writes/" + UUID.randomUUID());
    }

    /**
     * Path-segment sanitization: anything outside {@code [A-Za-z0-9._-]} becomes {@code _}.
     *
     * @throws IllegalArgumentException when the input carries an ASCII control character,
     *         or the result is empty or consists solely of {@code .} characters, because
     *         such a segment would collapse or climb out of its namespace
     */
    static String sanitize(String segment) {
        StringBuilder out = new StringBuilder(segment.length());
        for (int i = 0; i < segment.length(); i++) {
            char c = segment.charAt(i);
            if (c < 0x20 || c == 0x7F) {
                throw new IllegalArgumentException(
                        "archive key segment must not contain control characters");
            }
            boolean safe = (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z')
                    || (c >= '0' && c <= '9') || c == '.' || c == '_' || c == '-';
            out.append(safe ? c : '_');
        }
        String sanitized = out.toString();
        if (sanitized.isEmpty() || isDotOnly(sanitized)) {
            throw new IllegalArgumentException(
                    "archive key segment must not be empty or consist solely of '.'"
                            + " characters: '" + sanitized + "'");
        }
        return sanitized;
    }

    /** Whether every character is a dot; the empty string is not dot-only. */
    private static boolean isDotOnly(String value) {
        if (value.isEmpty()) {
            return false;
        }
        for (int i = 0; i < value.length(); i++) {
            if (value.charAt(i) != '.') {
                return false;
            }
        }
        return true;
    }
}
