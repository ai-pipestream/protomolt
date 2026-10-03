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

    /** Path-segment sanitization: anything outside {@code [A-Za-z0-9._-]} becomes {@code _}. */
    static String sanitize(String segment) {
        StringBuilder out = new StringBuilder(segment.length());
        for (int i = 0; i < segment.length(); i++) {
            char c = segment.charAt(i);
            boolean safe = (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z')
                    || (c >= '0' && c <= '9') || c == '.' || c == '_' || c == '-';
            out.append(safe ? c : '_');
        }
        return out.toString();
    }
}
