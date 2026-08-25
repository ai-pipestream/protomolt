package ai.pipestream.proto.repo.service;

import ai.pipestream.proto.repo.container.ledger.DriveRecord;

import java.util.UUID;

/**
 * Object keys of the archive family: entry-local content addressing under a
 * drive's prefix. A rendition object's key derives from its own content
 * hash, scoped under the entry — unchanged bytes across versions share one
 * object, and every deletion question stays bounded to one entry's own
 * manifests.
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
     * The content-addressed rendition key:
     * {@code <drive.prefix>/archive/<account>/<archive>/<entryUuid>/<name>[/<subKey>]/<sha256>}.
     */
    static String rendition(DriveRecord drive, String accountId, String archive,
                            UUID entryUuid, String name, String subKey, String sha256) {
        String middle = subKey == null || subKey.isBlank() ? name : name + "/" + subKey;
        return DriveKeys.under(drive, "archive/" + sanitize(accountId) + "/" + archive
                + "/" + entryUuid + "/" + middle + "/" + sha256);
    }

    /**
     * A staging key for a streamed upload whose hash is not yet known:
     * {@code .../<entryUuid>/staging/<uploadId>}. A crashed upload leaves
     * this object with no owning manifest — an orphan by the standing rule,
     * reclaimed by the reconciler's min-age sweep.
     */
    static String staging(DriveRecord drive, String accountId, String archive,
                          UUID entryUuid, UUID uploadId) {
        return DriveKeys.under(drive, "archive/" + sanitize(accountId) + "/" + archive
                + "/" + entryUuid + "/staging/" + uploadId);
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
