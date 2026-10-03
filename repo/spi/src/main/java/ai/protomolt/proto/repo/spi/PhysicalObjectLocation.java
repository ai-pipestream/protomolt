package ai.protomolt.proto.repo.spi;

import java.util.Objects;
import java.util.UUID;

/**
 * Original storage coordinates reserved before an upload. The object ID is a
 * repository identity, independent of documents, revisions and upload attempts.
 * This value grants neither access nor permission to reclaim content.
 *
 * <p>The generation resolves an immutable, trusted backend profile. The realm
 * identifies shared storage across profile generations. Reservation must reject
 * duplicate realm/namespace/key coordinates, including aliases through another
 * generation, until version-specific reclamation is supported. Constructing this
 * value does not perform that reservation or qualify a backend.
 *
 * <p>Coordinates are exact and are never trimmed, normalized or resolved through
 * a current drive configuration. Unknown legacy locations require explicit
 * migration evidence; they cannot be inferred from a current profile.
 */
public record PhysicalObjectLocation(UUID objectId, String backendGeneration,
        String storageRealm, String namespace, String key) {
    public PhysicalObjectLocation {
        Objects.requireNonNull(objectId, "objectId");
        requireText(backendGeneration, "backendGeneration");
        requireText(storageRealm, "storageRealm");
        requireText(namespace, "namespace");
        requireText(key, "key");
    }

    private static void requireText(String value, String field) {
        if (value == null || value.isBlank())
            throw new IllegalArgumentException(field + " must not be blank");
    }
}
