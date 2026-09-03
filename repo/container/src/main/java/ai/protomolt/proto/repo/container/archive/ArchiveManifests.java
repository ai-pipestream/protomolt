package ai.protomolt.proto.repo.container.archive;

import ai.protomolt.proto.repo.archive.v1.RenditionManifestEntry;
import ai.protomolt.proto.repo.archive.v1.RenditionState;
import ai.protomolt.proto.repo.archive.v1.VersionManifest;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The manifest's mechanics: JSON round trip for the ledger column, stable
 * rendition ordering, the root checksum, and the key accounting that makes
 * entry-local content addressing safe — which objects a set of manifests
 * references, and therefore which objects a mutation may add or delete.
 */
public final class ArchiveManifests {

    /** Orders manifest entries by (name, sub_key) — the stable manifest order. */
    public static final Comparator<RenditionManifestEntry> ORDER =
            Comparator.comparing((RenditionManifestEntry e) -> e.getRendition().getName())
                    .thenComparing(e -> e.getRendition().getSubKey());

    private ArchiveManifests() {
    }

    /**
     * Serializes a manifest for the ledger's JSONB column.
     *
     * @param manifest the manifest to store
     * @return protobuf-JSON of the manifest
     */
    public static String toJson(VersionManifest manifest) {
        try {
            return JsonFormat.printer().omittingInsignificantWhitespace().print(manifest);
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalStateException("manifest does not print as JSON", e);
        }
    }

    /**
     * Parses a manifest from the ledger's JSONB column.
     *
     * @param json protobuf-JSON of the manifest
     * @return the typed manifest
     */
    public static VersionManifest fromJson(String json) {
        try {
            VersionManifest.Builder builder = VersionManifest.newBuilder();
            JsonFormat.parser().merge(json, builder);
            return builder.build();
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalStateException("stored manifest does not parse", e);
        }
    }

    /**
     * The version's identity as content: SHA-256 over the ordered rendition
     * identities (name, sub key, state, content hash). Two versions with
     * equal root checksums hold identical bytes.
     *
     * @param renditions the manifest's entries, already in {@link #ORDER}
     * @return lowercase hex SHA-256
     */
    public static String rootChecksum(List<RenditionManifestEntry> renditions) {
        MessageDigest digest = sha256();
        for (RenditionManifestEntry entry : renditions) {
            digest.update(entry.getRendition().getName().getBytes(StandardCharsets.UTF_8));
            digest.update((byte) 0);
            digest.update(entry.getRendition().getSubKey().getBytes(StandardCharsets.UTF_8));
            digest.update((byte) 0);
            digest.update(entry.getState().name().getBytes(StandardCharsets.UTF_8));
            digest.update((byte) 0);
            digest.update(entry.getSha256().getBytes(StandardCharsets.UTF_8));
            digest.update((byte) 0);
        }
        return hex(digest.digest());
    }

    /**
     * Sum of PRESENT rendition sizes — the version's logical size.
     *
     * @param renditions the manifest's entries
     * @return total bytes a full read of the version fetches
     */
    public static long totalBytes(List<RenditionManifestEntry> renditions) {
        long total = 0;
        for (RenditionManifestEntry entry : renditions) {
            if (entry.getState() == RenditionState.RENDITION_STATE_PRESENT) {
                total += entry.getSizeBytes();
            }
        }
        return total;
    }

    /**
     * Every object the given manifests reference, keyed by object key with
     * the entry that describes it (PRESENT entries only — EMPTY has no
     * object and DELETED's object is already gone). This is the ownership
     * set every add/delete decision derives from.
     *
     * @param manifests the manifests to walk
     * @return object key → one referencing manifest entry
     */
    public static Map<String, RenditionManifestEntry> referencedObjects(
            List<VersionManifest> manifests) {
        Map<String, RenditionManifestEntry> objects = new HashMap<>();
        for (VersionManifest manifest : manifests) {
            for (RenditionManifestEntry entry : manifest.getRenditionsList()) {
                if (entry.getState() == RenditionState.RENDITION_STATE_PRESENT
                        && !entry.getObjectKey().isBlank()) {
                    objects.putIfAbsent(entry.getObjectKey(), entry);
                }
            }
        }
        return objects;
    }

    /**
     * The keys in {@code before} that {@code after} no longer references —
     * the objects a mutation may physically delete.
     *
     * @param before the ownership set before the mutation
     * @param after the ownership set after the mutation
     * @return the no-longer-referenced keys
     */
    public static List<String> unreferencedKeys(Map<String, RenditionManifestEntry> before,
                                                Map<String, RenditionManifestEntry> after) {
        List<String> gone = new ArrayList<>();
        for (String key : before.keySet()) {
            if (!after.containsKey(key)) {
                gone.add(key);
            }
        }
        return gone;
    }

    /**
     * Lowercase hex SHA-256 of some bytes.
     *
     * @param data the bytes
     * @return the digest as lowercase hex
     */
    public static String sha256Hex(byte[] data) {
        return hex(sha256().digest(data));
    }

    /** A fresh SHA-256 digest (the JDK guarantees the algorithm exists). */
    public static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }

    private static String hex(byte[] digest) {
        StringBuilder hex = new StringBuilder(digest.length * 2);
        for (byte b : digest) {
            hex.append(Character.forDigit((b >> 4) & 0xF, 16));
            hex.append(Character.forDigit(b & 0xF, 16));
        }
        return hex.toString();
    }
}
