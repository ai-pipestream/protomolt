package ai.protomolt.proto.repo.codec;

import ai.protomolt.proto.repo.v1.DocumentPart;

import java.util.List;

/**
 * One split-out part object: which part (and, for a chunked or keyed part,
 * which object within it), its serialized bytes, and their SHA-256.
 *
 * @param part the part this fragment carries
 * @param subKey the chunk-set identity for chunked CHUNKS fragments; for a
 *               keyed part, its key values joined with {@code /} for display
 *               and legacy object naming; empty otherwise
 * @param keyValues the key field values identifying a keyed part's fragment,
 *                  in the layout's key-field order (for example
 *                  {@code [segmentation_id]} or
 *                  {@code [segmentation_id, embedding_set_id]}); empty for
 *                  every other part. This is the fragment's identity; the
 *                  joined {@code subKey} is not, because a key value may
 *                  itself contain the separator.
 * @param bytes the fragment's serialized message bytes (same type as the
 *              document that was split)
 * @param sha256 lowercase hex SHA-256 of {@code bytes}
 */
public record PartObject(DocumentPart part, String subKey, List<String> keyValues, byte[] bytes, String sha256) {

    public PartObject {
        keyValues = List.copyOf(keyValues);
    }

    /** A part object with no key values: CORE, BLOBS, PARSED or a chunked CHUNKS run. */
    public PartObject(DocumentPart part, String subKey, byte[] bytes, String sha256) {
        this(part, subKey, List.of(), bytes, sha256);
    }

    /** A keyed part's fragment, with the display sub key derived from its key values. */
    public static PartObject keyed(DocumentPart part, List<String> keyValues, byte[] bytes, String sha256) {
        return new PartObject(part, String.join("/", keyValues), keyValues, bytes, sha256);
    }
}
