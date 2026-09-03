package ai.protomolt.proto.repo.container.codec;

import ai.protomolt.proto.repo.v1.DocumentPart;

/**
 * One split-out part object: which part (and, for CHUNKS, which chunk set),
 * its serialized bytes, and their SHA-256.
 *
 * @param part the part this fragment carries
 * @param subKey the chunk-set identity for CHUNKS fragments; empty otherwise
 * @param bytes the fragment's serialized message bytes (same type as the
 *              document that was split)
 * @param sha256 lowercase hex SHA-256 of {@code bytes}
 */
public record PartObject(DocumentPart part, String subKey, byte[] bytes, String sha256) {
}
