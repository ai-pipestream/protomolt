package ai.pipestream.proto.index.spi;

/**
 * Role a field plays in a parent/child (block-join) document layout.
 *
 * <p>Flat engines ignore block roles. Block engines use {@link #CHUNKS} to
 * locate the child scope, {@link #DOC_ID} for the parent identity they index
 * canonically on every block member, and {@link #CHUNK_ID} for per-chunk
 * identity inside the chunks scope.
 */
public enum BlockRole {
    UNSPECIFIED,
    /** The repeated message field holding the child documents. */
    CHUNKS,
    /** Stable parent identity, consumed by the engine rather than re-emitted. */
    DOC_ID,
    /** Per-chunk identity inside the CHUNKS scope. */
    CHUNK_ID
}
