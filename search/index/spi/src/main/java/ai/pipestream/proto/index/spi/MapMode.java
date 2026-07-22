package ai.pipestream.proto.index.spi;

/**
 * How protobuf map fields land in engine documents.
 * Mirrors {@code MapMode} in {@code indexing_hints.proto}; an unset mode
 * ({@link ResolvedFieldHint#mapMode()} {@code null}) keeps each engine's documented default.
 */
public enum MapMode {
    /** Dynamic keys: one engine field/property per map key. */
    FLATTEN,
    /** Nested {@code [{key, value}]} entries. */
    ENTRIES,
    /** The whole map serialized to one JSON string field. */
    JSON,
    SKIP
}
