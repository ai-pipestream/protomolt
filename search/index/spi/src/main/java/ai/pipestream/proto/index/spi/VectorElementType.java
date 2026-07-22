package ai.pipestream.proto.index.spi;

/**
 * Engine-neutral KNN element encoding.
 * Mirrors {@code VectorElementType} in {@code indexing_hints.proto}; unspecified resolves
 * to {@link #FLOAT32}.
 */
public enum VectorElementType {
    FLOAT32,
    BYTE
}
