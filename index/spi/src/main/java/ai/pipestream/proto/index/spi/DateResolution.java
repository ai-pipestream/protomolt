package ai.pipestream.proto.index.spi;

/**
 * Numeric emission unit for {@code google.protobuf.Timestamp} values, where engines emit
 * dates numerically (Lucene points/docValues). Mirrors {@code DateResolution} in
 * {@code indexing_hints.proto}; unspecified resolves to {@link #MILLIS}.
 */
public enum DateResolution {
    MILLIS,
    SECONDS
}
