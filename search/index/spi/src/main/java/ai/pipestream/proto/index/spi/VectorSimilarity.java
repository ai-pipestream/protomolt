package ai.pipestream.proto.index.spi;

/**
 * Engine-neutral KNN similarity function.
 * Mirrors {@code VectorSimilarity} in {@code indexing_hints.proto}; unspecified resolves
 * to {@link #COSINE}. Engines map it onto their native vocabulary (Lucene
 * {@code VectorSimilarityFunction}, OpenSearch {@code space_type}, Solr
 * {@code similarityFunction}).
 */
public enum VectorSimilarity {
    COSINE,
    DOT_PRODUCT,
    L2,
    MAX_INNER_PRODUCT
}
