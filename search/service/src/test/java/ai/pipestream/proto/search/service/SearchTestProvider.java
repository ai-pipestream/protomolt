package ai.pipestream.proto.search.service;

import ai.pipestream.proto.search.embedding.EmbeddingProvider;

/**
 * Deterministic token-hashing embeddings registered in this module's test
 * {@code META-INF/services}: wiring proof, not semantics. The same text
 * always embeds to the same vector, so a query drawn from a chunk ranks
 * that chunk first.
 */
public final class SearchTestProvider implements EmbeddingProvider {

    static final String PROVIDER_ID = "search-test";
    static final int DIMENSION = 8;

    @Override
    public String providerId() {
        return PROVIDER_ID;
    }

    @Override
    public int dimension() {
        return DIMENSION;
    }

    @Override
    public float[] embed(String text) {
        float[] vector = new float[DIMENSION];
        for (String token : text.toLowerCase().split("\\W+")) {
            if (!token.isBlank()) {
                vector[Math.floorMod(token.hashCode(), DIMENSION)] += 1.0f;
            }
        }
        double norm = 0;
        for (float component : vector) {
            norm += component * component;
        }
        if (norm > 0) {
            float scale = (float) Math.sqrt(norm);
            for (int i = 0; i < DIMENSION; i++) {
                vector[i] /= scale;
            }
        }
        return vector;
    }
}
