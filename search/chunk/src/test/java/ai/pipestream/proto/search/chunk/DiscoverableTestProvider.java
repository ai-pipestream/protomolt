package ai.pipestream.proto.search.chunk;

import ai.pipestream.proto.search.embedding.EmbeddingProvider;

/**
 * Registered in this module's test {@code META-INF/services} so
 * {@code PolicyDerivation.discover} has a provider to find.
 */
public final class DiscoverableTestProvider implements EmbeddingProvider {

    static final String PROVIDER_ID = "discoverable-test";
    static final int DIMENSION = 2;

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
        return new float[] {text.length(), 1};
    }
}
