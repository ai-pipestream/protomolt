package ai.pipestream.proto.embeddings;

import java.util.Map;

/**
 * A provider registered in this module's test {@code META-INF/services} so the ServiceLoader
 * seam has something to discover; the core module ships no provider of its own. Embeds from
 * a fixed table so tests can assert exact vectors.
 */
public final class FixedTableEmbeddingProvider implements EmbeddingProvider {

    static final String PROVIDER_ID = "fixed-table";

    private static final Map<String, float[]> TABLE = Map.of(
            "hello world", new float[] {0.1f, 0.2f, 0.3f},
            "a memoir", new float[] {0.4f, 0.5f, 0.6f});

    @Override
    public String providerId() {
        return PROVIDER_ID;
    }

    @Override
    public int dimension() {
        return 3;
    }

    @Override
    public float[] embed(String text) {
        float[] vector = TABLE.get(text);
        if (vector == null) {
            throw new IllegalArgumentException("No fixture vector for '" + text + "'");
        }
        return vector;
    }
}
