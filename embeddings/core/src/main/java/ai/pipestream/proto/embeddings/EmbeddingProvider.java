package ai.pipestream.proto.embeddings;

import java.util.ArrayList;
import java.util.List;

/**
 * ServiceLoader SPI: one implementation per embedding model or runtime.
 *
 * <p>Providers turn text into fixed-length float vectors for VECTOR index fields.
 * Discovery goes through {@link EmbeddingProviders}; {@link PlanEmbedder} applies a
 * provider to engine-neutral mapped documents using the shared indexing plan.
 */
public interface EmbeddingProvider {
    /** Stable id, e.g. {@code model2vec}. */
    String providerId();

    /** Number of components in every vector this provider produces. */
    int dimension();

    /** Embeds {@code text} into a {@link #dimension()}-component vector. */
    float[] embed(String text);

    /**
     * Embeds each text in order. The default loops over {@link #embed(String)};
     * providers with a batch API should override.
     */
    default List<float[]> embedAll(List<String> texts) {
        List<float[]> vectors = new ArrayList<>(texts.size());
        for (String text : texts) {
            vectors.add(embed(text));
        }
        return vectors;
    }
}
