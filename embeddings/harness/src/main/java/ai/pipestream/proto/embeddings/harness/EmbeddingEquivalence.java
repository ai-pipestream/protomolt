package ai.pipestream.proto.embeddings.harness;

import ai.pipestream.proto.embeddings.EmbeddingProvider;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Certifies that two {@link EmbeddingProvider}s serving the same model produce near-identical
 * vectors.
 *
 * <p>A runtime can mix two providers (index with one, query with the other, or shard across
 * both) only when they agree on the vector space: for the same model, per-text cosine
 * similarity must be ~1. {@link #compare} embeds a corpus with both providers and reduces the
 * per-text cosines to an {@link EquivalenceReport}; the pair is certified when the worst text
 * still clears the caller's threshold. Cosine is scale-invariant, so the report also carries
 * the range of per-text norm ratios: they expose a normalization disagreement that certifies
 * anyway but breaks an index scored with L2 or dot product.
 */
public final class EmbeddingEquivalence {

    private EmbeddingEquivalence() {
    }

    /**
     * Compares {@code a} and {@code b} over {@code texts}: one {@code embedAll} per provider,
     * one cosine and one norm ratio per text, reduced to an {@link EquivalenceReport}.
     *
     * @throws IllegalArgumentException when {@code texts} is empty or the providers disagree
     *         on {@link EmbeddingProvider#dimension()}
     */
    public static EquivalenceReport compare(EmbeddingProvider a, EmbeddingProvider b,
            List<String> texts, double threshold) {
        Objects.requireNonNull(a, "a");
        Objects.requireNonNull(b, "b");
        Objects.requireNonNull(texts, "texts");
        if (texts.isEmpty()) {
            throw new IllegalArgumentException("An empty corpus cannot certify a provider pair");
        }
        if (a.dimension() != b.dimension()) {
            throw new IllegalArgumentException("Dimension mismatch: provider '" + a.providerId()
                    + "' embeds at " + a.dimension() + ", provider '" + b.providerId()
                    + "' at " + b.dimension());
        }
        List<float[]> vectorsA = a.embedAll(texts);
        List<float[]> vectorsB = b.embedAll(texts);
        List<Double> cosines = new ArrayList<>(texts.size());
        List<Double> normRatios = new ArrayList<>(texts.size());
        for (int i = 0; i < texts.size(); i++) {
            float[] vectorA = vectorsA.get(i);
            float[] vectorB = vectorsB.get(i);
            cosines.add(Cosines.cosine(vectorA, vectorB));
            double normA = Cosines.norm(vectorA);
            double normB = Cosines.norm(vectorB);
            // Two zero vectors agree on scale; a lone zero norm below divides to +Infinity.
            normRatios.add(normA == 0 && normB == 0 ? 1.0 : normA / normB);
        }
        return EquivalenceReport.from(a.providerId(), b.providerId(), cosines, normRatios,
                threshold);
    }
}
