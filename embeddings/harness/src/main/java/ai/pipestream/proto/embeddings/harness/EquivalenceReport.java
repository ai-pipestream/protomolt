package ai.pipestream.proto.embeddings.harness;

import java.util.List;
import java.util.Objects;

/**
 * The outcome of comparing two embedding providers over a corpus: the worst and mean per-text
 * cosine, and whether the worst clears the certification threshold. A pair is
 * {@link #certified()} when every compared text scored at least {@link #threshold()}, so a
 * single bad text fails certification.
 *
 * @param providerA id of the first provider
 * @param providerB id of the second provider
 * @param texts number of texts compared
 * @param minCosine lowest per-text cosine seen
 * @param meanCosine mean per-text cosine
 * @param threshold the bar {@code minCosine} had to clear
 * @param certified whether {@code minCosine >= threshold}
 */
public record EquivalenceReport(String providerA, String providerB, int texts,
        double minCosine, double meanCosine, double threshold, boolean certified) {

    public EquivalenceReport {
        Objects.requireNonNull(providerA, "providerA");
        Objects.requireNonNull(providerB, "providerB");
    }

    /**
     * Builds the report from the per-text cosines of a comparison.
     *
     * @throws IllegalArgumentException when {@code cosines} is empty
     */
    public static EquivalenceReport from(String providerA, String providerB,
            List<Double> cosines, double threshold) {
        Objects.requireNonNull(cosines, "cosines");
        if (cosines.isEmpty()) {
            throw new IllegalArgumentException("No cosines to report on");
        }
        double min = Double.POSITIVE_INFINITY;
        double sum = 0;
        for (double cosine : cosines) {
            min = Math.min(min, cosine);
            sum += cosine;
        }
        return new EquivalenceReport(providerA, providerB, cosines.size(), min,
                sum / cosines.size(), threshold, min >= threshold);
    }
}
