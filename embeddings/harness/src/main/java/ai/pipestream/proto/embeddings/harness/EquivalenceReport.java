package ai.pipestream.proto.embeddings.harness;

import java.util.List;
import java.util.Objects;

/**
 * The outcome of comparing two embedding providers over a corpus: the worst and mean per-text
 * cosine, the range of per-text norm ratios, and whether the worst cosine clears the
 * certification threshold. A pair is {@link #certified()} when every compared text scored at
 * least {@link #threshold()}, so a single bad text fails certification.
 *
 * <p>Certification is cosine-only: cosine certifies direction, and the norm ratios expose
 * scale disagreement that matters when an index scores with L2 or dot product. A per-text
 * ratio is {@code norm(a) / norm(b)}, with two zero norms defined as 1.0 (two zero vectors
 * agree on scale) and a nonzero norm over a zero norm as
 * {@link Double#POSITIVE_INFINITY}.
 *
 * @param providerA id of the first provider
 * @param providerB id of the second provider
 * @param texts number of texts compared
 * @param minCosine lowest per-text cosine seen
 * @param meanCosine mean per-text cosine
 * @param minNormRatio lowest per-text norm ratio seen
 * @param maxNormRatio highest per-text norm ratio seen
 * @param threshold the bar {@code minCosine} had to clear
 * @param certified whether {@code minCosine >= threshold}
 */
public record EquivalenceReport(String providerA, String providerB, int texts,
        double minCosine, double meanCosine, double minNormRatio, double maxNormRatio,
        double threshold, boolean certified) {

    public EquivalenceReport {
        Objects.requireNonNull(providerA, "providerA");
        Objects.requireNonNull(providerB, "providerB");
    }

    /**
     * Builds the report from the per-text cosines and norm ratios of a comparison.
     *
     * @throws IllegalArgumentException when {@code cosines} is empty or the two lists differ
     *         in size
     */
    public static EquivalenceReport from(String providerA, String providerB,
            List<Double> cosines, List<Double> normRatios, double threshold) {
        Objects.requireNonNull(cosines, "cosines");
        Objects.requireNonNull(normRatios, "normRatios");
        if (cosines.isEmpty()) {
            throw new IllegalArgumentException("No cosines to report on");
        }
        if (normRatios.size() != cosines.size()) {
            throw new IllegalArgumentException("Per-text list size mismatch: " + cosines.size()
                    + " cosines vs " + normRatios.size() + " norm ratios");
        }
        double minCosine = Double.POSITIVE_INFINITY;
        double sum = 0;
        for (double cosine : cosines) {
            minCosine = Math.min(minCosine, cosine);
            sum += cosine;
        }
        double minRatio = Double.POSITIVE_INFINITY;
        double maxRatio = Double.NEGATIVE_INFINITY;
        for (double ratio : normRatios) {
            minRatio = Math.min(minRatio, ratio);
            maxRatio = Math.max(maxRatio, ratio);
        }
        return new EquivalenceReport(providerA, providerB, cosines.size(), minCosine,
                sum / cosines.size(), minRatio, maxRatio, threshold, minCosine >= threshold);
    }
}
