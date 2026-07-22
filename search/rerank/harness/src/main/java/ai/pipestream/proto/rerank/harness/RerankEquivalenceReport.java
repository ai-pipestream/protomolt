package ai.pipestream.proto.rerank.harness;

import java.util.List;
import java.util.Objects;

/**
 * The outcome of comparing two rerank providers over a set of query cases: how often the
 * top-1 documents agreed, the worst and mean per-query Kendall tau-b, and whether the worst
 * tau clears the certification threshold. A pair is {@link #certified()} when every compared
 * query scored at least {@link #threshold()}, so a single bad query fails certification.
 *
 * @param providerA id of the first provider
 * @param providerB id of the second provider
 * @param queries number of query cases compared
 * @param top1Agreement number of cases where both providers' argmax indexes agreed
 * @param minTau lowest per-query tau-b seen
 * @param meanTau mean per-query tau-b
 * @param scoreEpsilon the noise-floor epsilon the taus were computed with
 * @param threshold the bar {@code minTau} had to clear
 * @param certified whether {@code minTau >= threshold}
 */
public record RerankEquivalenceReport(String providerA, String providerB, int queries,
        int top1Agreement, double minTau, double meanTau, double scoreEpsilon,
        double threshold, boolean certified) {

    public RerankEquivalenceReport {
        Objects.requireNonNull(providerA, "providerA");
        Objects.requireNonNull(providerB, "providerB");
    }

    /**
     * Builds the report from the per-query taus and the top-1 agreement count of a
     * comparison.
     *
     * @throws IllegalArgumentException when {@code taus} is empty
     */
    public static RerankEquivalenceReport from(String providerA, String providerB,
            List<Double> taus, int top1Agreement, double scoreEpsilon, double threshold) {
        Objects.requireNonNull(taus, "taus");
        if (taus.isEmpty()) {
            throw new IllegalArgumentException("No taus to report on");
        }
        double min = Double.POSITIVE_INFINITY;
        double sum = 0;
        for (double tau : taus) {
            min = Math.min(min, tau);
            sum += tau;
        }
        return new RerankEquivalenceReport(providerA, providerB, taus.size(), top1Agreement,
                min, sum / taus.size(), scoreEpsilon, threshold, min >= threshold);
    }
}
