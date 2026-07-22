package ai.pipestream.proto.rerank.harness;

import ai.pipestream.proto.rerank.RerankProvider;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Certifies that two {@link RerankProvider}s serving the same model produce the same
 * ranking.
 *
 * <p>A runtime can mix two providers (rerank with a different runtime in a fallback path,
 * for example) only when they agree on the order of a query's candidates. Absolute score
 * scales differ across providers (sigmoid probabilities on some, raw logits on others), so
 * certification is on ranking plus top-1 agreement, never on raw score values.
 * {@link #compare} scores every case with both providers and reduces the per-query Kendall
 * tau-b correlations and the argmax agreement count to a {@link RerankEquivalenceReport};
 * the pair is certified when the worst query still clears the caller's threshold.
 */
public final class RerankEquivalence {

    /**
     * Compares {@code a} and {@code b} over {@code cases} with strict scoring: only exactly
     * equal scores tie. Equivalent to
     * {@link #compare(RerankProvider, RerankProvider, List, double, double)} with a score
     * epsilon of 0.
     *
     * @throws IllegalArgumentException when {@code cases} is empty
     */
    public RerankEquivalenceReport compare(RerankProvider a, RerankProvider b,
            List<QueryDocuments> cases, double threshold) {
        return compare(a, b, cases, threshold, 0.0);
    }

    /**
     * Compares {@code a} and {@code b} over {@code cases}: one {@code score} call per
     * provider per case, one Kendall tau-b per case between the two score vectors, and a
     * top-1 check on the argmax indexes, reduced to a {@link RerankEquivalenceReport}.
     *
     * <p>{@code scoreEpsilon} is the noise floor passed to
     * {@link KendallTau#tauB(double[], double[], double)}: score differences at or below it
     * are treated as ties. For sigmoid-scaled providers an epsilon around 1e-3 treats
     * sub-floor jitter (two runtimes rounding the same tiny probability differently) as the
     * same relevance; 0 means strict, only exactly equal scores tie.
     *
     * @throws IllegalArgumentException when {@code cases} is empty or {@code scoreEpsilon}
     *         is negative
     */
    public RerankEquivalenceReport compare(RerankProvider a, RerankProvider b,
            List<QueryDocuments> cases, double threshold, double scoreEpsilon) {
        Objects.requireNonNull(a, "a");
        Objects.requireNonNull(b, "b");
        Objects.requireNonNull(cases, "cases");
        if (cases.isEmpty()) {
            throw new IllegalArgumentException(
                    "An empty case list cannot certify a provider pair");
        }
        if (scoreEpsilon < 0) {
            throw new IllegalArgumentException(
                    "scoreEpsilon must not be negative: " + scoreEpsilon);
        }
        List<Double> taus = new ArrayList<>(cases.size());
        int top1Agreement = 0;
        for (QueryDocuments queryCase : cases) {
            List<Double> scoresA = a.score(queryCase.query(), queryCase.documents());
            List<Double> scoresB = b.score(queryCase.query(), queryCase.documents());
            taus.add(KendallTau.tauB(doubles(scoresA), doubles(scoresB), scoreEpsilon));
            if (argmax(scoresA) == argmax(scoresB)) {
                top1Agreement++;
            }
        }
        return RerankEquivalenceReport.from(a.providerId(), b.providerId(), taus,
                top1Agreement, scoreEpsilon, threshold);
    }

    /** The index of the highest score; ties resolve to the lowest index. */
    private static int argmax(List<Double> scores) {
        int best = 0;
        for (int i = 1; i < scores.size(); i++) {
            if (scores.get(i) > scores.get(best)) {
                best = i;
            }
        }
        return best;
    }

    private static double[] doubles(List<Double> scores) {
        double[] vector = new double[scores.size()];
        for (int i = 0; i < scores.size(); i++) {
            vector[i] = scores.get(i);
        }
        return vector;
    }
}
