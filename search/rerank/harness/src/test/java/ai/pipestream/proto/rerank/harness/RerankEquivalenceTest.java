package ai.pipestream.proto.rerank.harness;

import ai.pipestream.proto.rerank.RerankProvider;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

class RerankEquivalenceTest {

    private static final QueryDocuments FIVE_DOCS = new QueryDocuments("q",
            List.of("d0", "d1", "d2", "d3", "d4"));

    @Test
    void identicalOrderingsCertifyRegardlessOfScoreScale() {
        // One provider answers sigmoid-like scores, the other raw-logit-like ones; the order
        // is the same, so tau is 1 even though no raw value agrees.
        RerankProvider a = new FixedScoresProvider("a",
                Map.of("q", new double[] {0.99, 0.8, 0.5, 0.2, 0.01}));
        RerankProvider b = new FixedScoresProvider("b",
                Map.of("q", new double[] {12.3, 8.1, 4.0, 1.2, 0.3}));

        RerankEquivalenceReport report = new RerankEquivalence()
                .compare(a, b, List.of(FIVE_DOCS), 0.99);

        assertThat(report.certified()).isTrue();
        assertThat(report.minTau()).isCloseTo(1.0, within(1e-9));
        assertThat(report.meanTau()).isCloseTo(1.0, within(1e-9));
        assertThat(report.queries()).isEqualTo(1);
        assertThat(report.top1Agreement()).isEqualTo(1);
        assertThat(report.providerA()).isEqualTo("a");
        assertThat(report.providerB()).isEqualTo("b");
        assertThat(report.threshold()).isEqualTo(0.99);
    }

    @Test
    void aSingleAdjacentSwapInFiveDocumentsGivesTau08() {
        // Swapping indexes 2 and 3 makes one discordant pair out of ten: (9 - 1) / 10 = 0.8.
        RerankProvider a = new FixedScoresProvider("a",
                Map.of("q", new double[] {0.9, 0.8, 0.7, 0.6, 0.5}));
        RerankProvider b = new FixedScoresProvider("b",
                Map.of("q", new double[] {0.9, 0.8, 0.6, 0.7, 0.5}));

        RerankEquivalenceReport report = new RerankEquivalence()
                .compare(a, b, List.of(FIVE_DOCS), 0.99);

        assertThat(report.minTau()).isCloseTo(0.8, within(1e-9));
        assertThat(report.certified()).isFalse();
        assertThat(report.top1Agreement()).isEqualTo(1);
    }

    @Test
    void fullReversalGivesTauMinusOne() {
        RerankProvider a = new FixedScoresProvider("a",
                Map.of("q", new double[] {5, 4, 3, 2, 1}));
        RerankProvider b = new FixedScoresProvider("b",
                Map.of("q", new double[] {1, 2, 3, 4, 5}));

        RerankEquivalenceReport report = new RerankEquivalence()
                .compare(a, b, List.of(FIVE_DOCS), 0.0);

        assertThat(report.minTau()).isCloseTo(-1.0, within(1e-9));
        assertThat(report.certified()).isFalse();
        assertThat(report.top1Agreement()).isZero();
    }

    @Test
    void top1AgreementCountsAcrossMixedCases() {
        QueryDocuments first = new QueryDocuments("q1", List.of("d0", "d1", "d2"));
        QueryDocuments second = new QueryDocuments("q2", List.of("d0", "d1", "d2"));
        RerankProvider a = new FixedScoresProvider("a", Map.of(
                "q1", new double[] {0.9, 0.1, 0.05},
                "q2", new double[] {0.8, 0.7, 0.1}));
        RerankProvider b = new FixedScoresProvider("b", Map.of(
                "q1", new double[] {0.9, 0.1, 0.05},
                "q2", new double[] {0.1, 0.8, 0.7}));

        RerankEquivalenceReport report = new RerankEquivalence()
                .compare(a, b, List.of(first, second), 0.0);

        assertThat(report.queries()).isEqualTo(2);
        assertThat(report.top1Agreement()).isEqualTo(1);
        // q1 agrees exactly (tau 1); q2 reverses the top pair (tau -1/3).
        assertThat(report.minTau()).isCloseTo(-1.0 / 3.0, within(1e-9));
        assertThat(report.meanTau()).isCloseTo(1.0 / 3.0, within(1e-9));
    }

    @Test
    void constantSidesHitTheZeroDenominatorRule() {
        assertThat(KendallTau.tauB(new double[] {1, 1, 1, 1}, new double[] {1, 1, 1, 1}))
                .isEqualTo(1.0);
        assertThat(KendallTau.tauB(new double[] {1, 1, 1, 1}, new double[] {4, 3, 2, 1}))
                .isEqualTo(0.0);
        assertThat(KendallTau.tauB(new double[] {4, 3, 2, 1}, new double[] {1, 1, 1, 1}))
                .isEqualTo(0.0);
    }

    @Test
    void tiedScoresAreExcludedFromTheDenominator() {
        // One pair tied on a only: two concordant pairs over sqrt((3 - 1) * 3).
        assertThat(KendallTau.tauB(new double[] {1, 1, 2}, new double[] {1, 2, 3}))
                .isCloseTo(2 / Math.sqrt(6), within(1e-9));
    }

    @Test
    void subFloorJitterBecomesATieWithEpsilon() {
        // The observed live shape: TEI's sigmoid floor answers exactly equal floor scores,
        // OVMS jitters them in the seventh decimal in the same order. Strict tau counts the
        // one-sided ties in the correction and caps at 12/sqrt(12*15) ~= 0.894, failing the
        // 0.9 bar; a 1e-3 noise floor treats the jitter as ties on both sides and tau is 1.
        RerankProvider tei = new FixedScoresProvider("tei", Map.of("q",
                new double[] {0.989, 0.526, 0.168, 3.734357e-05, 3.734357e-05, 3.734357e-05}));
        RerankProvider ovms = new FixedScoresProvider("ovms", Map.of("q",
                new double[] {0.989, 0.523, 0.167, 3.741e-05, 3.739e-05, 3.728e-05}));
        QueryDocuments pandas = new QueryDocuments("q",
                List.of("d0", "d1", "d2", "d3", "d4", "d5"));
        RerankEquivalence equivalence = new RerankEquivalence();

        RerankEquivalenceReport strict = equivalence.compare(tei, ovms, List.of(pandas), 0.9);
        assertThat(strict.minTau()).isCloseTo(12 / Math.sqrt(12 * 15), within(1e-9));
        assertThat(strict.certified()).isFalse();
        assertThat(strict.scoreEpsilon()).isEqualTo(0.0);

        RerankEquivalenceReport floored = equivalence.compare(tei, ovms, List.of(pandas),
                0.9, 1e-3);
        assertThat(floored.minTau()).isCloseTo(1.0, within(1e-9));
        assertThat(floored.certified()).isTrue();
        assertThat(floored.scoreEpsilon()).isEqualTo(1e-3);
        assertThat(floored.top1Agreement()).isEqualTo(1);
    }

    @Test
    void aGenuineSwapAboveTheEpsilonStaysDiscordant() {
        // 0.52 vs 0.17 inverted between providers: the 0.35 gap dwarfs the 1e-3 noise
        // floor, so the pair is discordant and certification fails.
        RerankProvider a = new FixedScoresProvider("a",
                Map.of("q", new double[] {0.9, 0.52, 0.17, 0.01}));
        RerankProvider b = new FixedScoresProvider("b",
                Map.of("q", new double[] {0.9, 0.17, 0.52, 0.01}));
        QueryDocuments docs = new QueryDocuments("q", List.of("d0", "d1", "d2", "d3"));

        RerankEquivalenceReport report = new RerankEquivalence()
                .compare(a, b, List.of(docs), 0.9, 1e-3);

        assertThat(report.minTau()).isCloseTo(4.0 / 6.0, within(1e-9));
        assertThat(report.certified()).isFalse();
    }

    @Test
    void tauRejectsANegativeEpsilon() {
        assertThatThrownBy(() -> KendallTau.tauB(
                new double[] {1, 2}, new double[] {1, 2}, -1e-3))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void tauRejectsLengthMismatch() {
        assertThatThrownBy(() -> KendallTau.tauB(new double[] {1, 2}, new double[] {1, 2, 3}))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void tauRejectsFewerThanTwoElements() {
        assertThatThrownBy(() -> KendallTau.tauB(new double[] {1}, new double[] {1}))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void aCaseWithFewerThanTwoDocumentsIsRejected() {
        assertThatThrownBy(() -> new QueryDocuments("q", List.of("only one")))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void anEmptyCaseListIsRejected() {
        RerankProvider a = new FixedScoresProvider("a",
                Map.of("q", new double[] {1, 2}));
        RerankProvider b = new FixedScoresProvider("b",
                Map.of("q", new double[] {1, 2}));

        assertThatThrownBy(() -> new RerankEquivalence().compare(a, b, List.of(), 0.9))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void aReportWithoutTausIsRejected() {
        assertThatThrownBy(() -> RerankEquivalenceReport.from("a", "b", List.of(), 0, 0.0, 0.9))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** Scores from a fixed per-query table so tests can assert exact taus. */
    private static final class FixedScoresProvider implements RerankProvider {

        private final String id;
        private final Map<String, double[]> byQuery;

        private FixedScoresProvider(String id, Map<String, double[]> byQuery) {
            this.id = id;
            this.byQuery = byQuery;
        }

        @Override
        public String providerId() {
            return id;
        }

        @Override
        public List<Double> score(String query, List<String> texts) {
            double[] scores = byQuery.get(query);
            if (scores == null) {
                throw new IllegalArgumentException("No fixture scores for '" + query + "'");
            }
            List<Double> result = new ArrayList<>(scores.length);
            for (double score : scores) {
                result.add(score);
            }
            return result;
        }
    }
}
