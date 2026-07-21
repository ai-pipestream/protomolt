package ai.pipestream.proto.embeddings.harness;

import ai.pipestream.proto.embeddings.EmbeddingProvider;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

class EmbeddingEquivalenceTest {

    private static final List<String> CORPUS = List.of("alpha", "beta", "gamma");

    private static final Map<String, float[]> TABLE = Map.of(
            "alpha", new float[]{1, 0, 0},
            "beta", new float[]{0, 1, 0},
            "gamma", new float[]{0, 0, 1});

    @Test
    void identicalTablesCertify() {
        EquivalenceReport report = EmbeddingEquivalence.compare(
                new FixedTableProvider("a", TABLE),
                new FixedTableProvider("b", TABLE),
                CORPUS, 0.999);

        assertThat(report.certified()).isTrue();
        assertThat(report.minCosine()).isCloseTo(1.0, within(1e-9));
        assertThat(report.meanCosine()).isCloseTo(1.0, within(1e-9));
        assertThat(report.minNormRatio()).isCloseTo(1.0, within(1e-9));
        assertThat(report.maxNormRatio()).isCloseTo(1.0, within(1e-9));
        assertThat(report.texts()).isEqualTo(3);
        assertThat(report.providerA()).isEqualTo("a");
        assertThat(report.providerB()).isEqualTo("b");
        assertThat(report.threshold()).isEqualTo(0.999);
    }

    @Test
    void slightlyPerturbedTableCertifiesAt99ButNotAt9999() {
        // cosine((1,0,0), (1,0.1,0)) = 1/sqrt(1.01) ~= 0.99504.
        Map<String, float[]> perturbed = Map.of(
                "alpha", new float[]{1, 0.1f, 0},
                "beta", new float[]{0.1f, 1, 0},
                "gamma", new float[]{0, 0.1f, 1});

        EquivalenceReport report = EmbeddingEquivalence.compare(
                new FixedTableProvider("a", TABLE),
                new FixedTableProvider("b", perturbed),
                CORPUS, 0.99);

        assertThat(report.minCosine()).isCloseTo(0.995037, within(1e-5));
        assertThat(report.certified()).isTrue();

        assertThat(EmbeddingEquivalence.compare(
                new FixedTableProvider("a", TABLE),
                new FixedTableProvider("b", perturbed),
                CORPUS, 0.9999).certified()).isFalse();
    }

    @Test
    void orthogonalVectorsDoNotCertify() {
        Map<String, float[]> rotated = Map.of(
                "alpha", new float[]{0, 1, 0},
                "beta", new float[]{0, 0, 1},
                "gamma", new float[]{1, 0, 0});

        EquivalenceReport report = EmbeddingEquivalence.compare(
                new FixedTableProvider("a", TABLE),
                new FixedTableProvider("b", rotated),
                CORPUS, 0.99);

        assertThat(report.minCosine()).isEqualTo(0.0);
        assertThat(report.certified()).isFalse();
    }

    @Test
    void doubledVectorsCertifyButExposeTheScaleDisagreement() {
        // Cosine is scale-invariant, so a provider returning 2x vectors still certifies;
        // the norm ratios are what surface the disagreement.
        Map<String, float[]> doubled = Map.of(
                "alpha", new float[]{2, 0, 0},
                "beta", new float[]{0, 2, 0},
                "gamma", new float[]{0, 0, 2});

        EquivalenceReport report = EmbeddingEquivalence.compare(
                new FixedTableProvider("a", TABLE),
                new FixedTableProvider("b", doubled),
                CORPUS, 0.999);

        assertThat(report.certified()).isTrue();
        assertThat(report.minCosine()).isCloseTo(1.0, within(1e-9));
        assertThat(report.minNormRatio()).isCloseTo(0.5, within(1e-9));
        assertThat(report.maxNormRatio()).isCloseTo(0.5, within(1e-9));
    }

    @Test
    void zeroVectorNormRatiosFollowTheDocumentedConventions() {
        Map<String, float[]> left = Map.of(
                "alpha", new float[]{0, 0, 0},
                "beta", new float[]{0, 1, 0},
                "gamma", new float[]{0, 0, 1});
        Map<String, float[]> right = Map.of(
                "alpha", new float[]{0, 0, 0},
                "beta", new float[]{0, 0, 0},
                "gamma", new float[]{0, 0, 1});

        // alpha: 0/0 -> 1.0, beta: nonzero/0 -> +Infinity, gamma: 1/1 -> 1.0.
        EquivalenceReport report = EmbeddingEquivalence.compare(
                new FixedTableProvider("a", left),
                new FixedTableProvider("b", right),
                CORPUS, 0.0);

        assertThat(report.minNormRatio()).isEqualTo(1.0);
        assertThat(report.maxNormRatio()).isEqualTo(Double.POSITIVE_INFINITY);
    }

    @Test
    void dimensionMismatchIsRejected() {
        Map<String, float[]> wider = Map.of(
                "alpha", new float[]{1, 0, 0, 0},
                "beta", new float[]{0, 1, 0, 0},
                "gamma", new float[]{0, 0, 1, 0});

        assertThatThrownBy(() -> EmbeddingEquivalence.compare(
                new FixedTableProvider("a", TABLE),
                new FixedTableProvider("b", wider),
                CORPUS, 0.99))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("a")
                .hasMessageContaining("b");
    }

    @Test
    void emptyCorpusIsRejected() {
        assertThatThrownBy(() -> EmbeddingEquivalence.compare(
                new FixedTableProvider("a", TABLE),
                new FixedTableProvider("b", TABLE),
                List.of(), 0.99))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void zeroVectorTextYieldsCosineZero() {
        Map<String, float[]> withZero = Map.of(
                "alpha", new float[]{0, 0, 0},
                "beta", new float[]{0, 1, 0},
                "gamma", new float[]{0, 0, 1});

        EquivalenceReport report = EmbeddingEquivalence.compare(
                new FixedTableProvider("a", TABLE),
                new FixedTableProvider("b", withZero),
                CORPUS, 0.0);

        assertThat(report.minCosine()).isEqualTo(0.0);
        assertThat(report.meanCosine()).isCloseTo(2.0 / 3.0, within(1e-9));
    }

    @Test
    void reportRejectsCosinesAndNormRatiosOfDifferentSizes() {
        assertThatThrownBy(() -> EquivalenceReport.from("a", "b",
                List.of(1.0, 1.0), List.of(1.0), 0.99))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("2")
                .hasMessageContaining("1");
    }

    @Test
    void cosineRejectsLengthMismatch() {
        assertThatThrownBy(() -> Cosines.cosine(new float[]{1, 0}, new float[]{1, 0, 0}))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void cosineTreatsAZeroVectorAsMaximallyDissimilar() {
        assertThat(Cosines.cosine(new float[]{0, 0}, new float[]{1, 2})).isEqualTo(0.0);
        assertThat(Cosines.cosine(new float[]{1, 2}, new float[]{0, 0})).isEqualTo(0.0);
    }

    @Test
    void normIsTheEuclideanLength() {
        assertThat(Cosines.norm(new float[]{3, 4})).isEqualTo(5.0);
        assertThat(Cosines.norm(new float[]{0, 0, 0})).isEqualTo(0.0);
    }

    /** Embeds from a fixed table so tests can assert exact cosines. */
    private static final class FixedTableProvider implements EmbeddingProvider {

        private final String id;
        private final Map<String, float[]> table;

        private FixedTableProvider(String id, Map<String, float[]> table) {
            this.id = id;
            this.table = table;
        }

        @Override
        public String providerId() {
            return id;
        }

        @Override
        public int dimension() {
            return table.values().iterator().next().length;
        }

        @Override
        public float[] embed(String text) {
            float[] vector = table.get(text);
            if (vector == null) {
                throw new IllegalArgumentException("No fixture vector for '" + text + "'");
            }
            return vector;
        }
    }
}
