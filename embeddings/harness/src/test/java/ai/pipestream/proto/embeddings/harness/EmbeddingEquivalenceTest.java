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
        EquivalenceReport report = new EmbeddingEquivalence().compare(
                new FixedTableProvider("a", TABLE),
                new FixedTableProvider("b", TABLE),
                CORPUS, 0.999);

        assertThat(report.certified()).isTrue();
        assertThat(report.minCosine()).isCloseTo(1.0, within(1e-9));
        assertThat(report.meanCosine()).isCloseTo(1.0, within(1e-9));
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
        EmbeddingEquivalence equivalence = new EmbeddingEquivalence();

        EquivalenceReport report = equivalence.compare(
                new FixedTableProvider("a", TABLE),
                new FixedTableProvider("b", perturbed),
                CORPUS, 0.99);

        assertThat(report.minCosine()).isCloseTo(0.995037, within(1e-5));
        assertThat(report.certified()).isTrue();

        assertThat(equivalence.compare(
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

        EquivalenceReport report = new EmbeddingEquivalence().compare(
                new FixedTableProvider("a", TABLE),
                new FixedTableProvider("b", rotated),
                CORPUS, 0.99);

        assertThat(report.minCosine()).isEqualTo(0.0);
        assertThat(report.certified()).isFalse();
    }

    @Test
    void dimensionMismatchIsRejected() {
        Map<String, float[]> wider = Map.of(
                "alpha", new float[]{1, 0, 0, 0},
                "beta", new float[]{0, 1, 0, 0},
                "gamma", new float[]{0, 0, 1, 0});

        assertThatThrownBy(() -> new EmbeddingEquivalence().compare(
                new FixedTableProvider("a", TABLE),
                new FixedTableProvider("b", wider),
                CORPUS, 0.99))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("a")
                .hasMessageContaining("b");
    }

    @Test
    void emptyCorpusIsRejected() {
        assertThatThrownBy(() -> new EmbeddingEquivalence().compare(
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

        EquivalenceReport report = new EmbeddingEquivalence().compare(
                new FixedTableProvider("a", TABLE),
                new FixedTableProvider("b", withZero),
                CORPUS, 0.0);

        assertThat(report.minCosine()).isEqualTo(0.0);
        assertThat(report.meanCosine()).isCloseTo(2.0 / 3.0, within(1e-9));
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
