package ai.pipestream.proto.embeddings.ovms;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * The provider against a live OVMS embeddings servable: a BYTES string batch goes out and
 * FP32 vectors come back, dimensions match the response shape, and the vector space behaves
 * (a text is identical to itself, related texts outscore unrelated ones). The suite runs only
 * when {@value OvmsEmbeddingProvider#TARGET_ENVIRONMENT_VARIABLE} and
 * {@value OvmsEmbeddingProvider#MODEL_ENVIRONMENT_VARIABLE} point at a live server; it skips
 * cleanly otherwise. Tensor names default to the provider's; PROTOMOLT_OVMS_INPUT and
 * PROTOMOLT_OVMS_OUTPUT override them for servables that name tensors differently.
 */
@Tag("integration")
class OvmsLiveIntegrationTest {

    @Test
    void liveServableEmbedsABatchIntoABehavingVectorSpace() {
        String target = System.getenv(OvmsEmbeddingProvider.TARGET_ENVIRONMENT_VARIABLE);
        String model = System.getenv(OvmsEmbeddingProvider.MODEL_ENVIRONMENT_VARIABLE);
        assumeTrue(target != null && !target.isBlank(),
                "Set " + OvmsEmbeddingProvider.TARGET_ENVIRONMENT_VARIABLE
                        + " to the OVMS server's host:port to run this test");
        assumeTrue(model != null && !model.isBlank(),
                "Set " + OvmsEmbeddingProvider.MODEL_ENVIRONMENT_VARIABLE
                        + " to the servable name to run this test");

        override(OvmsEmbeddingProvider.INPUT_NAME_PROPERTY, "PROTOMOLT_OVMS_INPUT");
        override(OvmsEmbeddingProvider.OUTPUT_NAME_PROPERTY, "PROTOMOLT_OVMS_OUTPUT");
        try (OvmsEmbeddingProvider provider = new OvmsEmbeddingProvider(target, model)) {
            List<float[]> vectors = provider.embedAll(List.of(
                    "the dog sat on the mat",
                    "the dog sat on the mat",
                    "a puppy plays in the yard",
                    "quarterly earnings beat analyst expectations"));

            assertThat(provider.dimension()).isEqualTo(vectors.get(0).length);
            assertThat(vectors).hasSize(4).allSatisfy(v -> assertThat(v).hasSize(provider.dimension()));

            double identical = cosine(vectors.get(0), vectors.get(1));
            double related = cosine(vectors.get(0), vectors.get(2));
            double unrelated = cosine(vectors.get(0), vectors.get(3));
            assertThat(identical).isCloseTo(1.0, within(1e-4));
            assertThat(related).isGreaterThan(unrelated);
        } finally {
            System.clearProperty(OvmsEmbeddingProvider.INPUT_NAME_PROPERTY);
            System.clearProperty(OvmsEmbeddingProvider.OUTPUT_NAME_PROPERTY);
        }
    }

    private static void override(String property, String environmentVariable) {
        String value = System.getenv(environmentVariable);
        if (value != null && !value.isBlank()) {
            System.setProperty(property, value);
        }
    }

    private static double cosine(float[] a, float[] b) {
        double dot = 0;
        double normA = 0;
        double normB = 0;
        for (int i = 0; i < a.length; i++) {
            dot += a[i] * b[i];
            normA += a[i] * a[i];
            normB += b[i] * b[i];
        }
        return dot / (Math.sqrt(normA) * Math.sqrt(normB));
    }
}
