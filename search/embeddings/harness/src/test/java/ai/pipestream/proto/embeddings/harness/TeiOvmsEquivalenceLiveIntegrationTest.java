package ai.pipestream.proto.embeddings.harness;

import ai.pipestream.proto.embeddings.ovms.OvmsEmbeddingProvider;
import ai.pipestream.proto.embeddings.tei.TeiEmbeddingProvider;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Live equivalence check: TEI and OpenVINO Model Server serving the same embedding model must
 * produce near-identical vectors, or a runtime cannot mix the two providers. The suite runs
 * only when all three knobs point at live servers:
 * {@value TeiEmbeddingProvider#TARGET_ENVIRONMENT_VARIABLE},
 * {@value OvmsEmbeddingProvider#TARGET_ENVIRONMENT_VARIABLE}, and
 * {@value OvmsEmbeddingProvider#MODEL_ENVIRONMENT_VARIABLE}; it skips cleanly otherwise.
 */
@Tag("integration")
class TeiOvmsEquivalenceLiveIntegrationTest {

    private static final double THRESHOLD = 0.995;

    private static final List<String> SENTENCES = List.of(
            "The quick brown fox jumps over the lazy dog.",
            "Embedding models map text to points in a vector space.",
            "A cosine near one means two vectors point the same way.",
            "gRPC carries the request to the inference server.",
            "OpenVINO compiles the model for the local CPU.",
            "Text Embeddings Inference batches concurrent requests.",
            "The shipment arrived at the port on Tuesday morning.",
            "She sold seashells by the seashore all summer.",
            "Quantization trades a little accuracy for a lot of speed.",
            "Two runtimes serving one model must agree on the vectors.",
            "The committee published its report after the recess.",
            "A drizzle settled over the valley before dawn.",
            "Tokenization happens on the server, not in the client.",
            "Every shard answers with its own nearest neighbors.",
            "The recipe calls for two cloves of garlic and a bay leaf.",
            "His notebook filled with sketches of bridges and towers.");

    @Test
    void teiAndOvmsServingTheSameModelCertify() {
        String teiTarget = System.getenv(TeiEmbeddingProvider.TARGET_ENVIRONMENT_VARIABLE);
        String ovmsTarget = System.getenv(OvmsEmbeddingProvider.TARGET_ENVIRONMENT_VARIABLE);
        String ovmsModel = System.getenv(OvmsEmbeddingProvider.MODEL_ENVIRONMENT_VARIABLE);
        assumeTrue(teiTarget != null && !teiTarget.isBlank(),
                "Set " + TeiEmbeddingProvider.TARGET_ENVIRONMENT_VARIABLE
                        + " to the TEI server's host:port to run this test");
        assumeTrue(ovmsTarget != null && !ovmsTarget.isBlank(),
                "Set " + OvmsEmbeddingProvider.TARGET_ENVIRONMENT_VARIABLE
                        + " to the OVMS server's host:port to run this test");
        assumeTrue(ovmsModel != null && !ovmsModel.isBlank(),
                "Set " + OvmsEmbeddingProvider.MODEL_ENVIRONMENT_VARIABLE
                        + " to the servable name to run this test");

        System.setProperty(TeiEmbeddingProvider.TARGET_PROPERTY, teiTarget);
        System.setProperty(OvmsEmbeddingProvider.TARGET_PROPERTY, ovmsTarget);
        System.setProperty(OvmsEmbeddingProvider.MODEL_PROPERTY, ovmsModel);
        try (TeiEmbeddingProvider tei = new TeiEmbeddingProvider();
                OvmsEmbeddingProvider ovms = new OvmsEmbeddingProvider()) {
            EquivalenceReport report = EmbeddingEquivalence.compare(
                    tei, ovms, SENTENCES, THRESHOLD);
            assertThat(report.certified()).as("%s", report).isTrue();
        } finally {
            System.clearProperty(TeiEmbeddingProvider.TARGET_PROPERTY);
            System.clearProperty(OvmsEmbeddingProvider.TARGET_PROPERTY);
            System.clearProperty(OvmsEmbeddingProvider.MODEL_PROPERTY);
        }
    }
}
