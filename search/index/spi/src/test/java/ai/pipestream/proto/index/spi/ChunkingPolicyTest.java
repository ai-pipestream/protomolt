package ai.pipestream.proto.index.spi;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The policy is validated eagerly (a bad policy must fail at mapping time, not after a corpus
 * is chunked), and the digest is a public contract: it is SHA-256 over a fixed canonical
 * rendering, so a digest computed today must match one computed by a future release for
 * the same policy.
 */
class ChunkingPolicyTest {

    private static final ChunkingPolicy.ChunkingSpec CHUNKING =
            new ChunkingPolicy.ChunkingSpec("sentence-packed", 1, 384, 64, 32, 512, "rules-v1");
    private static final ChunkingPolicy.EmbeddingSpec EMBEDDING =
            new ChunkingPolicy.EmbeddingSpec("test-model-4d", 4, VectorSimilarity.COSINE, true);

    private static ChunkingPolicy policy() {
        return new ChunkingPolicy(CHUNKING, EMBEDDING, "body#test-model-4d", true);
    }

    @Test
    void nullChunkingAndEmbeddingAreRejected() {
        assertThatThrownBy(() -> new ChunkingPolicy(null, EMBEDDING, "", true))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("chunking");
        assertThatThrownBy(() -> new ChunkingPolicy(CHUNKING, null, "", true))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("embedding");
    }

    @Test
    void chunkingSpecRejectsNullAndBlankComponents() {
        assertThatThrownBy(() -> new ChunkingPolicy.ChunkingSpec(null, 1, 384, 64, 32, 512, "rules-v1"))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("strategy");
        assertThatThrownBy(() -> new ChunkingPolicy.ChunkingSpec(" ", 1, 384, 64, 32, 512, "rules-v1"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("strategy");
        assertThatThrownBy(() -> new ChunkingPolicy.ChunkingSpec("sentence-packed", 1, 384, 64, 32, 512, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("boundary");
    }

    @Test
    void embeddingSpecRejectsNullBlankModelAndNonPositiveDims() {
        assertThatThrownBy(() -> new ChunkingPolicy.EmbeddingSpec(null, 4, VectorSimilarity.COSINE, true))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("model");
        assertThatThrownBy(() -> new ChunkingPolicy.EmbeddingSpec("", 4, VectorSimilarity.COSINE, true))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("model");
        assertThatThrownBy(() -> new ChunkingPolicy.EmbeddingSpec("test-model-4d", 0, VectorSimilarity.COSINE, true))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("dims");
        assertThatThrownBy(() -> new ChunkingPolicy.EmbeddingSpec("test-model-4d", -1, VectorSimilarity.COSINE, true))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("dims");
    }

    @Test
    void nullVectorFieldDefaultsToEmptyAndNullSimilarityToCosine() {
        ChunkingPolicy policy = new ChunkingPolicy(CHUNKING,
                new ChunkingPolicy.EmbeddingSpec("test-model-4d", 4, null, true), null, false);

        assertThat(policy.vectorField()).isEmpty();
        assertThat(policy.embedding().similarity()).isEqualTo(VectorSimilarity.COSINE);
        assertThat(policy.storeChunkText()).isFalse();
    }

    /**
     * The canonical rendering is part of the public contract: consumers pin digests across
     * releases, so the digest must stay byte-for-byte SHA-256 over this exact layout.
     */
    @Test
    void digestIsSha256OverTheDocumentedCanonicalRendering() throws Exception {
        String canonical = "chunking-policy/1\n"
                + "chunking.strategy=sentence-packed\n"
                + "chunking.strategyVersion=1\n"
                + "chunking.targetTokens=384\n"
                + "chunking.overlapTokens=64\n"
                + "chunking.minTokens=32\n"
                + "chunking.maxTokens=512\n"
                + "chunking.boundary=rules-v1\n"
                + "embedding.model=test-model-4d\n"
                + "embedding.dims=4\n"
                + "embedding.similarity=COSINE\n"
                + "embedding.normalize=true\n"
                + "vectorField=body#test-model-4d\n"
                + "storeChunkText=true\n";
        String expected = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                .digest(canonical.getBytes(StandardCharsets.UTF_8)));

        assertThat(policy().digest()).isEqualTo(expected);
    }

    /** Every component can move a chunk boundary or change a vector, so each must move the digest. */
    @Test
    void digestChangesWhenAnySingleComponentChanges() {
        ChunkingPolicy base = policy();
        List<ChunkingPolicy> variants = List.of(
                new ChunkingPolicy(new ChunkingPolicy.ChunkingSpec("other", 1, 384, 64, 32, 512, "rules-v1"),
                        EMBEDDING, "body#test-model-4d", true),
                new ChunkingPolicy(new ChunkingPolicy.ChunkingSpec("sentence-packed", 2, 384, 64, 32, 512, "rules-v1"),
                        EMBEDDING, "body#test-model-4d", true),
                new ChunkingPolicy(new ChunkingPolicy.ChunkingSpec("sentence-packed", 1, 256, 64, 32, 512, "rules-v1"),
                        EMBEDDING, "body#test-model-4d", true),
                new ChunkingPolicy(new ChunkingPolicy.ChunkingSpec("sentence-packed", 1, 384, 32, 32, 512, "rules-v1"),
                        EMBEDDING, "body#test-model-4d", true),
                new ChunkingPolicy(new ChunkingPolicy.ChunkingSpec("sentence-packed", 1, 384, 64, 16, 512, "rules-v1"),
                        EMBEDDING, "body#test-model-4d", true),
                new ChunkingPolicy(new ChunkingPolicy.ChunkingSpec("sentence-packed", 1, 384, 64, 32, 1024, "rules-v1"),
                        EMBEDDING, "body#test-model-4d", true),
                new ChunkingPolicy(new ChunkingPolicy.ChunkingSpec("sentence-packed", 1, 384, 64, 32, 512, "rules-v2"),
                        EMBEDDING, "body#test-model-4d", true),
                new ChunkingPolicy(CHUNKING,
                        new ChunkingPolicy.EmbeddingSpec("other-model", 4, VectorSimilarity.COSINE, true),
                        "body#test-model-4d", true),
                new ChunkingPolicy(CHUNKING,
                        new ChunkingPolicy.EmbeddingSpec("test-model-4d", 8, VectorSimilarity.COSINE, true),
                        "body#test-model-4d", true),
                new ChunkingPolicy(CHUNKING,
                        new ChunkingPolicy.EmbeddingSpec("test-model-4d", 4, VectorSimilarity.L2, true),
                        "body#test-model-4d", true),
                new ChunkingPolicy(CHUNKING,
                        new ChunkingPolicy.EmbeddingSpec("test-model-4d", 4, VectorSimilarity.COSINE, false),
                        "body#test-model-4d", true),
                new ChunkingPolicy(CHUNKING, EMBEDDING, "other#test-model-4d", true),
                new ChunkingPolicy(CHUNKING, EMBEDDING, "body#test-model-4d", false));

        for (ChunkingPolicy variant : variants) {
            assertThat(variant.digest()).isNotEqualTo(base.digest());
        }
        assertThat(policy().digest()).isEqualTo(base.digest());
    }

    @Test
    void recordEqualityFollowsEveryComponent() {
        assertThat(policy()).isEqualTo(policy());
        assertThat(policy().hashCode()).isEqualTo(policy().hashCode());
        assertThat(new ChunkingPolicy(CHUNKING, EMBEDDING, "", true))
                .isNotEqualTo(policy());
    }
}
