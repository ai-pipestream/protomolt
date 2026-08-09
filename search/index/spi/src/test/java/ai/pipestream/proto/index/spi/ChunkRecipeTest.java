package ai.pipestream.proto.index.spi;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The recipe is validated eagerly (a bad recipe must fail at plan time, not after a corpus
 * is chunked), and the digest is a public contract: it is SHA-256 over a fixed canonical
 * rendering, so a digest computed today must match one computed by a future release for
 * the same recipe.
 */
class ChunkRecipeTest {

    private static final ChunkRecipe.ChunkingSpec CHUNKING =
            new ChunkRecipe.ChunkingSpec("sentence-packed", 1, 384, 64, 32, 512, "rules-v1");
    private static final ChunkRecipe.EmbeddingSpec EMBEDDING =
            new ChunkRecipe.EmbeddingSpec("test-model-4d", 4, VectorSimilarity.COSINE, true);

    private static ChunkRecipe recipe() {
        return new ChunkRecipe(CHUNKING, EMBEDDING, "body#test-model-4d", true);
    }

    @Test
    void nullChunkingAndEmbeddingAreRejected() {
        assertThatThrownBy(() -> new ChunkRecipe(null, EMBEDDING, "", true))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("chunking");
        assertThatThrownBy(() -> new ChunkRecipe(CHUNKING, null, "", true))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("embedding");
    }

    @Test
    void chunkingSpecRejectsNullAndBlankComponents() {
        assertThatThrownBy(() -> new ChunkRecipe.ChunkingSpec(null, 1, 384, 64, 32, 512, "rules-v1"))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("strategy");
        assertThatThrownBy(() -> new ChunkRecipe.ChunkingSpec(" ", 1, 384, 64, 32, 512, "rules-v1"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("strategy");
        assertThatThrownBy(() -> new ChunkRecipe.ChunkingSpec("sentence-packed", 1, 384, 64, 32, 512, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("boundary");
    }

    @Test
    void embeddingSpecRejectsNullBlankModelAndNonPositiveDims() {
        assertThatThrownBy(() -> new ChunkRecipe.EmbeddingSpec(null, 4, VectorSimilarity.COSINE, true))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("model");
        assertThatThrownBy(() -> new ChunkRecipe.EmbeddingSpec("", 4, VectorSimilarity.COSINE, true))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("model");
        assertThatThrownBy(() -> new ChunkRecipe.EmbeddingSpec("test-model-4d", 0, VectorSimilarity.COSINE, true))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("dims");
        assertThatThrownBy(() -> new ChunkRecipe.EmbeddingSpec("test-model-4d", -1, VectorSimilarity.COSINE, true))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("dims");
    }

    @Test
    void nullVectorFieldDefaultsToEmptyAndNullSimilarityToCosine() {
        ChunkRecipe recipe = new ChunkRecipe(CHUNKING,
                new ChunkRecipe.EmbeddingSpec("test-model-4d", 4, null, true), null, false);

        assertThat(recipe.vectorField()).isEmpty();
        assertThat(recipe.embedding().similarity()).isEqualTo(VectorSimilarity.COSINE);
        assertThat(recipe.storeChunkText()).isFalse();
    }

    /**
     * The canonical rendering is part of the public contract: consumers pin digests across
     * releases, so the digest must stay byte-for-byte SHA-256 over this exact layout.
     */
    @Test
    void digestIsSha256OverTheDocumentedCanonicalRendering() throws Exception {
        String canonical = "chunk-recipe/1\n"
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

        assertThat(recipe().digest()).isEqualTo(expected);
    }

    /** Every component can move a chunk boundary or change a vector, so each must move the digest. */
    @Test
    void digestChangesWhenAnySingleComponentChanges() {
        ChunkRecipe base = recipe();
        List<ChunkRecipe> variants = List.of(
                new ChunkRecipe(new ChunkRecipe.ChunkingSpec("other", 1, 384, 64, 32, 512, "rules-v1"),
                        EMBEDDING, "body#test-model-4d", true),
                new ChunkRecipe(new ChunkRecipe.ChunkingSpec("sentence-packed", 2, 384, 64, 32, 512, "rules-v1"),
                        EMBEDDING, "body#test-model-4d", true),
                new ChunkRecipe(new ChunkRecipe.ChunkingSpec("sentence-packed", 1, 256, 64, 32, 512, "rules-v1"),
                        EMBEDDING, "body#test-model-4d", true),
                new ChunkRecipe(new ChunkRecipe.ChunkingSpec("sentence-packed", 1, 384, 32, 32, 512, "rules-v1"),
                        EMBEDDING, "body#test-model-4d", true),
                new ChunkRecipe(new ChunkRecipe.ChunkingSpec("sentence-packed", 1, 384, 64, 16, 512, "rules-v1"),
                        EMBEDDING, "body#test-model-4d", true),
                new ChunkRecipe(new ChunkRecipe.ChunkingSpec("sentence-packed", 1, 384, 64, 32, 1024, "rules-v1"),
                        EMBEDDING, "body#test-model-4d", true),
                new ChunkRecipe(new ChunkRecipe.ChunkingSpec("sentence-packed", 1, 384, 64, 32, 512, "rules-v2"),
                        EMBEDDING, "body#test-model-4d", true),
                new ChunkRecipe(CHUNKING,
                        new ChunkRecipe.EmbeddingSpec("other-model", 4, VectorSimilarity.COSINE, true),
                        "body#test-model-4d", true),
                new ChunkRecipe(CHUNKING,
                        new ChunkRecipe.EmbeddingSpec("test-model-4d", 8, VectorSimilarity.COSINE, true),
                        "body#test-model-4d", true),
                new ChunkRecipe(CHUNKING,
                        new ChunkRecipe.EmbeddingSpec("test-model-4d", 4, VectorSimilarity.L2, true),
                        "body#test-model-4d", true),
                new ChunkRecipe(CHUNKING,
                        new ChunkRecipe.EmbeddingSpec("test-model-4d", 4, VectorSimilarity.COSINE, false),
                        "body#test-model-4d", true),
                new ChunkRecipe(CHUNKING, EMBEDDING, "other#test-model-4d", true),
                new ChunkRecipe(CHUNKING, EMBEDDING, "body#test-model-4d", false));

        for (ChunkRecipe variant : variants) {
            assertThat(variant.digest()).isNotEqualTo(base.digest());
        }
        assertThat(recipe().digest()).isEqualTo(base.digest());
    }

    @Test
    void recordEqualityFollowsEveryComponent() {
        assertThat(recipe()).isEqualTo(recipe());
        assertThat(recipe().hashCode()).isEqualTo(recipe().hashCode());
        assertThat(new ChunkRecipe(CHUNKING, EMBEDDING, "", true))
                .isNotEqualTo(recipe());
    }
}
