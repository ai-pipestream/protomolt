package ai.pipestream.proto.embeddings;

import ai.pipestream.proto.index.spi.ChunkingPolicy;
import ai.pipestream.proto.index.spi.VectorSimilarity;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The ServiceLoader provider seam. {@code FixedTableEmbeddingProvider} is registered in this
 * module's test {@code META-INF/services}; provider modules register themselves the same way.
 */
class EmbeddingProvidersTest {

    @Test
    void allDiscoversRegisteredProvidersKeyedByProviderId() {
        assertThat(EmbeddingProviders.all()).containsOnlyKeys("fixed-table");
    }

    @Test
    void byIdReturnsTheProviderRegisteredUnderTheId() {
        assertThat(EmbeddingProviders.byId("fixed-table"))
                .isInstanceOf(FixedTableEmbeddingProvider.class);
    }

    @Test
    void byIdListsTheKnownIdsWhenTheIdIsAbsent() {
        assertThatThrownBy(() -> EmbeddingProviders.byId("no-such-provider"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Unknown embedding provider 'no-such-provider'."
                        + " Available providers: fixed-table");
    }

    @Test
    void forSpecResolvesTheModelAndChecksItsDimension() {
        assertThat(EmbeddingProviders.forSpec(spec("fixed-table", 3)))
                .isInstanceOf(FixedTableEmbeddingProvider.class);
    }

    @Test
    void forSpecRefusesADimensionMismatchByName() {
        assertThatThrownBy(() -> EmbeddingProviders.forSpec(spec("fixed-table", 8)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("fixed-table")
                .hasMessageContaining("3")
                .hasMessageContaining("dims=8");
    }

    private static ChunkingPolicy.EmbeddingSpec spec(String model, int dims) {
        return new ChunkingPolicy.EmbeddingSpec(model, dims, VectorSimilarity.COSINE, true);
    }
}
