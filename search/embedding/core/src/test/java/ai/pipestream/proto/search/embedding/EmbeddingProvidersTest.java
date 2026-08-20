package ai.pipestream.proto.search.embedding;

import ai.pipestream.proto.search.index.spi.ChunkingPolicy;
import ai.pipestream.proto.search.index.spi.VectorSimilarity;
import org.junit.jupiter.api.Test;

import java.util.List;

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

    @Test
    void duplicateProviderIdsAreRejectedNotLastWins() {        // Two providers registering the same id are a classpath
        // misconfiguration; silently keeping one could derive a corpus with
        // the wrong model, so discovery must fail and name the id.
        EmbeddingProvider first = new FixedTableEmbeddingProvider();
        EmbeddingProvider second = new FixedTableEmbeddingProvider();

        assertThatThrownBy(() -> EmbeddingProviders.indexById(List.of(first, second)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("fixed-table")
                .hasMessageContaining(FixedTableEmbeddingProvider.class.getName());

        assertThat(EmbeddingProviders.indexById(List.of(first))).containsOnlyKeys("fixed-table");
    }

    @Test
    void selectByIdClosesTheProvidersItDoesNotReturn() {
        RecordingProvider wanted = new RecordingProvider("wanted");
        RecordingProvider discarded = new RecordingProvider("discarded");

        EmbeddingProvider selected =
                EmbeddingProviders.selectById(List.of(discarded, wanted), "wanted");

        assertThat(selected).isSameAs(wanted);
        assertThat(wanted.closed).isFalse();
        assertThat(discarded.closed).isTrue();
    }

    @Test
    void selectByIdClosesEverythingWhenTheIdIsAbsent() {
        RecordingProvider one = new RecordingProvider("one");
        RecordingProvider two = new RecordingProvider("two");

        assertThatThrownBy(() -> EmbeddingProviders.selectById(List.of(one, two), "absent"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("absent");
        assertThat(one.closed).isTrue();
        assertThat(two.closed).isTrue();
    }

    private static final class RecordingProvider implements EmbeddingProvider {
        private final String id;
        boolean closed;

        RecordingProvider(String id) {
            this.id = id;
        }

        @Override
        public String providerId() {
            return id;
        }

        @Override
        public int dimension() {
            return 1;
        }

        @Override
        public float[] embed(String text) {
            return new float[1];
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    private static ChunkingPolicy.EmbeddingSpec spec(String model, int dims) {
        return new ChunkingPolicy.EmbeddingSpec(model, dims, VectorSimilarity.COSINE, true);
    }
}
