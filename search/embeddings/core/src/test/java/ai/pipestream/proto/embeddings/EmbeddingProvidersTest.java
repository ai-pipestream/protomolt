package ai.pipestream.proto.embeddings;

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
}
