package ai.pipestream.proto.rerank;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The ServiceLoader provider seam. {@code FixedScoreRerankProvider} is registered in this
 * module's test {@code META-INF/services}; provider modules register themselves the same way.
 */
class RerankProvidersTest {

    @Test
    void allDiscoversRegisteredProvidersKeyedByProviderId() {
        assertThat(RerankProviders.all()).containsOnlyKeys("fixed-score");
    }

    @Test
    void byIdReturnsTheProviderRegisteredUnderTheId() {
        assertThat(RerankProviders.byId("fixed-score"))
                .isInstanceOf(FixedScoreRerankProvider.class);
    }

    @Test
    void byIdListsTheKnownIdsWhenTheIdIsAbsent() {
        assertThatThrownBy(() -> RerankProviders.byId("no-such-provider"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Unknown rerank provider 'no-such-provider'."
                        + " Available providers: fixed-score");
    }
}
