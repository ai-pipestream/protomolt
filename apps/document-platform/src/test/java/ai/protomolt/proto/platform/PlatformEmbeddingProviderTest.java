package ai.protomolt.proto.platform;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.protomolt.proto.search.embedding.EmbeddingProvider;
import org.junit.jupiter.api.Test;

class PlatformEmbeddingProviderTest {
    public static final class Provider implements EmbeddingProvider {
        public String providerId() { return "platform-test-pinned-bundle"; }
        public int dimension() { return 13; }
        public float[] embed(String text) { throw new AssertionError("mount must not embed source content"); }
    }

    @Test
    void explicitSelectionKeepsProviderIdentityInPolicy() {
        var policy = DocumentPlatform.defaultChunkingPolicy("platform-test-pinned-bundle");
        assertThat(policy.embedding().model()).isEqualTo("platform-test-pinned-bundle");
        assertThat(policy.embedding().dims()).isEqualTo(13);
    }

    @Test
    void invalidOrAbsentInstalledProviderDoesNotFallBack() {
        for (String value : new String[] {"", " ", " platform-test-pinned-bundle", "not-installed"}) {
            assertThatThrownBy(() -> DocumentPlatform.defaultChunkingPolicy(value))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }
}
