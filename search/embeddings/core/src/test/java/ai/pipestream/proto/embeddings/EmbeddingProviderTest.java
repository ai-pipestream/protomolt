package ai.pipestream.proto.embeddings;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class EmbeddingProviderTest {

    @Test
    void embedAllDefaultLoopsOverEmbedInOrder() {
        EmbeddingProvider provider = new FixedTableEmbeddingProvider();

        List<float[]> vectors = provider.embedAll(List.of("hello world", "a memoir"));

        assertThat(vectors).hasSize(2);
        assertThat(vectors.get(0)).containsExactly(0.1f, 0.2f, 0.3f);
        assertThat(vectors.get(1)).containsExactly(0.4f, 0.5f, 0.6f);
    }

    @Test
    void closeDefaultsToANoOpSoInProcessProvidersNeedNoOverride() {
        EmbeddingProvider provider = new FixedTableEmbeddingProvider();

        provider.close();

        assertThat(provider.embed("hello world")).containsExactly(0.1f, 0.2f, 0.3f);
    }
}
