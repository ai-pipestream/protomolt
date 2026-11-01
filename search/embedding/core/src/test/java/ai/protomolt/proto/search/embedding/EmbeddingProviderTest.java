package ai.protomolt.proto.search.embedding;

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
    void batchedPurposeDefaultsKeepThePurposeInsteadOfTheNeutralBatch() {
        EmbeddingProvider asymmetric = new EmbeddingProvider() {
            public String providerId() { return "asymmetric"; }
            public int dimension() { return 1; }
            public float[] embed(String text) { throw new AssertionError("neutral embedding"); }
            public List<float[]> embedAll(List<String> texts) { throw new AssertionError("neutral batch"); }
            public float[] embedDocument(String text) { return new float[] {1}; }
            public float[] embedQuery(String text) { return new float[] {2}; }
        };

        assertThat(asymmetric.embedAllDocuments(List.of("a", "b"))).containsExactly(new float[] {1}, new float[] {1});
        assertThat(asymmetric.embedAllQueries(List.of("a"))).containsExactly(new float[] {2});
    }

    @Test
    void closeDefaultsToANoOpSoInProcessProvidersNeedNoOverride() {
        EmbeddingProvider provider = new FixedTableEmbeddingProvider();

        provider.close();

        assertThat(provider.embed("hello world")).containsExactly(0.1f, 0.2f, 0.3f);
    }
}
