package ai.protomolt.proto.search.chunk;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

import ai.protomolt.proto.search.embedding.EmbeddingProvider;
import ai.protomolt.proto.search.index.spi.ChunkingPolicy;
import ai.protomolt.proto.search.index.spi.VectorSimilarity;
import java.util.List;
import org.junit.jupiter.api.Test;

class PolicyDerivationTest {

    /** Deterministic 3-dim provider: components derived from text lengths. */
    private static final class FakeProvider implements EmbeddingProvider {
        @Override
        public String providerId() {
            return "fake";
        }

        @Override
        public int dimension() {
            return 3;
        }

        @Override
        public float[] embed(String text) {
            return new float[] {text.length(), text.length() % 7, 1};
        }
    }

    private static ChunkingPolicy policy(boolean normalize) {
        return new ChunkingPolicy(
                new ChunkingPolicy.ChunkingSpec(
                        SentencePackedChunker.STRATEGY,
                        SentencePackedChunker.STRATEGY_VERSION,
                        8, 0, 0, 0,
                        SentencePackedChunker.BOUNDARY),
                new ChunkingPolicy.EmbeddingSpec("fake", 3, VectorSimilarity.COSINE, normalize),
                "",
                true);
    }

    @Test
    void chunkDerivationUsesDocumentPurpose() {
        EmbeddingProvider asymmetric = new EmbeddingProvider() {
            public String providerId() { return "fake"; }
            public int dimension() { return 3; }
            public float[] embed(String text) { throw new AssertionError("purpose missing"); }
            public float[] embedDocument(String text) { return new float[] {1, 2, 3}; }
        };
        assertThat(new PolicyDerivation(asymmetric).derive("One short sentence.", policy(false)))
                .singleElement().satisfies(chunk -> assertThat(chunk.vector()).containsExactly(1, 2, 3));
    }

    @Test
    void chunksOfOneTextShareOneBatchedDocumentCall() {
        List<List<String>> batches = new java.util.ArrayList<>();
        EmbeddingProvider batched = new EmbeddingProvider() {
            public String providerId() { return "fake"; }
            public int dimension() { return 3; }
            public float[] embed(String text) { throw new AssertionError("purpose missing"); }
            public float[] embedDocument(String text) { throw new AssertionError("batch missing"); }
            public List<float[]> embedAllDocuments(List<String> texts) {
                batches.add(List.copyOf(texts));
                return texts.stream().map(text -> new float[] {text.length(), 0, 1}).toList();
            }
        };
        List<PolicyDerivation.DerivedChunk> derived = new PolicyDerivation(batched)
                .derive("First sentence here now. Second sentence here too. Third one closes it.",
                        policy(false));
        assertThat(derived).hasSize(2);
        assertThat(batches).singleElement().satisfies(batch -> assertThat(batch).hasSize(2));
        for (var chunk : derived) {
            assertThat(chunk.vector()).containsExactly(chunk.chunk().text().length(), 0, 1);
        }
    }

    @Test
    void batchWithTheWrongVectorCountRefuses() {
        EmbeddingProvider undersized = new EmbeddingProvider() {
            public String providerId() { return "fake"; }
            public int dimension() { return 3; }
            public float[] embed(String text) { return new float[] {1, 2, 3}; }
            public List<float[]> embedAllDocuments(List<String> texts) { return List.of(new float[] {1, 2, 3}); }
        };
        assertThatThrownBy(() -> new PolicyDerivation(undersized)
                .derive("First sentence here now. Second sentence here too. Third one closes it.", policy(false)))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("1 vectors for 2 chunks");
    }

    @Test
    void derivesOneVectorPerChunkInOrder() {
        List<PolicyDerivation.DerivedChunk> derived = new PolicyDerivation(new FakeProvider())
                .derive("First sentence here now. Second sentence here too. Third one closes it.",
                        policy(false));
        assertThat(derived).hasSize(2);
        assertThat(derived.getFirst().chunk().ordinal()).isZero();
        assertThat(derived.getFirst().vector())
                .containsExactly(derived.getFirst().chunk().text().length(),
                        derived.getFirst().chunk().text().length() % 7, 1);
    }

    @Test
    void normalizesWhenThePolicySaysSo() {
        List<PolicyDerivation.DerivedChunk> derived = new PolicyDerivation(new FakeProvider())
                .derive("One short sentence.", policy(true));
        double norm = 0;
        for (float component : derived.getFirst().vector()) {
            norm += (double) component * component;
        }
        assertThat(Math.sqrt(norm)).isCloseTo(1.0, within(1e-6));
    }

    @Test
    void wrongModelAndWrongDimsAreRefusedByName() {
        PolicyDerivation derivation = new PolicyDerivation(new FakeProvider());
        ChunkingPolicy wrongModel = new ChunkingPolicy(
                policy(false).chunking(),
                new ChunkingPolicy.EmbeddingSpec("model2vec", 3, VectorSimilarity.COSINE, false),
                "", true);
        assertThatThrownBy(() -> derivation.derive("Text.", wrongModel))
                .hasMessageContaining("model2vec").hasMessageContaining("fake");
        ChunkingPolicy wrongDims = new ChunkingPolicy(
                policy(false).chunking(),
                new ChunkingPolicy.EmbeddingSpec("fake", 8, VectorSimilarity.COSINE, false),
                "", true);
        assertThatThrownBy(() -> derivation.derive("Text.", wrongDims))
                .hasMessageContaining("8").hasMessageContaining("3");
    }

    @Test
    void blankTextDerivesNothing() {
        assertThat(new PolicyDerivation(new FakeProvider()).derive("  ", policy(false))).isEmpty();
    }

    /**
     * A provider whose vector is null or not its declared dimension is broken;
     * the derivation refuses it rather than deriving a corrupt corpus.
     */
    @Test
    void aProviderReturningNullOrWrongDimsFailsHard() {
        EmbeddingProvider nullReturning = new StubProvider(null);
        assertThatThrownBy(() -> new PolicyDerivation(nullReturning)
                .derive("One short sentence.", policy(false)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("null");

        EmbeddingProvider wrongDims = new StubProvider(new float[] {1, 2});
        assertThatThrownBy(() -> new PolicyDerivation(wrongDims)
                .derive("One short sentence.", policy(false)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("2 dims");
    }

    @Test
    void aZeroVectorStaysZeroUnderNormalize() {
        // A zero vector has no direction to normalize; dividing by its zero
        // norm would NaN every component, so it passes through unchanged.
        EmbeddingProvider zero = new StubProvider(new float[] {0, 0, 0});
        List<PolicyDerivation.DerivedChunk> derived = new PolicyDerivation(zero)
                .derive("One short sentence.", policy(true));
        assertThat(derived.getFirst().vector()).containsExactly(0f, 0f, 0f);
    }

    /** A 3-dim provider misbehaving exactly as constructed. */
    private static final class StubProvider implements EmbeddingProvider {
        private final float[] vector;

        private StubProvider(float[] vector) {
            this.vector = vector;
        }

        @Override
        public String providerId() {
            return "fake";
        }

        @Override
        public int dimension() {
            return 3;
        }

        @Override
        public float[] embed(String text) {
            return vector;
        }
    }

    @Test
    void discoverResolvesThePolicysProviderThroughTheServiceLoader() {
        ChunkingPolicy discovered = new ChunkingPolicy(
                policy(false).chunking(),
                new ChunkingPolicy.EmbeddingSpec(DiscoverableTestProvider.PROVIDER_ID,
                        DiscoverableTestProvider.DIMENSION, VectorSimilarity.COSINE, false),
                "", true);
        List<PolicyDerivation.DerivedChunk> derived = PolicyDerivation.discover(discovered)
                .derive("One short sentence.", discovered);
        assertThat(derived).hasSize(1);
        assertThat(derived.getFirst().vector()).hasSize(DiscoverableTestProvider.DIMENSION);
    }

    @Test
    void discoverRefusesAPolicyNamingAnAbsentModel() {
        ChunkingPolicy absent = new ChunkingPolicy(
                policy(false).chunking(),
                new ChunkingPolicy.EmbeddingSpec("no-such-model", 2,
                        VectorSimilarity.COSINE, false),
                "", true);
        assertThatThrownBy(() -> PolicyDerivation.discover(absent))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("no-such-model")
                .hasMessageContaining(DiscoverableTestProvider.PROVIDER_ID);
    }
}
