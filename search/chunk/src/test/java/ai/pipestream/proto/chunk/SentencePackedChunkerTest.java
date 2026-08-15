package ai.pipestream.proto.chunk;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.pipestream.proto.index.spi.ChunkingPolicy;
import java.util.List;
import org.junit.jupiter.api.Test;

class SentencePackedChunkerTest {

    private static final SentencePackedChunker CHUNKER = new SentencePackedChunker();

    private static ChunkingPolicy.ChunkingSpec spec(
            int target, int overlap, int min, int max) {
        return new ChunkingPolicy.ChunkingSpec(
                SentencePackedChunker.STRATEGY,
                SentencePackedChunker.STRATEGY_VERSION,
                target, overlap, min, max,
                SentencePackedChunker.BOUNDARY);
    }

    /** Ten sentences of five tokens each, distinct first words. */
    private static String corpus() {
        StringBuilder text = new StringBuilder();
        for (int i = 0; i < 10; i++) {
            text.append("Sentence").append(i).append(" has exactly five tokens. ");
        }
        return text.toString();
    }

    @Test
    void packsSentencesTowardTheTargetWithoutSplittingThem() {
        List<Chunk> chunks = CHUNKER.chunk(corpus(), spec(12, 0, 0, 0));
        // Five tokens per sentence: two sentences (10 tokens) fit, a third
        // (15) would exceed the target of 12.
        assertThat(chunks).hasSize(5);
        assertThat(chunks).allSatisfy(chunk -> assertThat(chunk.tokenCount()).isEqualTo(10));
        assertThat(chunks.getFirst().text())
                .startsWith("Sentence0").contains("Sentence1").doesNotContain("Sentence2");
    }

    @Test
    void overlapRepeatsTrailingSentencesInOrder() {
        List<Chunk> chunks = CHUNKER.chunk(corpus(), spec(12, 5, 0, 0));
        // Each chunk after the first re-includes the previous chunk's last
        // sentence (5 tokens fits the overlap budget exactly).
        assertThat(chunks.get(1).text()).startsWith("Sentence1 has exactly five tokens.");
        assertThat(chunks.get(1).text()).contains("Sentence2");
        // Offsets overlap by design and stay faithful to the source.
        assertThat(chunks.get(1).startOffset()).isLessThan(chunks.get(0).endOffset());
        String source = corpus();
        for (Chunk chunk : chunks) {
            assertThat(source.substring(chunk.startOffset(), chunk.endOffset()))
                    .isEqualTo(chunk.text());
        }
    }

    @Test
    void aSentenceLargerThanMaxSplitsAtTokenBoundaries() {
        String text = "one two three four five six seven eight nine ten";
        List<Chunk> chunks = CHUNKER.chunk(text, spec(4, 0, 0, 4));
        assertThat(chunks).hasSize(3);
        assertThat(chunks.get(0).text()).isEqualTo("one two three four");
        assertThat(chunks.get(1).text()).isEqualTo("five six seven eight");
        assertThat(chunks.get(2).text()).isEqualTo("nine ten");
        assertThat(chunks).allSatisfy(chunk ->
                assertThat(chunk.tokenCount()).isLessThanOrEqualTo(4));
    }

    @Test
    void anUndersizedTailMergesIntoItsPredecessor() {
        // Three sentences of 5 tokens; target 10 packs two, leaving a
        // 5-token tail below the min of 6, which merges back.
        String text = "Alpha beta gamma delta one. Epsilon zeta eta theta two. Iota kappa lambda mu three.";
        List<Chunk> chunks = CHUNKER.chunk(text, spec(10, 0, 6, 0));
        assertThat(chunks).hasSize(1);
        assertThat(chunks.getFirst().tokenCount()).isEqualTo(15);
    }

    @Test
    void theMaxOutranksTheMinWhenMergingWouldOverflow() {
        String text = "Alpha beta gamma delta one. Epsilon zeta eta theta two. Iota kappa lambda mu three.";
        List<Chunk> chunks = CHUNKER.chunk(text, spec(10, 0, 6, 12));
        // Merging the 5-token tail into the 10-token chunk would exceed the
        // max of 12, so the undersized tail is emitted after all.
        assertThat(chunks).hasSize(2);
        assertThat(chunks.getLast().tokenCount()).isEqualTo(5);
    }

    @Test
    void identicalInputsYieldIdenticalChunks() {
        List<Chunk> first = CHUNKER.chunk(corpus(), spec(12, 5, 3, 20));
        List<Chunk> second = CHUNKER.chunk(corpus(), spec(12, 5, 3, 20));
        assertThat(first).isEqualTo(second);
    }

    @Test
    void blankTextYieldsNoChunks() {
        assertThat(CHUNKER.chunk("", spec(10, 0, 0, 0))).isEmpty();
        assertThat(CHUNKER.chunk("   \n  ", spec(10, 0, 0, 0))).isEmpty();
        assertThat(CHUNKER.chunk(null, spec(10, 0, 0, 0))).isEmpty();
    }

    @Test
    void foreignPoliciesAreRefusedByName() {
        assertThatThrownBy(() -> CHUNKER.chunk("text.", new ChunkingPolicy.ChunkingSpec(
                "token-window", 1, 10, 0, 0, 0, SentencePackedChunker.BOUNDARY)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("token-window");
        assertThatThrownBy(() -> CHUNKER.chunk("text.", new ChunkingPolicy.ChunkingSpec(
                SentencePackedChunker.STRATEGY, 2, 10, 0, 0, 0, SentencePackedChunker.BOUNDARY)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("version");
        assertThatThrownBy(() -> CHUNKER.chunk("text.", new ChunkingPolicy.ChunkingSpec(
                SentencePackedChunker.STRATEGY, 1, 10, 0, 0, 0, "rules-v2")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("rules-v2");
    }

    @Test
    void invalidBudgetsAreRefusedByName() {
        assertThatThrownBy(() -> CHUNKER.chunk("text.", spec(0, 0, 0, 0)))
                .hasMessageContaining("targetTokens");
        assertThatThrownBy(() -> CHUNKER.chunk("text.", spec(10, 10, 0, 0)))
                .hasMessageContaining("overlapTokens");
        assertThatThrownBy(() -> CHUNKER.chunk("text.", spec(10, 0, 0, 5)))
                .hasMessageContaining("maxTokens");
    }

    /**
     * The frozen v1 golden: hand-computed boundaries for a fixed corpus.
     * If this test ever needs an update, that IS a behavior change and
     * STRATEGY_VERSION must bump with it.
     */
    @Test
    void goldenBoundariesForVersionOne() {
        String text = "The court convened at nine. Counsel presented three motions in turn.\n\n"
                + "The first motion was denied! The second, after argument, was granted. "
                + "Was the third even heard? No.";
        List<Chunk> chunks = CHUNKER.chunk(text, spec(15, 4, 3, 0));
        assertThat(chunks).extracting(Chunk::text).containsExactly(
                "The court convened at nine. Counsel presented three motions in turn.",
                "The first motion was denied! The second, after argument, was granted.",
                "Was the third even heard? No.");
        assertThat(chunks).extracting(Chunk::tokenCount).containsExactly(11, 11, 6);
        assertThat(chunks).extracting(Chunk::startOffset).containsExactly(0, 70, 140);
    }
}
