package ai.protomolt.proto.search.chunk;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.protomolt.proto.search.index.spi.ChunkingPolicy;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * The {@code opennlp-v1} boundary rule set: OpenNLP sentence detection over
 * the pinned English UD-EWT model, tokenized on Unicode whitespace.
 */
class OpenNlpSentenceRulesTest {

    private static final SentencePackedChunker CHUNKER = new SentencePackedChunker();

    private static ChunkingPolicy.ChunkingSpec openNlpSpec(
            int target, int overlap, int min, int max) {
        return new ChunkingPolicy.ChunkingSpec(
                SentencePackedChunker.STRATEGY,
                SentencePackedChunker.STRATEGY_VERSION,
                target, overlap, min, max,
                OpenNlpSentenceRules.ID);
    }

    @Test
    void segmentsPunctuatedEnglishSentences() {
        List<SentenceRules.Sentence> sentences = OpenNlpSentenceRules.segment(
                "The court affirmed the judgment. The defendant appealed? It was denied!");
        assertThat(sentences).hasSize(3);
        assertThat(sentences.get(0).tokens()).isEqualTo(5);
    }

    @Test
    void blankInputYieldsNoSentences() {
        assertThat(OpenNlpSentenceRules.segment("")).isEmpty();
        assertThat(OpenNlpSentenceRules.segment("   \n\t  ")).isEmpty();
    }

    @Test
    void sentenceSpansCarryNoSurroundingWhitespace() {
        List<SentenceRules.Sentence> sentences =
                OpenNlpSentenceRules.segment("  First sentence here.   Second one follows.  ");
        assertThat(sentences).hasSize(2);
        String text = "  First sentence here.   Second one follows.  ";
        for (SentenceRules.Sentence sentence : sentences) {
            assertThat(text.substring(sentence.start(), sentence.end()))
                    .isEqualTo(text.substring(sentence.start(), sentence.end()).strip());
        }
    }

    @Test
    void tokensBreakOnUnicodeWhitespaceWhereRulesV1DoesNot() {
        // Non-breaking space: Unicode White_Space, but not Character.isWhitespace.
        String text = "one\u00A0two";
        assertThat(OpenNlpSentenceRules.tokens(text, 0, text.length())).hasSize(2);
        assertThat(SentenceRules.tokenSpans(text, 0, text.length())).hasSize(1);
        // The Unicode line separator U+2028 breaks tokens too.
        String separated = "one\u2028two";
        assertThat(OpenNlpSentenceRules.tokens(separated, 0, separated.length())).hasSize(2);
    }

    @Test
    void tokensKeepSupplementaryPlaneCharactersWhole() {
        String text = "party 🎉 time";
        List<BoundaryRules.TokenSpan> tokens = OpenNlpSentenceRules.tokens(text, 0, text.length());
        assertThat(tokens).hasSize(3);
        assertThat(text.substring(tokens.get(1).start(), tokens.get(1).end())).isEqualTo("🎉");
    }

    @Test
    void segmentationIsDeterministic() {
        String text = "The court affirmed. The defendant appealed? Denied! Costs follow.";
        assertThat(OpenNlpSentenceRules.segment(text))
                .isEqualTo(OpenNlpSentenceRules.segment(text));
    }

    @Test
    void theChunkerExecutesOpenNlpV1Policies() {
        String text = "Alpha one two. Beta three four. Gamma five six. Delta seven eight.";
        List<Chunk> chunks = CHUNKER.chunk(text, openNlpSpec(4, 0, 0, 0));
        assertThat(chunks).hasSize(4);
        assertThat(chunks.get(0).tokenCount()).isEqualTo(3);
        assertThat(chunks.get(0).text()).isEqualTo("Alpha one two.");
        // Offsets address the source text.
        for (Chunk chunk : chunks) {
            assertThat(text.substring(chunk.startOffset(), chunk.endOffset())).isEqualTo(chunk.text());
        }
    }

    @Test
    void openNlpV1SplitsOversizeSentencesAtTokenBoundaries() {
        String text = "one two three four five six seven eight nine ten";
        List<Chunk> chunks = CHUNKER.chunk(text, openNlpSpec(4, 0, 0, 4));
        assertThat(chunks).isNotEmpty();
        for (Chunk chunk : chunks) {
            assertThat(chunk.tokenCount()).isLessThanOrEqualTo(4);
        }
    }

    @Test
    void anUnknownBoundaryIdIsRefusedLoudly() {
        ChunkingPolicy.ChunkingSpec spec = new ChunkingPolicy.ChunkingSpec(
                SentencePackedChunker.STRATEGY, SentencePackedChunker.STRATEGY_VERSION,
                10, 0, 0, 0, "rules-v99");
        assertThatThrownBy(() -> CHUNKER.chunk("some text here.", spec))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("rules-v99")
                .hasMessageContaining("rules-v1")
                .hasMessageContaining("opennlp-v1");
    }

    @Test
    void theBoundaryIdIsAPolicyDigestComponent() {
        ChunkingPolicy.EmbeddingSpec embedding = new ChunkingPolicy.EmbeddingSpec(
                "model", 4, ai.protomolt.proto.search.index.spi.VectorSimilarity.COSINE, false);
        ChunkingPolicy rulesV1 = new ChunkingPolicy(
                new ChunkingPolicy.ChunkingSpec("sentence-packed", 1, 10, 0, 0, 0, "rules-v1"),
                embedding, "", false);
        ChunkingPolicy openNlp = new ChunkingPolicy(
                new ChunkingPolicy.ChunkingSpec("sentence-packed", 1, 10, 0, 0, 0, "opennlp-v1"),
                embedding, "", false);
        assertThat(rulesV1.digest()).isNotEqualTo(openNlp.digest());
    }
}
