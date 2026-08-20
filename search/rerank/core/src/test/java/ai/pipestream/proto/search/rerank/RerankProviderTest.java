package ai.pipestream.proto.search.rerank;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RerankProviderTest {

    @Test
    void rankDefaultSortsByDescendingScoreAndTruncates() {
        RerankProvider provider = new FixedScoreRerankProvider();

        List<ScoredText> ranked = provider.rank("q",
                List.of("quarterly earnings", "bamboo shoots", "a memoir"), 2);

        assertThat(ranked).containsExactly(
                new ScoredText(1, "bamboo shoots", 0.9),
                new ScoredText(2, "a memoir", 0.4));
    }

    @Test
    void rankDefaultKeepsEveryTextWhenTopKCoversTheBatch() {
        RerankProvider provider = new FixedScoreRerankProvider();

        List<ScoredText> ranked = provider.rank("q",
                List.of("quarterly earnings", "bamboo shoots", "a memoir"), 10);

        assertThat(ranked).hasSize(3);
        assertThat(ranked.get(0).text()).isEqualTo("bamboo shoots");
    }

    @Test
    void rankRejectsANegativeTopK() {
        RerankProvider provider = new FixedScoreRerankProvider();

        assertThatThrownBy(() -> provider.rank("q", List.of("a memoir"), -1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rankKeepsInputOrderBetweenTiedScores() {
        RerankProvider provider = new AllTiedRerankProvider();

        List<ScoredText> ranked = provider.rank("q", List.of("first", "second", "third"), 3);

        assertThat(ranked).containsExactly(
                new ScoredText(0, "first", 0.5),
                new ScoredText(1, "second", 0.5),
                new ScoredText(2, "third", 0.5));
    }

    @Test
    void rankWithAZeroTopKReturnsEmpty() {
        RerankProvider provider = new FixedScoreRerankProvider();

        assertThat(provider.rank("q", List.of("bamboo shoots"), 0)).isEmpty();
    }

    @Test
    void rankOverAnEmptyBatchReturnsEmpty() {
        RerankProvider provider = new FixedScoreRerankProvider();

        assertThat(provider.rank("q", List.of(), 5)).isEmpty();
    }

    /** Scores every text the same, so every comparison is a tie. */
    private static final class AllTiedRerankProvider implements RerankProvider {

        @Override
        public String providerId() {
            return "all-tied";
        }

        @Override
        public List<Double> score(String query, List<String> texts) {
            return texts.stream().map(text -> 0.5).toList();
        }
    }
}
