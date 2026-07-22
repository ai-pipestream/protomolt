package ai.pipestream.proto.rerank;

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
}
