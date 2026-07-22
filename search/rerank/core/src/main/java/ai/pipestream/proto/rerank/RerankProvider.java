package ai.pipestream.proto.rerank;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * ServiceLoader SPI: one implementation per rerank model or runtime.
 *
 * <p>Providers score how relevant each candidate text is to a query. Absolute score scales
 * are provider-specific (sigmoid probabilities on some, raw logits on others), so only the
 * ORDER a provider produces is comparable across providers: two providers serving the same
 * model must agree on which text comes first, not on any score value. Discovery goes through
 * {@link RerankProviders}.
 */
public interface RerankProvider {
    /** Stable id, e.g. {@code tei}. */
    String providerId();

    /**
     * Scores each text in {@code texts} against {@code query}. The returned list aligns with
     * the input: element {@code i} is the relevance of {@code texts.get(i)}, higher meaning
     * more relevant.
     */
    List<Double> score(String query, List<String> texts);

    /**
     * Scores with {@link #score(String, List)}, sorts by descending score, and truncates to
     * {@code topK} entries. Each {@link ScoredText} carries the index its text held in the
     * input, so callers can map the ranking back to their own candidate list. Ties keep input
     * order.
     *
     * @throws IllegalArgumentException when {@code topK} is negative
     */
    default List<ScoredText> rank(String query, List<String> texts, int topK) {
        if (topK < 0) {
            throw new IllegalArgumentException("topK must not be negative: " + topK);
        }
        List<Double> scores = score(query, texts);
        List<ScoredText> ranked = new ArrayList<>(texts.size());
        for (int i = 0; i < texts.size(); i++) {
            ranked.add(new ScoredText(i, texts.get(i), scores.get(i)));
        }
        ranked.sort(Comparator.comparingDouble(ScoredText::score).reversed());
        if (topK >= ranked.size()) {
            return ranked;
        }
        return new ArrayList<>(ranked.subList(0, topK));
    }
}
