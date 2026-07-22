package ai.pipestream.proto.rerank;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A provider registered in this module's test {@code META-INF/services} so the ServiceLoader
 * seam has something to discover; the core module ships no provider of its own. Scores from
 * a fixed table so tests can assert exact orderings.
 */
public final class FixedScoreRerankProvider implements RerankProvider {

    static final String PROVIDER_ID = "fixed-score";

    private static final Map<String, Double> TABLE = Map.of(
            "bamboo shoots", 0.9,
            "a memoir", 0.4,
            "quarterly earnings", 0.1);

    @Override
    public String providerId() {
        return PROVIDER_ID;
    }

    @Override
    public List<Double> score(String query, List<String> texts) {
        List<Double> scores = new ArrayList<>(texts.size());
        for (String text : texts) {
            Double score = TABLE.get(text);
            if (score == null) {
                throw new IllegalArgumentException("No fixture score for '" + text + "'");
            }
            scores.add(score);
        }
        return scores;
    }
}
