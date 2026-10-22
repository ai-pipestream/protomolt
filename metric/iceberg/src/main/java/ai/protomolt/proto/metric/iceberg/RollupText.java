package ai.protomolt.proto.metric.iceberg;

import java.util.ArrayList;
import java.util.List;

/** Package-private text helpers shared by the rollup sink, subjects, and SQL rendering. */
final class RollupText {

    private RollupText() {
    }

    /**
     * {@link String#split(String)} (limit 0) on one literal separator, without regex: an
     * empty input is one empty token, trailing empty tokens are dropped, leading and
     * interior empty tokens are kept.
     */
    static String[] splitOn(String text, char separator) {
        if (text.isEmpty()) {
            return new String[]{""};
        }
        List<String> tokens = new ArrayList<>();
        int start = 0;
        int next;
        while ((next = text.indexOf(separator, start)) >= 0) {
            tokens.add(text.substring(start, next));
            start = next + 1;
        }
        tokens.add(text.substring(start));
        int end = tokens.size();
        while (end > 0 && tokens.get(end - 1).isEmpty()) {
            end--;
        }
        return tokens.subList(0, end).toArray(String[]::new);
    }
}
