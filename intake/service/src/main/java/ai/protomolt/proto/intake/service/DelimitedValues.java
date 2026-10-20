package ai.protomolt.proto.intake.service;

import java.util.ArrayList;
import java.util.List;

/**
 * Zero-regex single-character delimiter splitting for environment values,
 * with the exact limit-0 semantics of splitting on a literal delimiter:
 * an empty input yields one empty segment, trailing empty segments drop,
 * and leading and inner empty segments stay for the caller to filter.
 */
final class DelimitedValues {

    private DelimitedValues() {
    }

    static List<String> segments(String value, char delimiter) {
        if (value.isEmpty()) {
            return List.of("");
        }
        List<String> parts = new ArrayList<>();
        int from = 0;
        while (true) {
            int at = value.indexOf(delimiter, from);
            if (at < 0) {
                parts.add(value.substring(from));
                break;
            }
            parts.add(value.substring(from, at));
            from = at + 1;
        }
        int end = parts.size();
        while (end > 0 && parts.get(end - 1).isEmpty()) {
            end--;
        }
        return parts.subList(0, end);
    }
}
