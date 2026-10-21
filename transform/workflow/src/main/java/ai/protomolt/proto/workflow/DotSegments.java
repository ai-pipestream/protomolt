package ai.protomolt.proto.workflow;

import java.util.ArrayList;
import java.util.List;

/**
 * Zero-regex dot-path segment splitting shared by the typed-edge and
 * stream-join path walks, with the exact limit-0 semantics of splitting on
 * a literal {@code '.'}: an empty path yields one empty segment, trailing
 * empty segments drop (a separators-only path yields none), and leading
 * and inner empty segments stay so the walks reject them by name.
 */
final class DotSegments {

    private DotSegments() {
    }

    static String[] segments(String path) {
        if (path.isEmpty()) {
            return new String[] {""};
        }
        List<String> parts = new ArrayList<>();
        int from = 0;
        while (true) {
            int dot = path.indexOf('.', from);
            if (dot < 0) {
                parts.add(path.substring(from));
                break;
            }
            parts.add(path.substring(from, dot));
            from = dot + 1;
        }
        int end = parts.size();
        while (end > 0 && parts.get(end - 1).isEmpty()) {
            end--;
        }
        return parts.subList(0, end).toArray(String[]::new);
    }
}
