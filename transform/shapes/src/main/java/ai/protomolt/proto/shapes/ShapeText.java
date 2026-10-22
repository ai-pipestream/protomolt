package ai.protomolt.proto.shapes;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/** Lexical operations shared by shape checks and execution. */
final class ShapeText {
    private ShapeText() {}

    /** Splits dotted paths, discarding trailing empty segments unless the input is empty. */
    static String[] splitPath(String path) {
        if (path.isEmpty()) {
            return new String[]{""};
        }
        List<String> parts = new ArrayList<>();
        int start = 0;
        while (true) {
            int dot = path.indexOf('.', start);
            if (dot < 0) {
                parts.add(path.substring(start));
                break;
            }
            parts.add(path.substring(start, dot));
            start = dot + 1;
        }
        int end = parts.size();
        while (end > 0 && parts.get(end - 1).isEmpty()) {
            end--;
        }
        return parts.subList(0, end).toArray(String[]::new);
    }
    /** Splits at the first operator, keeping an empty right side. */
    static String[] sides(String text, boolean append) {
        String operator = append ? "+=" : "=";
        int at = text.indexOf(operator);
        return at < 0 ? new String[] {text}
                : new String[] {text.substring(0, at), text.substring(at + operator.length())};
    }

    static String snakeCase(String name) {
        var out = new StringBuilder(name.length());
        for (int i = 0; i < name.length(); i++) {
            char c = name.charAt(i);
            if (i > 0 && c >= 'A' && c <= 'Z') {
                char previous = name.charAt(i - 1);
                if ((previous >= 'a' && previous <= 'z') || (previous >= '0' && previous <= '9'))
                    out.append('_');
            }
            out.append(c);
        }
        return out.toString().toLowerCase(Locale.ROOT);
    }
}
