package ai.protomolt.proto.agenthost;

import java.util.ArrayList;
import java.util.List;

/**
 * Character classes and line splits for the host's input checks, written as plain scans so
 * no host check depends on a regular expression. Every class here is ASCII only: a letter
 * or digit outside ASCII is not a letter or digit to these checks.
 */
final class HostText {

    private HostText() {
    }

    /** True for {@code A-Z}. */
    static boolean isAsciiUpper(char c) {
        return c >= 'A' && c <= 'Z';
    }

    /** True for {@code 0-9}. */
    static boolean isAsciiDigit(char c) {
        return c >= '0' && c <= '9';
    }

    /** True for {@code A-Z}, {@code a-z} and {@code 0-9}. */
    static boolean isAsciiLetterOrDigit(char c) {
        return isAsciiUpper(c) || (c >= 'a' && c <= 'z') || isAsciiDigit(c);
    }

    /**
     * Splits on {@code '\n'} alone, the way {@code String.split("\n")} does: a {@code '\r'}
     * stays on its line, leading and interior empty lines are kept, trailing empty lines
     * are dropped, and an empty text is one empty line.
     */
    static List<String> splitOnNewline(String text) {
        List<String> lines = new ArrayList<>();
        int start = 0;
        int end;
        while ((end = text.indexOf('\n', start)) >= 0) {
            lines.add(text.substring(start, end));
            start = end + 1;
        }
        lines.add(text.substring(start));
        return withoutTrailingEmpty(text, lines);
    }

    /**
     * Splits on every line break the {@code \R} class names, the way
     * {@code String.split("\\R")} does: {@code "\r\n"} as one break, then {@code '\n'},
     * {@code '\r'}, vertical tab, form feed, NEL (U+0085), and the Unicode line and
     * paragraph separators (U+2028, U+2029) each as one break. Leading and interior empty
     * lines are kept, trailing empty lines are dropped, and an empty text is one empty line.
     */
    static List<String> splitOnLineBreaks(String text) {
        List<String> lines = new ArrayList<>();
        int start = 0;
        int i = 0;
        while (i < text.length()) {
            char c = text.charAt(i);
            if (c == '\r' && i + 1 < text.length() && text.charAt(i + 1) == '\n') {
                lines.add(text.substring(start, i));
                i += 2;
                start = i;
            } else if (isLineBreak(c)) {
                lines.add(text.substring(start, i));
                i++;
                start = i;
            } else {
                i++;
            }
        }
        lines.add(text.substring(start));
        return withoutTrailingEmpty(text, lines);
    }

    private static boolean isLineBreak(char c) {
        return c == '\n' || c == '\r' || c == 0x0B || c == '\f'
                || c == 0x85 || c == 0x2028 || c == 0x2029;
    }

    /** {@code String.split} keeps an empty text as one empty line and drops trailing ones. */
    private static List<String> withoutTrailingEmpty(String text, List<String> lines) {
        if (text.isEmpty()) {
            return lines;
        }
        int size = lines.size();
        while (size > 0 && lines.get(size - 1).isEmpty()) {
            size--;
        }
        return lines.subList(0, size);
    }
}
