package ai.protomolt.proto.asset.characterize;

import java.util.List;
import java.util.Objects;

/**
 * One format name grammar: the contract expression it stands for, matched
 * without regex. Every declared expression has the same shape,
 * {@code (?i)^.+\\.(ext|ext)$}: at least one character, a dot, then one of
 * the extensions to the end of the name.
 *
 * @param expression the pattern the contract declares on the format
 *        message's {@code filename} field, kept verbatim so the parity
 *        test can hold the two declarations together
 * @param extensions the extension alternatives of the expression,
 *        lowercase, without the dot ({@code tar.gz} keeps its inner dot)
 */
public record FormatGrammar(String expression, List<String> extensions) {

    /** Defensively copies the alternatives. */
    public FormatGrammar {
        Objects.requireNonNull(expression, "expression");
        extensions = List.copyOf(extensions);
    }

    static FormatGrammar of(String expression, String... extensions) {
        return new FormatGrammar(expression, List.of(extensions));
    }

    /**
     * Whether {@code filename} fully matches the expression: no line
     * terminator anywhere (the expression's {@code .} matches none), at
     * least one character before the final extension dot, and an
     * ASCII-case-insensitive extension suffix.
     *
     * @param filename the name to test, or null
     * @return true when the grammar admits the name
     */
    public boolean matches(String filename) {
        if (filename == null) {
            return false;
        }
        for (int i = 0; i < filename.length(); i++) {
            if (isLineTerminator(filename.charAt(i))) {
                return false;
            }
        }
        for (String extension : extensions) {
            int suffix = extension.length() + 1;
            if (filename.length() > suffix
                    && filename.charAt(filename.length() - suffix) == '.'
                    && extensionMatches(filename, filename.length() - extension.length(), extension)) {
                return true;
            }
        }
        return false;
    }

    private static boolean extensionMatches(String filename, int from, String extension) {
        for (int i = 0; i < extension.length(); i++) {
            if (asciiLower(filename.charAt(from + i)) != extension.charAt(i)) {
                return false;
            }
        }
        return true;
    }

    private static char asciiLower(char c) {
        return c >= 'A' && c <= 'Z' ? (char) (c + ('a' - 'A')) : c;
    }

    private static boolean isLineTerminator(char c) {
        return c == '\n' || c == '\r' || c == '\u0085' || c == '\u2028' || c == '\u2029';
    }
}
