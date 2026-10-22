package ai.protomolt.proto.sources;

import java.util.ArrayList;
import java.util.List;

/**
 * Extracts {@code import} statements from {@code .proto} source text without a full parse.
 *
 * <p>This is a syntactic scan used for dependency ordering and reachability; the authoritative
 * check remains compilation ({@link ProtoSourceCompiler}), which fails on imports the scan may
 * have mis-read. Line ({@code //}) and block ({@code /* *&#47;}) comments collapse to whitespace
 * during the scan so commented-out imports are ignored; {@code import public} and
 * {@code import weak} are treated as plain imports.</p>
 */
public final class ProtoImports {

    private ProtoImports() {
    }

    /**
     * Returns the import paths declared by the given {@code .proto} source, in order.
     *
     * <p>A single left-to-right pass: an {@code import} keyword is only considered when nothing
     * but whitespace precedes it on its line, comments are skipped wherever they appear, and a
     * comment between the keyword, an optional {@code public} or {@code weak} modifier, the
     * quoted path and the terminating {@code ;} reads as whitespace.</p>
     */
    public static List<String> of(String protoContent) {
        List<String> imports = new ArrayList<>();
        int i = 0;
        int length = protoContent.length();
        boolean lineStart = true;
        while (i < length) {
            char c = protoContent.charAt(i);
            if (c == '\n' || c == '\r') {
                lineStart = true;
                i++;
                continue;
            }
            if (isWhitespace(c)) {
                i++;
                continue;
            }
            if (c == '/' && i + 1 < length && protoContent.charAt(i + 1) == '/') {
                i = skipLineComment(protoContent, i + 2);
                continue;
            }
            if (c == '/' && i + 1 < length && protoContent.charAt(i + 1) == '*') {
                int close = protoContent.indexOf("*/", i + 2);
                if (close >= 0) {
                    i = close + 2;
                    continue;
                }
                // Without a terminator the comment never forms; the '/' is plain text.
            }
            if (lineStart && c == 'i') {
                int end = parseImport(protoContent, i, imports);
                if (end > i) {
                    i = end;
                    lineStart = false;
                    continue;
                }
            }
            lineStart = false;
            i++;
        }
        return List.copyOf(imports);
    }

    /**
     * Parses the import statement starting at {@code start} (the {@code i} of {@code import},
     * at a line start) and appends its path to {@code out}. Returns the index just past the
     * terminating {@code ;}, or -1 when the text at {@code start} is no well-formed import.
     */
    private static int parseImport(String text, int start, List<String> out) {
        if (!text.startsWith("import", start)) {
            return -1;
        }
        int i = skipRequiredGap(text, start + 6);
        if (i < 0) {
            return -1;
        }
        if (text.startsWith("public", i)) {
            int after = skipRequiredGap(text, i + 6);
            if (after >= 0) {
                i = after;
            }
        } else if (text.startsWith("weak", i)) {
            int after = skipRequiredGap(text, i + 4);
            if (after >= 0) {
                i = after;
            }
        }
        if (i >= text.length() || text.charAt(i) != '"') {
            return -1;
        }
        int close = text.indexOf('"', i + 1);
        if (close < 0 || close == i + 1) {
            return -1;
        }
        int end = skipGap(text, close + 1);
        if (end >= text.length() || text.charAt(end) != ';') {
            return -1;
        }
        out.add(text.substring(i + 1, close));
        return end + 1;
    }

    /** Skips whitespace and comments; fails when there is not at least one such unit. */
    private static int skipRequiredGap(String text, int i) {
        int end = skipGap(text, i);
        return end == i ? -1 : end;
    }

    /** Skips a run of whitespace and comments (a comment collapses to whitespace). */
    private static int skipGap(String text, int i) {
        int length = text.length();
        while (i < length) {
            char c = text.charAt(i);
            if (isWhitespace(c)) {
                i++;
            } else if (c == '/' && i + 1 < length && text.charAt(i + 1) == '/') {
                i = skipLineComment(text, i + 2);
            } else if (c == '/' && i + 1 < length && text.charAt(i + 1) == '*') {
                int close = text.indexOf("*/", i + 2);
                if (close < 0) {
                    break;
                }
                i = close + 2;
            } else {
                break;
            }
        }
        return i;
    }

    /** Skips a line comment, stopping at (and not consuming) the closing line break. */
    private static int skipLineComment(String text, int i) {
        int length = text.length();
        while (i < length && text.charAt(i) != '\n' && text.charAt(i) != '\r') {
            i++;
        }
        return i;
    }

    /** The regex whitespace set: space, tab, line feed, vertical tab, form feed, carriage return. */
    private static boolean isWhitespace(char c) {
        return c == ' ' || c == '\t' || c == '\n' || c == '\u000B' || c == '\f' || c == '\r';
    }
}
