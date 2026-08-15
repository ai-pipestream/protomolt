package ai.pipestream.proto.parse.text;

import java.util.ArrayList;
import java.util.List;
import opennlp.tools.util.StringUtil;
import opennlp.tools.util.normalizer.UnicodeWhitespace;

/**
 * Text cleaning on OpenNLP's Unicode whitespace model
 * ({@link UnicodeWhitespace}, {@link StringUtil}): line breaks and blank
 * lines are recognized across Unicode — LF, CR, CRLF, NEL, and the U+2028 /
 * U+2029 separators — rather than through regex, whose line-break classes
 * are not the Unicode ones.
 */
final class TextCleaning {

    private TextCleaning() {
    }

    /**
     * Splits the text into blocks on blank lines: two or more consecutive
     * line breaks, where CRLF counts as one break. A run longer than two
     * leaves its extra breaks on the following block; callers trim.
     */
    static List<String> splitOnBlankLines(String text) {
        List<String> blocks = new ArrayList<>();
        int blockStart = 0;
        int breakRunStart = -1;
        int consecutiveBreaks = 0;
        int i = 0;
        int length = text.length();
        while (i < length) {
            int codePoint = text.codePointAt(i);
            if (isLineBreak(codePoint)) {
                if (consecutiveBreaks == 0) {
                    breakRunStart = i;
                }
                consecutiveBreaks++;
                if (codePoint == '\r' && i + 1 < length && text.charAt(i + 1) == '\n') {
                    i++;
                }
                i++;
                if (consecutiveBreaks == 2) {
                    if (blockStart < breakRunStart) {
                        blocks.add(text.substring(blockStart, breakRunStart));
                    }
                    blockStart = i;
                }
            } else {
                consecutiveBreaks = 0;
                i += Character.charCount(codePoint);
            }
        }
        if (blockStart < length) {
            blocks.add(text.substring(blockStart));
        }
        return blocks;
    }

    /** Flows every line break into a single space (CRLF is one break). */
    static String flowLines(String text) {
        StringBuilder flowed = new StringBuilder(text.length());
        int i = 0;
        int length = text.length();
        while (i < length) {
            int codePoint = text.codePointAt(i);
            if (isLineBreak(codePoint)) {
                flowed.append(' ');
                if (codePoint == '\r' && i + 1 < length && text.charAt(i + 1) == '\n') {
                    i++;
                }
                i++;
            } else {
                flowed.appendCodePoint(codePoint);
                i += Character.charCount(codePoint);
            }
        }
        return flowed.toString();
    }

    /** Removes markdown heading markup: leading '#' runs plus the whitespace after them. */
    static String stripHeadingMarkup(String heading) {
        int i = 0;
        int length = heading.length();
        while (i < length && heading.charAt(i) == '#') {
            i++;
        }
        while (i < length && StringUtil.isUnicodeWhitespace(heading.codePointAt(i))) {
            i += Character.charCount(heading.codePointAt(i));
        }
        return StringUtil.trimUnicodeWhitespace(heading.substring(i));
    }

    /** Trims Unicode whitespace from both ends. */
    static String trim(String text) {
        return StringUtil.trimUnicodeWhitespace(text);
    }

    private static boolean isLineBreak(int codePoint) {
        return UnicodeWhitespace.byCodePoint(codePoint)
                .map(UnicodeWhitespace.WhitespaceCharacter::isLineBreak)
                .orElse(false);
    }
}
