package ai.pipestream.proto.search.chunk;

import java.util.ArrayList;
import java.util.List;

/**
 * Boundary rule set {@code rules-v1}: deterministic sentence segmentation,
 * hand-rolled against the JDK's stable character classes so a JDK upgrade
 * can never silently move a boundary (the policy contract's requirement;
 * this is why {@code java.text.BreakIterator} is deliberately not used).
 *
 * <p>The rules, frozen forever under this id (a changed rule is a NEW rule
 * set id):
 *
 * <ul>
 *   <li>A sentence ends after a run of {@code . ? !}, optionally followed
 *   by closing quotes or brackets ({@code " ' ) ] } » ’ ”}), when the next
 *   character is whitespace or the end of text.</li>
 *   <li>A blank line (two consecutive line feeds, ignoring intervening
 *   spaces, tabs, and carriage returns) always ends the sentence: paragraph
 *   structure outranks punctuation.</li>
 *   <li>A token is a maximal run of non-whitespace characters
 *   ({@link Character#isWhitespace(char)}).</li>
 *   <li>A sentence's span is trimmed of surrounding whitespace; whitespace
 *   between sentences belongs to neither.</li>
 * </ul>
 */
final class SentenceRules {

    /** The pinned rule-set id. */
    static final String ID = "rules-v1";

    private static final String CLOSERS = "\"')]}»’”";

    private SentenceRules() {
    }

    /** A sentence span in the source text, with its token count. */
    record Sentence(int start, int end, int tokens) {
    }

    /** Segments the text; blank input yields no sentences. */
    static List<Sentence> segment(String text) {
        List<Sentence> sentences = new ArrayList<>();
        int length = text.length();
        int cursor = 0;
        while (cursor < length) {
            while (cursor < length && Character.isWhitespace(text.charAt(cursor))) {
                cursor++;
            }
            if (cursor >= length) {
                break;
            }
            int start = cursor;
            int end = -1;
            int scan = cursor;
            while (scan < length) {
                char c = text.charAt(scan);
                if (c == '.' || c == '?' || c == '!') {
                    int after = scan + 1;
                    while (after < length && (isTerminator(text.charAt(after))
                            || CLOSERS.indexOf(text.charAt(after)) >= 0)) {
                        after++;
                    }
                    if (after >= length || Character.isWhitespace(text.charAt(after))) {
                        end = after;
                        break;
                    }
                    scan = after;
                    continue;
                }
                if (c == '\n' && blankLineFollows(text, scan)) {
                    end = scan;
                    break;
                }
                scan++;
            }
            if (end < 0) {
                end = length;
            }
            while (end > start && Character.isWhitespace(text.charAt(end - 1))) {
                end--;
            }
            int tokens = countTokens(text, start, end);
            if (tokens > 0) {
                sentences.add(new Sentence(start, end, tokens));
            }
            cursor = Math.max(end, cursor + 1);
        }
        return sentences;
    }

    /** Tokens in {@code [start, end)}: maximal non-whitespace runs. */
    static int countTokens(String text, int start, int end) {
        // Counted in place rather than through tokenSpans: segment() calls this once per
        // sentence over the whole document, and the spans it would build are discarded.
        int tokens = 0;
        boolean inToken = false;
        for (int i = start; i < end; i++) {
            if (Character.isWhitespace(text.charAt(i))) {
                inToken = false;
            } else if (!inToken) {
                inToken = true;
                tokens++;
            }
        }
        return tokens;
    }

    /** Token spans in {@code [start, end)}: maximal non-whitespace runs. */
    static List<BoundaryRules.TokenSpan> tokenSpans(String text, int start, int end) {
        List<BoundaryRules.TokenSpan> spans = new ArrayList<>();
        int tokenStart = -1;
        for (int i = start; i < end; i++) {
            boolean whitespace = Character.isWhitespace(text.charAt(i));
            if (!whitespace && tokenStart < 0) {
                tokenStart = i;
            }
            if (whitespace && tokenStart >= 0) {
                spans.add(new BoundaryRules.TokenSpan(tokenStart, i));
                tokenStart = -1;
            }
        }
        if (tokenStart >= 0) {
            spans.add(new BoundaryRules.TokenSpan(tokenStart, end));
        }
        return spans;
    }

    private static boolean isTerminator(char c) {
        return c == '.' || c == '?' || c == '!';
    }

    /**
     * Whether the line feed at {@code index} starts a blank line: a second
     * line feed follows with only spaces, tabs, or carriage returns between.
     */
    private static boolean blankLineFollows(String text, int index) {
        for (int i = index + 1; i < text.length(); i++) {
            char c = text.charAt(i);
            if (c == '\n') {
                return true;
            }
            if (c != ' ' && c != '\t' && c != '\r') {
                return false;
            }
        }
        return false;
    }
}
