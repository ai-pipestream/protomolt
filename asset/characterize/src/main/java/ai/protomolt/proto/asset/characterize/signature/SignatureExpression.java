package ai.protomolt.proto.asset.characterize.signature;

import ai.protomolt.proto.asset.characterize.ByteWindows;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * One compiled signature expression: a byte pattern written in the
 * signature syntax that format registries publish, matched against a
 * position in an asset's byte windows.
 *
 * <p>The syntax is small and fully documented, which is why this compiles
 * it rather than depending on an engine to interpret it. Values are hex
 * pairs ({@code 49492A00}) or Latin-1 strings ({@code 'PK'}); a value may
 * be constrained to a range ({@code [30:39]}, {@code ['0'-'9']}), to a set
 * ({@code [00 C2 DE]}), or to bits ({@code [&01]}), and any of those may be
 * inverted with a leading {@code !}. Alternatives go in round brackets
 * ({@code (00|F0|3C)}). Unknown stretches are written {@code ??} for one
 * byte, {@code {n}} for exactly n, {@code {n-m}} for a range, and
 * {@code *} or {@code {n-*}} for a stretch with no limit.
 *
 * <p>An expression with an unlimited stretch is compiled but reports
 * itself {@link #bounded() unbounded}, because matching it would mean
 * reading to the end of the content and identification does not do that.
 * The published sets in use write no such expression; one that appeared
 * would be skipped by name instead of quietly turning identification into
 * a full read.
 */
public final class SignatureExpression {

    /** The answer from {@link #matchAt} when the pattern does not match. */
    public static final int NO_MATCH = -1;

    /** The length of a stretch with no upper limit. */
    public static final int UNBOUNDED = Integer.MAX_VALUE;

    /** One piece of a pattern. */
    private sealed interface Step {
    }

    /** A run of single-byte tests, one per position. */
    private record Run(ByteMatcher[] matchers) implements Step {
    }

    /** A stretch of bytes of any value, between min and max long. */
    private record Gap(int min, int max) implements Step {
    }

    /** A choice between whole sub-patterns. */
    private record Choice(List<SignatureExpression> arms) implements Step {
    }

    private final String source;
    private final List<Step> steps;
    private final int minLength;
    private final int maxLength;

    private SignatureExpression(String source, List<Step> steps) {
        this.source = source;
        this.steps = steps;
        int min = 0;
        int max = 0;
        for (Step step : steps) {
            switch (step) {
                case Run run -> {
                    min += run.matchers().length;
                    max = add(max, run.matchers().length);
                }
                case Gap gap -> {
                    min += gap.min();
                    max = add(max, gap.max());
                }
                case Choice choice -> {
                    int armMin = Integer.MAX_VALUE;
                    int armMax = 0;
                    for (SignatureExpression arm : choice.arms()) {
                        armMin = Math.min(armMin, arm.minLength());
                        armMax = Math.max(armMax, arm.maxLength());
                    }
                    min += armMin;
                    max = add(max, armMax);
                }
            }
        }
        this.minLength = min;
        this.maxLength = max;
    }

    private static int add(int left, int right) {
        long sum = (long) left + right;
        return sum >= UNBOUNDED ? UNBOUNDED : (int) sum;
    }

    /**
     * Compiles an expression.
     *
     * @param expression the expression text; whitespace between elements is
     *        insignificant
     * @return the compiled expression
     * @throws SignatureSyntaxException if the text is not a valid expression
     */
    public static SignatureExpression compile(String expression)
            throws SignatureSyntaxException {
        if (expression == null) {
            throw new SignatureSyntaxException("", 0, "no expression");
        }
        Parser parser = new Parser(expression);
        List<Step> steps = parser.parseSteps(false);
        parser.expectEnd();
        if (steps.isEmpty()) {
            throw new SignatureSyntaxException(expression, 0, "the expression is empty");
        }
        return new SignatureExpression(expression, steps);
    }

    /** The expression text this was compiled from. */
    public String source() {
        return source;
    }

    /** The shortest run of bytes this can match. */
    public int minLength() {
        return minLength;
    }

    /** The longest run of bytes this can match, or {@link #UNBOUNDED}. */
    public int maxLength() {
        return maxLength;
    }

    /** Whether the longest possible match has a limit. */
    public boolean bounded() {
        return maxLength != UNBOUNDED;
    }

    /**
     * Whether the bytes this pattern could need are within the windows, so
     * that a failure to match is a real finding and not a blind spot.
     *
     * @param source the byte windows
     * @param offset the absolute offset to match at
     * @return true when the pattern can be evaluated here
     */
    public boolean evaluableAt(ByteWindows source, long offset) {
        return bounded() && source.observable(offset, maxLength);
    }

    /**
     * Matches the pattern anchored at an offset.
     *
     * @param source the byte windows
     * @param offset the absolute offset the pattern must start at
     * @return how many bytes matched, or {@link #NO_MATCH}
     */
    public int matchAt(ByteWindows source, long offset) {
        long end = matchFrom(source, offset, 0);
        return end == NO_MATCH ? NO_MATCH : (int) (end - offset);
    }

    /** Matches from a step index; the absolute end offset, or NO_MATCH. */
    private long matchFrom(ByteWindows bytes, long offset, int index) {
        if (index == steps.size()) {
            return offset;
        }
        return switch (steps.get(index)) {
            case Run run -> {
                long at = offset;
                for (ByteMatcher matcher : run.matchers()) {
                    int value = bytes.byteAt(at);
                    if (value == ByteWindows.NOT_OBSERVED || !matcher.matches(value)) {
                        yield NO_MATCH;
                    }
                    at++;
                }
                yield matchFrom(bytes, at, index + 1);
            }
            case Gap gap -> {
                // Shortest first, so a match is the leftmost one and the
                // result does not depend on how much content follows.
                int limit = gap.max() == UNBOUNDED ? gap.min() : gap.max();
                for (int length = gap.min(); length <= limit; length++) {
                    if (!bytes.observable(offset, length)) {
                        break;
                    }
                    long end = matchFrom(bytes, offset + length, index + 1);
                    if (end != NO_MATCH) {
                        yield end;
                    }
                }
                yield NO_MATCH;
            }
            case Choice choice -> {
                for (SignatureExpression arm : choice.arms()) {
                    long armEnd = arm.matchFrom(bytes, offset, 0);
                    if (armEnd == NO_MATCH) {
                        continue;
                    }
                    long end = matchFrom(bytes, armEnd, index + 1);
                    if (end != NO_MATCH) {
                        yield end;
                    }
                }
                yield NO_MATCH;
            }
        };
    }

    /**
     * Searches for the pattern in a span, at every offset in turn.
     *
     * @param bytes the byte windows
     * @param from the first absolute offset to try
     * @param to the last absolute offset to try, inclusive
     * @return the absolute offset of the leftmost match, or
     *         {@link #NO_MATCH}
     */
    public long findBetween(ByteWindows bytes, long from, long to) {
        for (long at = Math.max(0, from); at <= to; at++) {
            if (matchAt(bytes, at) != NO_MATCH) {
                return at;
            }
        }
        return NO_MATCH;
    }

    @Override
    public String toString() {
        return source;
    }

    // ------------------------------------------------------------------
    // Compiling
    // ------------------------------------------------------------------

    /** A cursor over the expression text. */
    private static final class Parser {

        private final String text;
        private int at;

        Parser(String text) {
            this.text = text;
        }

        /**
         * Reads steps until the end, or until a closing bracket or bar when
         * reading the arm of a choice.
         */
        List<Step> parseSteps(boolean insideChoice) throws SignatureSyntaxException {
            List<Step> steps = new ArrayList<>();
            List<ByteMatcher> run = new ArrayList<>();
            while (true) {
                skipSpace();
                if (at >= text.length()) {
                    break;
                }
                char c = text.charAt(at);
                if (c == '|' || c == ')') {
                    if (insideChoice) {
                        break;
                    }
                    throw fail("unexpected '" + c + "'");
                }
                switch (c) {
                    case '\'' -> run.addAll(readString());
                    case '[' -> run.add(readSet());
                    case '?' -> {
                        expectPair("??");
                        run.add(ByteMatcher.ANY);
                    }
                    case '(' -> {
                        flush(steps, run);
                        steps.add(readChoice());
                    }
                    case '{' -> {
                        flush(steps, run);
                        steps.add(readGap());
                    }
                    case '*' -> {
                        at++;
                        flush(steps, run);
                        steps.add(new Gap(0, UNBOUNDED));
                    }
                    default -> run.add(ByteMatcher.exact(readHexByte()));
                }
            }
            flush(steps, run);
            return steps;
        }

        void expectEnd() throws SignatureSyntaxException {
            skipSpace();
            if (at < text.length()) {
                throw fail("unexpected '" + text.charAt(at) + "'");
            }
        }

        private void flush(List<Step> steps, List<ByteMatcher> run) {
            if (!run.isEmpty()) {
                steps.add(new Run(run.toArray(new ByteMatcher[0])));
                run.clear();
            }
        }

        private void skipSpace() {
            while (at < text.length() && Character.isWhitespace(text.charAt(at))) {
                at++;
            }
        }

        private void expectPair(String pair) throws SignatureSyntaxException {
            if (!text.startsWith(pair, at)) {
                throw fail("expected '" + pair + "'");
            }
            at += pair.length();
        }

        /** A Latin-1 string in single quotes, as one exact matcher per char. */
        private List<ByteMatcher> readString() throws SignatureSyntaxException {
            List<ByteMatcher> matchers = new ArrayList<>();
            for (int value : readStringBytes()) {
                matchers.add(ByteMatcher.exact(value));
            }
            return matchers;
        }

        /** The bytes of a Latin-1 string in single quotes. */
        private int[] readStringBytes() throws SignatureSyntaxException {
            int start = at;
            at++;
            int close = text.indexOf('\'', at);
            if (close < 0) {
                at = start;
                throw fail("unterminated string");
            }
            String value = text.substring(at, close);
            at = close + 1;
            if (value.isEmpty()) {
                throw fail("an empty string matches nothing");
            }
            for (int i = 0; i < value.length(); i++) {
                if (value.charAt(i) > 0xFF) {
                    throw fail("'" + value.charAt(i) + "' is outside Latin-1");
                }
            }
            byte[] bytes = value.getBytes(StandardCharsets.ISO_8859_1);
            int[] out = new int[bytes.length];
            for (int i = 0; i < bytes.length; i++) {
                out[i] = bytes[i] & 0xFF;
            }
            return out;
        }

        /** A bracketed set: values, ranges and bitmasks, optionally inverted. */
        private ByteMatcher readSet() throws SignatureSyntaxException {
            at++;
            boolean inverted = false;
            skipSpace();
            if (at < text.length() && text.charAt(at) == '!') {
                inverted = true;
                at++;
            }
            List<ByteMatcher> members = new ArrayList<>();
            while (true) {
                skipSpace();
                if (at >= text.length()) {
                    throw fail("unterminated set");
                }
                if (text.charAt(at) == ']') {
                    at++;
                    break;
                }
                members.add(readSetMember());
            }
            if (members.isEmpty()) {
                throw fail("an empty set matches nothing");
            }
            ByteMatcher union = ByteMatcher.anyOf(members);
            return inverted ? ByteMatcher.not(union) : union;
        }

        /** One member of a set: a bitmask, a range, a string's characters, or a value. */
        private ByteMatcher readSetMember() throws SignatureSyntaxException {
            if (text.charAt(at) == '&') {
                at++;
                return ByteMatcher.bitmask(readHexByte());
            }
            if (text.charAt(at) == '\'') {
                int[] characters = readStringBytes();
                if (characters.length > 1) {
                    // A multi-character string in a set offers its characters
                    // as alternatives; the string itself is not the match.
                    List<ByteMatcher> each = new ArrayList<>();
                    for (int value : characters) {
                        each.add(ByteMatcher.exact(value));
                    }
                    return ByteMatcher.anyOf(each);
                }
                return rangeOrValue(characters[0]);
            }
            return rangeOrValue(readHexByte());
        }

        /** A value, or the low end of a range when a separator follows. */
        private ByteMatcher rangeOrValue(int low) throws SignatureSyntaxException {
            if (at >= text.length()) {
                throw fail("unterminated set");
            }
            char separator = text.charAt(at);
            if (separator != ':' && separator != '-') {
                return ByteMatcher.exact(low);
            }
            at++;
            if (at < text.length() && text.charAt(at) == '\'') {
                int[] characters = readStringBytes();
                if (characters.length != 1) {
                    throw fail("a range bound is one character");
                }
                return ByteMatcher.range(low, characters[0]);
            }
            return ByteMatcher.range(low, readHexByte());
        }

        /** A parenthesised choice of whole sub-patterns. */
        private Step readChoice() throws SignatureSyntaxException {
            at++;
            List<SignatureExpression> arms = new ArrayList<>();
            while (true) {
                int start = at;
                List<Step> steps = parseSteps(true);
                if (steps.isEmpty()) {
                    at = start;
                    throw fail("an empty alternative matches nothing");
                }
                arms.add(new SignatureExpression(text.substring(start, at).trim(), steps));
                if (at >= text.length()) {
                    throw fail("unterminated alternatives");
                }
                if (text.charAt(at) == ')') {
                    at++;
                    break;
                }
                at++;
            }
            return new Choice(List.copyOf(arms));
        }

        /** A braced stretch: {n}, {n-m} or {n-*}. */
        private Step readGap() throws SignatureSyntaxException {
            at++;
            int min = readNumber();
            int max = min;
            if (at < text.length() && text.charAt(at) == '-') {
                at++;
                if (at < text.length() && text.charAt(at) == '*') {
                    at++;
                    max = UNBOUNDED;
                } else {
                    max = readNumber();
                }
            }
            if (at >= text.length() || text.charAt(at) != '}') {
                throw fail("expected '}'");
            }
            at++;
            if (max != UNBOUNDED && max < min) {
                throw fail("a stretch cannot end before it begins");
            }
            return new Gap(min, max);
        }

        private int readNumber() throws SignatureSyntaxException {
            int start = at;
            while (at < text.length() && Character.isDigit(text.charAt(at))) {
                at++;
            }
            if (start == at) {
                throw fail("expected a number");
            }
            try {
                return Integer.parseInt(text, start, at, 10);
            } catch (NumberFormatException tooBig) {
                at = start;
                throw fail("the number does not fit");
            }
        }

        private int readHexByte() throws SignatureSyntaxException {
            skipSpace();
            if (at + 1 >= text.length()) {
                throw fail("expected two hex digits");
            }
            int high = Character.digit(text.charAt(at), 16);
            int low = Character.digit(text.charAt(at + 1), 16);
            if (high < 0 || low < 0) {
                throw fail("expected two hex digits");
            }
            at += 2;
            return (high << 4) | low;
        }

        private SignatureSyntaxException fail(String detail) {
            return new SignatureSyntaxException(text, at, detail);
        }
    }
}
