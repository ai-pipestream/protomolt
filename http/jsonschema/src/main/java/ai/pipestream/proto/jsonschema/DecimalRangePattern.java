package ai.pipestream.proto.jsonschema;

import java.math.BigInteger;

/**
 * Builds anchored regular expressions matching the canonical decimal spellings of the
 * integers inside an inclusive range. Used to apply int64/uint64 range constraints to
 * the string spelling of proto3 canonical JSON (JsonFormat prints 64-bit integers as
 * JSON strings, where {@code minimum}/{@code maximum} keywords do not apply).
 *
 * <p>The generated patterns are digit-class alternations without nested quantifiers, so
 * matching is linear in the input length (no catastrophic backtracking).
 */
final class DecimalRangePattern {

    private DecimalRangePattern() {
    }

    /**
     * Anchored pattern matching the canonical decimal spellings (optional leading minus,
     * no leading zeros) of all integers {@code n} with {@code lo <= n <= hi}.
     *
     * @param lo inclusive lower bound, or {@code null} for unbounded below
     * @param hi inclusive upper bound, or {@code null} for unbounded above
     */
    static String range(BigInteger lo, BigInteger hi) {
        if (lo != null && hi != null && lo.compareTo(hi) > 0) {
            return "^(?!)$"; // empty range: matches nothing
        }
        StringBuilder alternation = new StringBuilder();
        // Negative side: spellings are "-" + |n| for lo <= n <= min(hi, -1).
        if (lo == null || lo.signum() < 0) {
            BigInteger negHi = lo == null ? null : lo.negate();
            BigInteger negLo = (hi == null || hi.signum() >= 0) ? BigInteger.ONE : hi.negate();
            append(alternation, "-(?:" + nonNegative(negLo, negHi) + ")");
        }
        // Non-negative side: max(lo, 0) <= n <= hi.
        if (hi == null || hi.signum() >= 0) {
            BigInteger nonNegLo = (lo == null || lo.signum() < 0) ? BigInteger.ZERO : lo;
            append(alternation, nonNegative(nonNegLo, hi));
        }
        return "^(?:" + alternation + ")$";
    }

    private static void append(StringBuilder alternation, String branch) {
        if (!alternation.isEmpty()) {
            alternation.append('|');
        }
        alternation.append(branch);
    }

    /** Alternation for canonical spellings of {@code lo <= n <= hi}, {@code lo >= 0}. */
    private static String nonNegative(BigInteger lo, BigInteger hi) {
        String loDigits = lo.toString();
        if (hi == null) {
            if (lo.signum() == 0) {
                return "0|[1-9][0-9]*";
            }
            // Same digit count and >= lo, or any canonical number with more digits.
            return sameLengthAtLeast(loDigits) + "|[1-9][0-9]{" + loDigits.length() + ",}";
        }
        String hiDigits = hi.toString();
        StringBuilder alternation = new StringBuilder();
        for (int length = loDigits.length(); length <= hiDigits.length(); length++) {
            String low = length == loDigits.length() ? loDigits : "1" + "0".repeat(length - 1);
            String high = length == hiDigits.length() ? hiDigits : "9".repeat(length);
            append(alternation, sameLength(low, high));
        }
        return alternation.toString();
    }

    /** Alternation for equal-length digit strings numerically in {@code [lo, hi]}. */
    private static String sameLength(String lo, String hi) {
        if (lo.equals(hi)) {
            return lo;
        }
        char d1 = lo.charAt(0);
        char d2 = hi.charAt(0);
        if (lo.length() == 1) {
            return charClass(d1, d2);
        }
        String restLo = lo.substring(1);
        String restHi = hi.substring(1);
        if (d1 == d2) {
            return d1 + group(sameLength(restLo, restHi));
        }
        StringBuilder alternation = new StringBuilder();
        append(alternation, d1 + group(sameLengthAtLeast(restLo)));
        if (d2 - d1 >= 2) {
            append(alternation, charClass((char) (d1 + 1), (char) (d2 - 1)) + anyDigits(restLo.length()));
        }
        append(alternation, d2 + group(sameLengthAtMost(restHi)));
        return alternation.toString();
    }

    /** Alternation for equal-length digit strings numerically {@code >= s}. */
    private static String sameLengthAtLeast(String s) {
        char d = s.charAt(0);
        if (s.length() == 1) {
            return charClass(d, '9');
        }
        String rest = s.substring(1);
        StringBuilder alternation = new StringBuilder();
        append(alternation, d + group(sameLengthAtLeast(rest)));
        if (d < '9') {
            append(alternation, charClass((char) (d + 1), '9') + anyDigits(rest.length()));
        }
        return alternation.toString();
    }

    /** Alternation for equal-length digit strings numerically {@code <= s}. */
    private static String sameLengthAtMost(String s) {
        char d = s.charAt(0);
        if (s.length() == 1) {
            return charClass('0', d);
        }
        String rest = s.substring(1);
        StringBuilder alternation = new StringBuilder();
        append(alternation, d + group(sameLengthAtMost(rest)));
        if (d > '0') {
            append(alternation, charClass('0', (char) (d - 1)) + anyDigits(rest.length()));
        }
        return alternation.toString();
    }

    private static String group(String pattern) {
        return pattern.indexOf('|') >= 0 ? "(?:" + pattern + ")" : pattern;
    }

    private static String charClass(char from, char to) {
        if (from == to) {
            return String.valueOf(from);
        }
        return "[" + from + "-" + to + "]";
    }

    private static String anyDigits(int count) {
        if (count == 0) {
            return "";
        }
        if (count == 1) {
            return "[0-9]";
        }
        return "[0-9]{" + count + "}";
    }
}
