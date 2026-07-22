package ai.pipestream.format;

/**
 * Email-address validation using the WHATWG "valid e-mail address" production
 * (<a href="https://html.spec.whatwg.org/multipage/input.html#valid-e-mail-address">HTML spec</a>).
 *
 * <p>This is deliberately the pragmatic web-form definition rather than the full RFC 5322 grammar:
 * a local part of the permitted ASCII characters, an {@code @}, and a dotted domain of
 * letter/digit/hyphen labels. It is the same syntactic definition protovalidate uses, so a value
 * that passes here passes there.
 *
 * <p>Implemented by direct character scanning rather than a regular expression, so validation is
 * linear-time and carries no catastrophic-backtracking (ReDoS) risk.
 */
public final class Emails {

    private static final String LOCAL_SPECIALS = ".!#$%&'*+/=?^_`{|}~-";

    private Emails() {
    }

    /** Returns whether {@code value} is a syntactically valid email address. */
    public static boolean isEmail(String value) {
        // The local-part and domain character sets both exclude '@', so the first '@' is the only
        // possible separator; any second '@' would fall in the domain and be rejected there.
        int at = value.indexOf('@');
        if (at < 0) {
            return false;
        }
        return validLocalPart(value, 0, at) && validDomain(value, at + 1, value.length());
    }

    /** Local part: one or more of the WHATWG-permitted ASCII characters. */
    private static boolean validLocalPart(String s, int start, int end) {
        if (end <= start) {
            return false;
        }
        for (int i = start; i < end; i++) {
            char c = s.charAt(i);
            if (!isAlphanumeric(c) && LOCAL_SPECIALS.indexOf(c) < 0) {
                return false;
            }
        }
        return true;
    }

    /** Domain: dot-separated labels, each a 1-63 char letter/digit/hyphen label (no leading/trailing hyphen). */
    private static boolean validDomain(String s, int start, int end) {
        if (end <= start) {
            return false;
        }
        int labelStart = start;
        while (true) {
            int dot = s.indexOf('.', labelStart);
            int labelEnd = (dot < 0 || dot >= end) ? end : dot;
            if (!validLabel(s, labelStart, labelEnd)) {
                return false;
            }
            if (dot < 0 || dot >= end) {
                return true;
            }
            labelStart = dot + 1;
        }
    }

    private static boolean validLabel(String s, int start, int end) {
        int len = end - start;
        if (len < 1 || len > 63) {
            return false;
        }
        if (!isAlphanumeric(s.charAt(start)) || !isAlphanumeric(s.charAt(end - 1))) {
            return false;
        }
        for (int i = start + 1; i < end - 1; i++) {
            char c = s.charAt(i);
            if (!isAlphanumeric(c) && c != '-') {
                return false;
            }
        }
        return true;
    }

    private static boolean isAlphanumeric(char c) {
        return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9');
    }
}
