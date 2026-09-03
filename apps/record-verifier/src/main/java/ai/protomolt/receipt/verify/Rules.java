package ai.protomolt.receipt.verify;

/**
 * The declared-rule formats the receipt contract uses, restated by hand with the same
 * semantics as the platform's format parsers: lowercase slug, lowercase hex SHA-256, and
 * the RFC 6838 media-type grammar. Lengths count Unicode code points.
 */
final class Rules {

    private Rules() {
    }

    /** A required slug of at most {@code maxLen} code points, or the violation. */
    static String requiredSlug(String value, int maxLen, String field) {
        if (value.isEmpty()) {
            return field + ": is required";
        }
        if (codePoints(value) > maxLen) {
            return field + ": at most " + maxLen + " characters";
        }
        if (!isSlug(value)) {
            return field + ": must be a slug";
        }
        return null;
    }

    /** Lowercase {@code a-z0-9} with interior single {@code .}, {@code _} or {@code -}. */
    static boolean isSlug(String value) {
        if (value.isEmpty()) {
            return false;
        }
        boolean previousSeparator = false;
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            boolean alphanumeric = (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9');
            boolean separator = c == '.' || c == '_' || c == '-';
            if (!alphanumeric && !separator) {
                return false;
            }
            if (separator && (i == 0 || i == value.length() - 1 || previousSeparator)) {
                return false;
            }
            previousSeparator = separator;
        }
        return true;
    }

    /** Lowercase hex SHA-256 digest: exactly 64 chars of {@code 0-9a-f}. */
    /** Dashed UUID text, either hex case: the platform's own uuid rule. */
    static boolean isUuid(String value) {
        if (value.length() != 36) {
            return false;
        }
        for (int i = 0; i < 36; i++) {
            char character = value.charAt(i);
            if (i == 8 || i == 13 || i == 18 || i == 23) {
                if (character != '-') {
                    return false;
                }
            } else if ((character < '0' || character > '9')
                    && (character < 'a' || character > 'f')
                    && (character < 'A' || character > 'F')) {
                return false;
            }
        }
        return true;
    }

    static boolean isSha256Hex(String value) {
        if (value.length() != 64) {
            return false;
        }
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'))) {
                return false;
            }
        }
        return true;
    }

    /** RFC 6838 {@code type/subtype}, each a 1..127-char restricted name. */
    static boolean isMediaType(String value) {
        int slash = value.indexOf('/');
        if (slash <= 0 || slash != value.lastIndexOf('/')) {
            return false;
        }
        return restrictedName(value, 0, slash)
                && restrictedName(value, slash + 1, value.length());
    }

    private static boolean restrictedName(String value, int from, int to) {
        int length = to - from;
        if (length < 1 || length > 127) {
            return false;
        }
        for (int i = from; i < to; i++) {
            char c = value.charAt(i);
            boolean alphanumeric = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
                    || (c >= '0' && c <= '9');
            if (i == from) {
                if (!alphanumeric) {
                    return false;
                }
                continue;
            }
            if (!alphanumeric && c != '!' && c != '#' && c != '$' && c != '&'
                    && c != '-' && c != '^' && c != '_' && c != '.' && c != '+') {
                return false;
            }
        }
        return true;
    }

    static int codePoints(String value) {
        return value.codePointCount(0, value.length());
    }
}
