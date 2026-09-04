package ai.protomolt.proto.validate.model;

/**
 * The {@code well_known_regex} HTTP-header string rules (RFC 7230), in their strict and loose
 * forms. A header name additionally rejects the empty string (reported under the
 * {@code …header_name_empty} rule); a header value permits empty. The character sets are the ones
 * protovalidate's regexes use verbatim, evaluated here by direct character scanning (no regular
 * expressions) so the checks are linear-time and ReDoS-free.
 */
public enum HttpHeaderRule {
    NAME_STRICT("string.well_known_regex.header_name", true),
    NAME_LOOSE("string.well_known_regex.header_name", true),
    VALUE_STRICT("string.well_known_regex.header_value", false),
    VALUE_LOOSE("string.well_known_regex.header_value", false);

    // Strict header-name token characters: protovalidate's class [0-9a-zA-Z!#$%&'*+.^_|~`-]. The
    // class once read "+-.", a range that admitted the comma; 63347b8 moved the hyphen to the end.
    private static final String NAME_TOKEN_SPECIALS = "!#$%&'*+-.^_|~`";

    private final String ruleId;
    private final boolean rejectEmpty;

    HttpHeaderRule(String ruleId, boolean rejectEmpty) {
        this.ruleId = ruleId;
        this.rejectEmpty = rejectEmpty;
    }

    public String ruleId() {
        return ruleId;
    }

    public String emptyRuleId() {
        return ruleId + "_empty";
    }

    /** Whether an empty value is rejected (header name) rather than accepted (header value). */
    public boolean rejectEmpty() {
        return rejectEmpty;
    }

    public boolean matches(String value) {
        return switch (this) {
            case NAME_STRICT -> matchesNameStrict(value);
            case NAME_LOOSE -> !value.isEmpty() && noneOf(value, (char) 0x00, (char) 0x0A, (char) 0x0D);
            case VALUE_STRICT -> matchesValueStrict(value);
            case VALUE_LOOSE -> noneOf(value, (char) 0x00, (char) 0x0A, (char) 0x0D);
        };
    }

    /** {@code ^:?[0-9a-zA-Z!#$%&'*+-.^_|~`]+$}: an optional leading colon then one or more tokens. */
    private static boolean matchesNameStrict(String value) {
        int i = 0;
        if (i < value.length() && value.charAt(i) == ':') {
            i++;
        }
        if (i >= value.length()) {
            return false; // the token run must be non-empty
        }
        for (; i < value.length(); i++) {
            char c = value.charAt(i);
            if (!isAlphanumeric(c) && NAME_TOKEN_SPECIALS.indexOf(c) < 0) {
                return false;
            }
        }
        return true;
    }

    /** {@code ^[^\x00-\x08\x0A-\x1F\x7F]*$}: printable-ish, allowing tab (0x09) but not other controls. */
    private static boolean matchesValueStrict(String value) {
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c <= 0x08 || (c >= 0x0A && c <= 0x1F) || c == 0x7F) {
                return false;
            }
        }
        return true;
    }

    private static boolean noneOf(String value, char a, char b, char c) {
        for (int i = 0; i < value.length(); i++) {
            char ch = value.charAt(i);
            if (ch == a || ch == b || ch == c) {
                return false;
            }
        }
        return true;
    }

    private static boolean isAlphanumeric(char c) {
        return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z');
    }
}
