package ai.pipestream.proto.validate.model;

import java.util.regex.Pattern;

/**
 * The {@code well_known_regex} HTTP-header string rules (RFC 7230), in their strict and loose
 * forms. A header name additionally rejects the empty string (reported under the
 * {@code …header_name_empty} rule); a header value permits empty. Regexes are the ones protovalidate
 * uses verbatim.
 */
public enum HttpHeaderRule {
    NAME_STRICT("string.well_known_regex.header_name", "^:?[0-9a-zA-Z!#$%&'*+-.^_|~`]+$", true),
    NAME_LOOSE("string.well_known_regex.header_name", "^[^\\x00\\x0A\\x0D]+$", true),
    VALUE_STRICT("string.well_known_regex.header_value", "^[^\\x00-\\x08\\x0A-\\x1F\\x7F]*$", false),
    VALUE_LOOSE("string.well_known_regex.header_value", "^[^\\x00\\x0A\\x0D]*$", false);

    private final String ruleId;
    private final Pattern pattern;
    private final boolean rejectEmpty;

    HttpHeaderRule(String ruleId, String pattern, boolean rejectEmpty) {
        this.ruleId = ruleId;
        this.pattern = Pattern.compile(pattern);
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
        return pattern.matcher(value).matches();
    }
}
