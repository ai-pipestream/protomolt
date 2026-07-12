package ai.pipestream.proto.validate.model;

import java.util.function.IntPredicate;

/**
 * Well-known bytes formats a {@link BytesConstraints} can demand. Unlike string formats these are
 * checks on the packed byte length: an IPv4 address is 4 bytes, an IPv6 address and a UUID are 16.
 */
public enum BytesFormat {
    IP("bytes.ip", "value must be a valid IP address", n -> n == 4 || n == 16),
    IPV4("bytes.ipv4", "value must be a valid IPv4 address", n -> n == 4),
    IPV6("bytes.ipv6", "value must be a valid IPv6 address", n -> n == 16),
    UUID("bytes.uuid", "value must be a valid UUID", n -> n == 16);

    private final String ruleId;
    private final String defaultMessage;
    private final IntPredicate lengthTest;

    BytesFormat(String ruleId, String defaultMessage, IntPredicate lengthTest) {
        this.ruleId = ruleId;
        this.defaultMessage = defaultMessage;
        this.lengthTest = lengthTest;
    }

    public String ruleId() {
        return ruleId;
    }

    /** Rule id reported when the value is empty, e.g. {@code bytes.uuid_empty}. */
    public String emptyRuleId() {
        return ruleId + "_empty";
    }

    public String defaultMessage() {
        return defaultMessage;
    }

    /** True when a byte array of {@code length} bytes satisfies this format. */
    public boolean matches(int length) {
        return lengthTest.test(length);
    }
}
