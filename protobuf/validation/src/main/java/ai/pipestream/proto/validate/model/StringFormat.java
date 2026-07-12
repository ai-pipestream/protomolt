package ai.pipestream.proto.validate.model;

import ai.pipestream.format.Formats;

import java.util.function.Predicate;

/**
 * Well-known string formats a {@link StringConstraints} can demand. Each format carries its stable
 * violation rule id and delegates the actual test to the RFC-accurate {@code ai.pipestream.format}
 * validators. Checks are purely syntactic (no DNS lookups or network access).
 *
 * <p>Matching protovalidate, an empty string satisfies the format check itself — an empty value is
 * reported under the companion {@code <id>_empty} rule instead (see {@link #emptyRuleId()}).
 */
public enum StringFormat {
    EMAIL("string.email", "value must be a valid email address", Formats::isEmail),
    HOSTNAME("string.hostname", "value must be a valid hostname", Formats::isHostname),
    ADDRESS("string.address", "value must be a valid hostname or IP address", Formats::isAddress),
    UUID("string.uuid", "value must be a valid UUID", Formats::isUuid),
    TUUID("string.tuuid", "value must be a valid trimmed UUID", Formats::isTuuid),
    ULID("string.ulid", "value must be a valid ULID", Formats::isUlid),
    URI("string.uri", "value must be an absolute URI", Formats::isUri),
    URI_REF("string.uri_ref", "value must be a valid URI reference", Formats::isUriRef),
    IP("string.ip", "value must be a valid IP address", Formats::isIp),
    IPV4("string.ipv4", "value must be a valid IPv4 address", v -> Formats.isIp(v, 4)),
    IPV6("string.ipv6", "value must be a valid IPv6 address", v -> Formats.isIp(v, 6)),
    IP_PREFIX("string.ip_prefix", "value must be a valid IP prefix", v -> Formats.isIpPrefix(v, 0, true)),
    IPV4_PREFIX("string.ipv4_prefix", "value must be a valid IPv4 prefix", v -> Formats.isIpPrefix(v, 4, true)),
    IPV6_PREFIX("string.ipv6_prefix", "value must be a valid IPv6 prefix", v -> Formats.isIpPrefix(v, 6, true)),
    HOST_AND_PORT("string.host_and_port", "value must be a valid host and port pair",
            v -> Formats.isHostAndPort(v, true));

    private final String ruleId;
    private final String defaultMessage;
    private final Predicate<String> test;

    StringFormat(String ruleId, String defaultMessage, Predicate<String> test) {
        this.ruleId = ruleId;
        this.defaultMessage = defaultMessage;
        this.test = test;
    }

    /** Stable violation rule id for a non-empty value that fails the format, e.g. {@code string.email}. */
    public String ruleId() {
        return ruleId;
    }

    /** Rule id reported when the value is empty, e.g. {@code string.email_empty}. */
    public String emptyRuleId() {
        return ruleId + "_empty";
    }

    public String defaultMessage() {
        return defaultMessage;
    }

    public String emptyMessage() {
        return "value is empty";
    }

    /** True when {@code value} satisfies this format. */
    public boolean matches(String value) {
        return test.test(value);
    }
}
