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
    IP_WITH_PREFIXLEN("string.ip_with_prefixlen", "value must be a valid IP address with prefix length",
            v -> Formats.isIpWithPrefixLen(v, 0)),
    IPV4_WITH_PREFIXLEN("string.ipv4_with_prefixlen", "value must be a valid IPv4 address with prefix length",
            v -> Formats.isIpWithPrefixLen(v, 4)),
    IPV6_WITH_PREFIXLEN("string.ipv6_with_prefixlen", "value must be a valid IPv6 address with prefix length",
            v -> Formats.isIpWithPrefixLen(v, 6)),
    PROTOBUF_FQN("string.protobuf_fqn", "value must be a valid protobuf fully-qualified name",
            Formats::isProtobufFqn),
    PROTOBUF_DOT_FQN("string.protobuf_dot_fqn", "value must be a valid protobuf fully-qualified name",
            Formats::isProtobufDotFqn),
    HOST_AND_PORT("string.host_and_port", "value must be a valid host and port pair",
            v -> Formats.isHostAndPort(v, true)),
    DATE("string.date", "value must be an ISO-8601 calendar date", Formats::isDate),
    DATE_TIME("string.date_time", "value must be an RFC 3339 date-time",
            Formats::isDateTime),
    MIME_TYPE("string.mime_type", "value must be an IANA media type (type/subtype)",
            Formats::isMediaType),
    LANGUAGE_TAG("string.language_tag", "value must be a BCP 47 language tag",
            Formats::isLanguageTag),
    CURRENCY_CODE("string.currency_code", "value must be an ISO 4217 currency code",
            Formats::isCurrencyCode),
    PHONE_NUMBER("string.phone_number", "value must be a telephone number",
            Formats::isPhoneNumber),
    SHA256_HEX("string.sha256_hex", "value must be a lowercase hex SHA-256 digest",
            Formats::isSha256Hex),
    SHA1_HEX("string.sha1_hex", "value must be a lowercase hex SHA-1 digest",
            Formats::isSha1Hex),
    HEX("string.hex", "value must be lowercase hexadecimal", Formats::isHex),
    BASE64("string.base64", "value must be base64", Formats::isBase64),
    SLUG("string.slug", "value must be a lowercase slug (a-z0-9 with interior ., _ or -)",
            Formats::isSlug),
    REGION_CODE("string.region_code", "value must be an ISO 3166-1 alpha-2 region code",
            Formats::isRegionCode),
    PATH_SAFE_NAME("string.path_safe_name",
            "value must be a path-safe name (a letter or digit, then letters, digits, ., _ or -)",
            Formats::isPathSafeName),
    GTIN("string.gtin",
            "value must be a GTIN (8, 12, 13, or 14 digits with a valid check digit)",
            Formats::isGtin),
    DECIMAL("string.decimal", "value must be an unsigned decimal string",
            Formats::isDecimal),
    ENDPOINT_ADDRESS("string.endpoint_address",
            "value must be a host:port pair with a valid port or an absolute URI",
            Formats::isEndpointAddress),
    IDENTIFIER("string.identifier",
            "value must be a bare identifier (a letter or underscore, then letters, digits,"
                    + " or underscores)",
            Formats::isIdentifier);

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
