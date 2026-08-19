package ai.pipestream.format;

/**
 * Facade over the RFC-accurate string format validators in this package, exposing them under the
 * names protovalidate's CEL standard library uses ({@code isHostname}, {@code isEmail},
 * {@code isIp}, {@code isIpPrefix}, {@code isUri}, {@code isUriRef}, {@code isHostAndPort}).
 *
 * <p>Every method is purely syntactic: no DNS resolution, no network access, no normalization.
 * The module has no runtime dependencies, so it is usable on its own.
 */
public final class Formats {

    private Formats() {
    }

    /** RFC 1034 hostname. */
    public static boolean isHostname(String value) {
        return Hostnames.isHostname(value);
    }

    /** WHATWG email address. */
    public static boolean isEmail(String value) {
        return Emails.isEmail(value);
    }

    /** IPv4 or IPv6 address. */
    public static boolean isIp(String value) {
        return IpAddresses.isIp(value, 0);
    }

    /**
     * IP address of a specific version.
     *
     * @param version 4, 6, or 0 for "either"; any other value yields {@code false}
     */
    public static boolean isIp(String value, long version) {
        return isKnownIpVersion(version) && IpAddresses.isIp(value, (int) version);
    }

    /** IPv4 or IPv6 CIDR prefix (host bits need not be zero). */
    public static boolean isIpPrefix(String value) {
        return IpAddresses.isIpPrefix(value, 0, false);
    }

    /** IP CIDR prefix of a specific version. */
    public static boolean isIpPrefix(String value, long version) {
        return isKnownIpVersion(version) && IpAddresses.isIpPrefix(value, (int) version, false);
    }

    /** IP CIDR prefix, optionally requiring host bits to be zero. */
    public static boolean isIpPrefix(String value, long version, boolean strict) {
        return isKnownIpVersion(version) && IpAddresses.isIpPrefix(value, (int) version, strict);
    }

    /** Absolute URI (RFC 3986 {@code URI}). */
    public static boolean isUri(String value) {
        return Rfc3986.isUri(value);
    }

    /** URI reference (RFC 3986 {@code URI-reference}). */
    public static boolean isUriRef(String value) {
        return Rfc3986.isUriReference(value);
    }

    /** {@code host:port}, port optional. */
    public static boolean isHostAndPort(String value, boolean portRequired) {
        return HostAndPort.isHostAndPort(value, portRequired);
    }

    /** Canonical dashed UUID. */
    public static boolean isUuid(String value) {
        return Identifiers.isUuid(value);
    }

    /** Trimmed (dash-less) UUID. */
    public static boolean isTuuid(String value) {
        return Identifiers.isTuuid(value);
    }

    /** ULID. */
    public static boolean isUlid(String value) {
        return Identifiers.isUlid(value);
    }

    /** A fully-qualified protobuf name, e.g. {@code foo.bar.Baz}. */
    public static boolean isProtobufFqn(String value) {
        return Identifiers.isProtobufFqn(value);
    }

    /** The absolute (leading-dot) fully-qualified protobuf name, e.g. {@code .foo.bar.Baz}. */
    public static boolean isProtobufDotFqn(String value) {
        return Identifiers.isProtobufDotFqn(value);
    }

    /** An IP address carrying a prefix length ({@code 192.168.1.1/24}); host bits may be set. */
    public static boolean isIpWithPrefixLen(String value, long version) {
        return isKnownIpVersion(version) && IpAddresses.isIpPrefix(value, (int) version, false);
    }

    /** A hostname or an IP address. */
    public static boolean isAddress(String value) {
        return Hostnames.isHostname(value) || IpAddresses.isIp(value, 0);
    }

    /**
     * The only IP versions these checks understand. The full {@code long} is checked before any
     * narrowing so values such as {@code 4294967300} (which truncates to 4 as an {@code int}) are
     * rejected rather than silently treated as a known version.
     */
    private static boolean isKnownIpVersion(long version) {
        return version == 0 || version == 4 || version == 6;
    }

    /** ISO-8601 calendar date ({@code 2026-07-01}), strictly resolved. */
    public static boolean isDate(String value) {
        return Temporals.isDate(value);
    }

    /** RFC 3339 date-time with an offset ({@code 2026-07-01T12:00:00Z}). */
    public static boolean isDateTime(String value) {
        return Temporals.isDateTime(value);
    }

    /** IANA media type per the RFC 6838 grammar ({@code type/subtype}). */
    public static boolean isMediaType(String value) {
        return MediaTypes.isMediaType(value);
    }

    /** Well-formed BCP 47 language tag, via the JDK's strict parser. */
    public static boolean isLanguageTag(String value) {
        return Vocabularies.isLanguageTag(value);
    }

    /** ISO 4217 currency code known to the JDK's own table. */
    public static boolean isCurrencyCode(String value) {
        return Vocabularies.isCurrencyCode(value);
    }

    /** Display-tolerant E.164 telephone number. */
    public static boolean isPhoneNumber(String value) {
        return PhoneNumbers.isPhoneNumber(value);
    }

    /** Lowercase hexadecimal of any positive length. */
    public static boolean isHex(String value) {
        return Encodings.isHex(value);
    }

    /** Lowercase hex SHA-256 digest (64 chars). */
    public static boolean isSha256Hex(String value) {
        return Encodings.isSha256Hex(value);
    }

    /** Lowercase hex SHA-1 digest (40 chars). */
    public static boolean isSha1Hex(String value) {
        return Encodings.isSha1Hex(value);
    }

    /** RFC 4648 base64 (standard alphabet, correct padding). */
    public static boolean isBase64(String value) {
        return Encodings.isBase64(value);
    }

    /**
     * Lowercase slug: {@code a-z0-9} with interior single {@code .}, {@code _} or
     * {@code -} separators, starting and ending alphanumeric. Compose with len
     * rules for length bounds.
     */
    public static boolean isSlug(String value) {
        return Identifiers.isSlug(value);
    }

    /** ISO 3166-1 alpha-2 region code from the JDK's own table ({@code US}). */
    public static boolean isRegionCode(String value) {
        return Vocabularies.isRegionCode(value);
    }

    /**
     * Path-safe reference name: an ASCII letter or digit followed by letters, digits and
     * {@code . _ -}. The mixed-origin name family (aliases carrying service FQNs, sanitized
     * endpoints) where slug is too narrow. Compose with len rules for length bounds.
     */
    public static boolean isPathSafeName(String value) {
        return Identifiers.isPathSafeName(value);
    }

    /**
     * GS1 Global Trade Item Number: 8, 12, 13, or 14 digits with a valid mod-10 check digit
     * ({@code 00012345678905}).
     */
    public static boolean isGtin(String value) {
        return TradeItemNumbers.isGtin(value);
    }

    /** Unsigned decimal string: {@code digits} or {@code digits.digits} ({@code 19.99}). */
    public static boolean isDecimal(String value) {
        return Decimals.isDecimal(value);
    }
}
