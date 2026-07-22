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
}
