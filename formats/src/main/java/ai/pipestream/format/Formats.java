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
     * @param version 4, 6, or 0 for "either"
     */
    public static boolean isIp(String value, long version) {
        return IpAddresses.isIp(value, (int) version);
    }

    /** IPv4 or IPv6 CIDR prefix (host bits need not be zero). */
    public static boolean isIpPrefix(String value) {
        return IpAddresses.isIpPrefix(value, 0, false);
    }

    /** IP CIDR prefix of a specific version. */
    public static boolean isIpPrefix(String value, long version) {
        return IpAddresses.isIpPrefix(value, (int) version, false);
    }

    /** IP CIDR prefix, optionally requiring host bits to be zero. */
    public static boolean isIpPrefix(String value, long version, boolean strict) {
        return IpAddresses.isIpPrefix(value, (int) version, strict);
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

    /** A hostname or an IP address. */
    public static boolean isAddress(String value) {
        return Hostnames.isHostname(value) || IpAddresses.isIp(value, 0);
    }
}
