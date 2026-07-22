package ai.pipestream.format;

/**
 * Validation of {@code host:port} authorities.
 *
 * <p>The host is a hostname or an IPv4 address, or a bracketed IPv6 literal ({@code [::1]}). The
 * port, when present, is a decimal number in 0–65535 with no leading zeros. Whether the port is
 * mandatory is controlled by the caller.
 */
public final class HostAndPort {

    private HostAndPort() {
    }

    /**
     * Returns whether {@code value} is a valid host-and-port.
     *
     * @param portRequired when true, a port must be present
     */
    public static boolean isHostAndPort(String value, boolean portRequired) {
        if (value.isEmpty()) {
            return false;
        }
        if (value.charAt(0) == '[') {
            // Bracketed IPv6, optionally followed by ":port".
            int close = value.lastIndexOf(']');
            if (close < 0) {
                return false;
            }
            String host = value.substring(1, close);
            String rest = value.substring(close + 1);
            if (rest.isEmpty()) {
                return !portRequired && IpAddresses.isIp(host, 6);
            }
            if (rest.charAt(0) != ':') {
                return false;
            }
            return IpAddresses.isIp(host, 6) && isPort(rest.substring(1));
        }

        int split = value.lastIndexOf(':');
        if (split < 0) {
            return !portRequired && (Hostnames.isHostname(value) || IpAddresses.isIp(value, 4));
        }
        String host = value.substring(0, split);
        String port = value.substring(split + 1);
        return (Hostnames.isHostname(host) || IpAddresses.isIp(host, 4)) && isPort(port);
    }

    /** A port: 1–5 decimal digits, value 0–65535, no leading zeros. */
    public static boolean isPort(String value) {
        if (value.isEmpty() || value.length() > 5) {
            return false;
        }
        if (value.length() > 1 && value.charAt(0) == '0') {
            return false;
        }
        int port = 0;
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c < '0' || c > '9') {
                return false;
            }
            port = port * 10 + (c - '0');
        }
        return port <= 65535;
    }
}
