package ai.pipestream.proto.formats;

/**
 * Validation of endpoint addresses: where a listener can be dialed. An address is either a
 * {@code host:port} authority or an absolute URI, with precedence: a value shaped like
 * {@code host:port} (digits after the last colon, or a bracketed IPv6 literal) must carry a
 * valid host and a valid port, and never falls back to the URI reading. Without the
 * precedence, {@code localhost:99999} would pass as a URI with scheme {@code localhost},
 * which is exactly the confusion the alternation invites. Implemented with direct character
 * scanning (no regular expressions).
 */
public final class Endpoints {

    private Endpoints() {
    }

    /** A dialable endpoint address: a {@code host:port} authority or an absolute URI. */
    public static boolean isEndpointAddress(String value) {
        if (value.isEmpty()) {
            return false;
        }
        if (hasAuthorityShape(value)) {
            // Port zero is bindable but not dialable; leading zeros are already
            // refused, so the zero port is exactly the ":0" suffix.
            return HostAndPort.isHostAndPort(value, true) && !value.endsWith(":0");
        }
        // RFC 3986 admits a scheme with an empty rest ("foo:"), but there is
        // nothing to dial in it; an address URI carries at least one character
        // after the scheme delimiter.
        return Rfc3986.isUri(value) && value.charAt(value.length() - 1) != ':';
    }

    /**
     * Whether the value reads as a {@code host:port} authority rather than a URI: a
     * bracketed IPv6 literal, or a host-shaped half (letters, digits, dots, and dashes,
     * starting alphanumeric) before the last colon and only digits after it. A URI with a
     * numeric port ({@code http://example.com:8080}) has a slash in its "host half" and so
     * never takes the authority reading.
     */
    private static boolean hasAuthorityShape(String value) {
        if (value.charAt(0) == '[') {
            return true;
        }
        int colon = value.lastIndexOf(':');
        if (colon <= 0 || colon == value.length() - 1 || colon > 254) {
            return false;
        }
        char first = value.charAt(0);
        if (!isHostChar(first) || first == '.' || first == '-') {
            return false;
        }
        for (int i = 1; i < colon; i++) {
            if (!isHostChar(value.charAt(i))) {
                return false;
            }
        }
        for (int i = colon + 1; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c < '0' || c > '9') {
                return false;
            }
        }
        return true;
    }

    private static boolean isHostChar(char c) {
        return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9')
                || c == '.' || c == '-';
    }
}
