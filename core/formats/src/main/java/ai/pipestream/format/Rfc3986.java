package ai.pipestream.format;

/**
 * URI and URI-reference validation per RFC 3986.
 *
 * <p>Implemented directly from the RFC 3986 ABNF: a value is decomposed into scheme, authority,
 * path, query, and fragment following the component grammar (Appendix B), then each component is
 * checked against its production — including {@code pct-encoded} triplets, the authority's
 * userinfo/host/port structure (with bracketed IPv6 / IPvFuture literals), and the four path forms
 * (abempty, absolute, rootless, noscheme). It is a syntactic validator with no normalization or
 * scheme-specific rules, so it applies uniformly to any URI scheme.
 *
 * <p>{@link #isUri(String)} requires an absolute URI (a scheme is mandatory);
 * {@link #isUriReference(String)} additionally accepts relative references.
 */
public final class Rfc3986 {

    private static final String SUB_DELIMS = "!$&'()*+,;=";

    private Rfc3986() {
    }

    /** Returns whether {@code value} is a valid absolute URI (RFC 3986 {@code URI}). */
    public static boolean isUri(String value) {
        String[] parts = splitQueryFragment(value);
        String core = parts[0];
        String query = parts[1];
        String fragment = parts[2];

        int colon = core.indexOf(':');
        if (colon < 0) {
            return false; // a URI must have a scheme
        }
        int slash = core.indexOf('/');
        if (slash >= 0 && slash < colon) {
            return false; // the ':' is inside a path segment, not a scheme delimiter
        }
        if (!validScheme(core.substring(0, colon))) {
            return false;
        }
        return validHierPart(core.substring(colon + 1), false)
                && validQueryFragment(query)
                && validQueryFragment(fragment);
    }

    /** Returns whether {@code value} is a valid URI-reference (absolute URI or relative reference). */
    public static boolean isUriReference(String value) {
        if (isUri(value)) {
            return true;
        }
        String[] parts = splitQueryFragment(value);
        return validHierPart(parts[0], true)
                && validQueryFragment(parts[1])
                && validQueryFragment(parts[2]);
    }

    /** Splits off {@code #fragment} then {@code ?query}; returns {core, query|null, fragment|null}. */
    private static String[] splitQueryFragment(String value) {
        String fragment = null;
        int hash = value.indexOf('#');
        if (hash >= 0) {
            fragment = value.substring(hash + 1);
            value = value.substring(0, hash);
        }
        String query = null;
        int q = value.indexOf('?');
        if (q >= 0) {
            query = value.substring(q + 1);
            value = value.substring(0, q);
        }
        return new String[] {value, query, fragment};
    }

    private static boolean validScheme(String s) {
        if (s.isEmpty() || !isAlpha(s.charAt(0))) {
            return false;
        }
        for (int i = 1; i < s.length(); i++) {
            char c = s.charAt(i);
            if (!isAlpha(c) && !isDigit(c) && c != '+' && c != '-' && c != '.') {
                return false;
            }
        }
        return true;
    }

    /** Validates a hier-part (absolute) or relative-part (relative). Query/fragment already removed. */
    private static boolean validHierPart(String rest, boolean relative) {
        if (rest.startsWith("//")) {
            String afterSlashes = rest.substring(2);
            int slash = afterSlashes.indexOf('/');
            String authority = slash < 0 ? afterSlashes : afterSlashes.substring(0, slash);
            String path = slash < 0 ? "" : afterSlashes.substring(slash);
            return validAuthority(authority) && validPathAbempty(path);
        }
        if (rest.isEmpty()) {
            return true; // path-empty
        }
        if (rest.startsWith("/")) {
            return validPathAbsolute(rest);
        }
        return relative ? validPathNoscheme(rest) : validPathRootless(rest);
    }

    private static boolean validAuthority(String authority) {
        String hostPort = authority;
        int at = authority.indexOf('@');
        if (at >= 0) {
            if (!validComponent(authority.substring(0, at), ":")) {
                return false; // userinfo
            }
            hostPort = authority.substring(at + 1);
        }
        if (hostPort.startsWith("[")) {
            int close = hostPort.indexOf(']');
            if (close < 0) {
                return false;
            }
            if (!validIpLiteral(hostPort.substring(1, close))) {
                return false;
            }
            String after = hostPort.substring(close + 1);
            if (after.isEmpty()) {
                return true;
            }
            return after.charAt(0) == ':' && isPortStar(after.substring(1));
        }
        int colon = hostPort.indexOf(':');
        String host = colon < 0 ? hostPort : hostPort.substring(0, colon);
        // reg-name (which subsumes IPv4address): pct-encoded and sub-delims permitted, and the
        // pct-decoded octets must form valid UTF-8.
        if (!validHost(host)) {
            return false;
        }
        return colon < 0 || isPortStar(hostPort.substring(colon + 1));
    }

    private static boolean validIpLiteral(String inner) {
        if (!inner.isEmpty() && (inner.charAt(0) == 'v' || inner.charAt(0) == 'V')) {
            return validIpvFuture(inner);
        }
        // IP-literal = "[" ( IPv6address / IPv6addrz / IPvFuture ) "]", where a zone id is written
        // percent-encoded: IPv6addrz = IPv6address "%25" ZoneID, ZoneID = 1*( unreserved / pct-encoded ).
        int zone = inner.indexOf("%25");
        if (zone >= 0) {
            String address = inner.substring(0, zone);
            String zoneId = inner.substring(zone + 3);
            return !zoneId.isEmpty() && validZoneId(zoneId) && IpAddresses.parseIpv6(address) != null;
        }
        // A bare "%" is not a valid IPv6 character: an unencoded zone id (%eth0) is rejected here.
        if (inner.indexOf('%') >= 0) {
            return false;
        }
        return IpAddresses.parseIpv6(inner) != null;
    }

    /** IPvFuture = "v" 1*HEXDIG "." 1*( unreserved / sub-delims / ":" ). */
    private static boolean validIpvFuture(String s) {
        int dot = s.indexOf('.');
        if (dot < 2) {
            return false; // needs "v" then at least one hex digit before "."
        }
        for (int i = 1; i < dot; i++) {
            if (!isHex(s.charAt(i))) {
                return false;
            }
        }
        String tail = s.substring(dot + 1);
        if (tail.isEmpty()) {
            return false;
        }
        for (int i = 0; i < tail.length(); i++) {
            char c = tail.charAt(i);
            if (!isUnreserved(c) && SUB_DELIMS.indexOf(c) < 0 && c != ':') {
                return false;
            }
        }
        return true;
    }

    /** port = *DIGIT (empty permitted in a URI authority). */
    private static boolean isPortStar(String s) {
        for (int i = 0; i < s.length(); i++) {
            if (!isDigit(s.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    private static boolean validPathAbempty(String path) {
        // *( "/" segment ): empty, or a run of "/"-prefixed segments.
        if (path.isEmpty()) {
            return true;
        }
        if (path.charAt(0) != '/') {
            return false;
        }
        return validSegments(path.substring(1));
    }

    private static boolean validPathAbsolute(String path) {
        // "/" [ segment-nz *( "/" segment ) ]: first segment (if any) must be non-empty.
        String body = path.substring(1);
        if (body.isEmpty()) {
            return true; // just "/"
        }
        int firstSlash = body.indexOf('/');
        String firstSegment = firstSlash < 0 ? body : body.substring(0, firstSlash);
        if (firstSegment.isEmpty()) {
            return false; // "//..." is an authority, not path-absolute
        }
        return validSegments(body);
    }

    private static boolean validPathRootless(String path) {
        return validFirstSegmentPath(path, false);
    }

    private static boolean validPathNoscheme(String path) {
        return validFirstSegmentPath(path, true);
    }

    /** Rootless/noscheme: first segment non-empty; noscheme additionally forbids ':' in it. */
    private static boolean validFirstSegmentPath(String path, boolean noColonInFirst) {
        int firstSlash = path.indexOf('/');
        String firstSegment = firstSlash < 0 ? path : path.substring(0, firstSlash);
        if (firstSegment.isEmpty()) {
            return false;
        }
        if (!validComponent(firstSegment, noColonInFirst ? "@" : ":@")) {
            return false;
        }
        if (firstSlash < 0) {
            return true;
        }
        return validSegments(path.substring(firstSlash + 1));
    }

    /** Validates '/'-separated segments (each may be empty) as pchar*. */
    private static boolean validSegments(String s) {
        int start = 0;
        for (int i = 0; i <= s.length(); i++) {
            if (i == s.length() || s.charAt(i) == '/') {
                if (!validComponent(s.substring(start, i), ":@")) {
                    return false;
                }
                start = i + 1;
            }
        }
        return true;
    }

    private static boolean validQueryFragment(String s) {
        // query / fragment = *( pchar / "/" / "?" )
        return s == null || validComponent(s, ":@/?");
    }

    /**
     * Validates that {@code s} consists only of {@code unreserved}, {@code sub-delims}, any of the
     * {@code extra} characters, and well-formed {@code pct-encoded} triplets.
     */
    private static boolean validComponent(String s, String extra) {
        int i = 0;
        while (i < s.length()) {
            char c = s.charAt(i);
            if (c == '%') {
                if (i + 2 >= s.length() || !isHex(s.charAt(i + 1)) || !isHex(s.charAt(i + 2))) {
                    return false;
                }
                i += 3;
            } else if (isUnreserved(c) || SUB_DELIMS.indexOf(c) >= 0 || extra.indexOf(c) >= 0) {
                i++;
            } else {
                return false;
            }
        }
        return true;
    }

    /**
     * As {@link #validComponent} but for a host reg-name (or IPv6 zone id): every {@code pct-encoded}
     * octet is decoded and the resulting byte sequence must be valid UTF-8. This rejects hosts such
     * as {@code foo%c3x%96} whose percent-encoding does not decode to well-formed UTF-8.
     */
    private static boolean validHost(String s) {
        java.io.ByteArrayOutputStream bytes = new java.io.ByteArrayOutputStream(s.length());
        int i = 0;
        while (i < s.length()) {
            char c = s.charAt(i);
            if (c == '%') {
                if (i + 2 >= s.length() || !isHex(s.charAt(i + 1)) || !isHex(s.charAt(i + 2))) {
                    return false;
                }
                bytes.write(Integer.parseInt(s.substring(i + 1, i + 3), 16));
                i += 3;
            } else if (isUnreserved(c) || SUB_DELIMS.indexOf(c) >= 0) {
                bytes.write(c);
                i++;
            } else {
                return false;
            }
        }
        return isValidUtf8(bytes.toByteArray());
    }

    /**
     * RFC 6874 {@code ZoneID = 1*( unreserved / pct-encoded )}. Unlike a host reg-name,
     * {@code sub-delims} are NOT permitted, so {@code [fe80::1%25e!0]} is rejected. As with
     * {@link #validHost}, the pct-decoded octets must form valid UTF-8.
     */
    private static boolean validZoneId(String s) {
        java.io.ByteArrayOutputStream bytes = new java.io.ByteArrayOutputStream(s.length());
        int i = 0;
        while (i < s.length()) {
            char c = s.charAt(i);
            if (c == '%') {
                if (i + 2 >= s.length() || !isHex(s.charAt(i + 1)) || !isHex(s.charAt(i + 2))) {
                    return false;
                }
                bytes.write(Integer.parseInt(s.substring(i + 1, i + 3), 16));
                i += 3;
            } else if (isUnreserved(c)) {
                bytes.write(c);
                i++;
            } else {
                return false;
            }
        }
        return isValidUtf8(bytes.toByteArray());
    }

    private static boolean isValidUtf8(byte[] bytes) {
        try {
            java.nio.charset.StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(java.nio.charset.CodingErrorAction.REPORT)
                    .onUnmappableCharacter(java.nio.charset.CodingErrorAction.REPORT)
                    .decode(java.nio.ByteBuffer.wrap(bytes));
            return true;
        } catch (java.nio.charset.CharacterCodingException e) {
            return false;
        }
    }

    private static boolean isUnreserved(char c) {
        return isAlpha(c) || isDigit(c) || c == '-' || c == '.' || c == '_' || c == '~';
    }

    private static boolean isAlpha(char c) {
        return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z');
    }

    private static boolean isDigit(char c) {
        return c >= '0' && c <= '9';
    }

    private static boolean isHex(char c) {
        return isDigit(c) || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F');
    }
}
