package ai.pipestream.format;

/**
 * IPv4 and IPv6 address and CIDR-prefix validation, implemented from the address grammars in
 * RFC 3986 §3.2.2 (IPv4address, IPv6address) and RFC 4291 §2.2.
 *
 * <p>Acceptance is strict and canonical: dotted-decimal octets carry no leading zeros, IPv6 permits
 * at most one {@code ::} zero-compression and an optional trailing embedded IPv4. Prefix validation
 * additionally supports a {@code strict} mode that requires the host bits below the prefix length to
 * be zero (i.e. the string names a network, not a host within it). No DNS resolution.
 */
public final class IpAddresses {

    private IpAddresses() {
    }

    /** Valid IPv4 dotted-decimal address. */
    public static boolean isIpv4(String value) {
        return parseIpv4(value) != null;
    }

    /**
     * Valid IPv6 address (with optional {@code ::} compression and embedded IPv4), optionally
     * carrying an RFC 4007 zone identifier ({@code fe80::1%eth0}). The zone is any non-empty run of
     * characters after {@code %} that contains no NUL.
     */
    public static boolean isIpv6(String value) {
        int pct = value.indexOf('%');
        if (pct >= 0) {
            String zone = value.substring(pct + 1);
            if (zone.isEmpty() || zone.indexOf('\0') >= 0) {
                return false;
            }
            value = value.substring(0, pct);
        }
        return parseIpv6(value) != null;
    }

    /**
     * Valid IP address of the given version.
     *
     * @param version 4, 6, or 0 for "either"
     */
    public static boolean isIp(String value, int version) {
        return switch (version) {
            case 4 -> isIpv4(value);
            case 6 -> isIpv6(value);
            case 0 -> isIpv4(value) || isIpv6(value);
            default -> false;
        };
    }

    /**
     * Valid CIDR prefix of the given version.
     *
     * @param version 4, 6, or 0 for "either"
     * @param strict when true, the host bits below the prefix length must be zero
     */
    public static boolean isIpPrefix(String value, int version, boolean strict) {
        return switch (version) {
            case 4 -> isIpv4Prefix(value, strict);
            case 6 -> isIpv6Prefix(value, strict);
            case 0 -> isIpv4Prefix(value, strict) || isIpv6Prefix(value, strict);
            default -> false;
        };
    }

    private static boolean isIpv4Prefix(String value, boolean strict) {
        int slash = value.indexOf('/');
        if (slash < 0 || value.indexOf('/', slash + 1) >= 0) {
            return false;
        }
        byte[] addr = parseIpv4(value.substring(0, slash));
        if (addr == null) {
            return false;
        }
        int bits = parsePrefixLength(value.substring(slash + 1), 32);
        if (bits < 0) {
            return false;
        }
        return !strict || hostBitsZero(addr, bits);
    }

    private static boolean isIpv6Prefix(String value, boolean strict) {
        int slash = value.indexOf('/');
        if (slash < 0 || value.indexOf('/', slash + 1) >= 0) {
            return false;
        }
        byte[] addr = parseIpv6(value.substring(0, slash));
        if (addr == null) {
            return false;
        }
        int bits = parsePrefixLength(value.substring(slash + 1), 128);
        if (bits < 0) {
            return false;
        }
        return !strict || hostBitsZero(addr, bits);
    }

    /** Parses a prefix length in [0, max]; returns -1 if malformed. No leading zeros. */
    private static int parsePrefixLength(String s, int max) {
        if (s.isEmpty() || s.length() > 3) {
            return -1;
        }
        if (s.length() > 1 && s.charAt(0) == '0') {
            return -1;
        }
        int value = 0;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c < '0' || c > '9') {
                return -1;
            }
            value = value * 10 + (c - '0');
        }
        return value <= max ? value : -1;
    }

    private static boolean hostBitsZero(byte[] addr, int prefixLen) {
        for (int bit = prefixLen; bit < addr.length * 8; bit++) {
            int b = addr[bit >>> 3] & 0xFF;
            if ((b & (0x80 >>> (bit & 7))) != 0) {
                return false;
            }
        }
        return true;
    }

    /** RFC 3986 IPv4address = dec-octet "." dec-octet "." dec-octet "." dec-octet. */
    static byte[] parseIpv4(String s) {
        byte[] out = new byte[4];
        int octet = 0;
        int start = 0;
        for (int i = 0; i <= s.length(); i++) {
            if (i == s.length() || s.charAt(i) == '.') {
                if (octet == 4) {
                    return null;
                }
                int value = parseDecOctet(s, start, i);
                if (value < 0) {
                    return null;
                }
                out[octet++] = (byte) value;
                start = i + 1;
            }
        }
        return octet == 4 ? out : null;
    }

    /** dec-octet: 1–3 digits, value 0–255, no leading zeros. */
    private static int parseDecOctet(String s, int start, int end) {
        int len = end - start;
        if (len < 1 || len > 3) {
            return -1;
        }
        if (len > 1 && s.charAt(start) == '0') {
            return -1;
        }
        int value = 0;
        for (int i = start; i < end; i++) {
            char c = s.charAt(i);
            if (c < '0' || c > '9') {
                return -1;
            }
            value = value * 10 + (c - '0');
        }
        return value <= 255 ? value : -1;
    }

    /** RFC 4291 IPv6address, allowing one {@code ::} and an optional trailing embedded IPv4. */
    static byte[] parseIpv6(String s) {
        if (s.isEmpty()) {
            return null;
        }
        int dbl = s.indexOf("::");
        boolean compressed = dbl >= 0;
        if (compressed && s.indexOf("::", dbl + 1) >= 0) {
            return null; // more than one "::"
        }

        String leftStr = compressed ? s.substring(0, dbl) : s;
        String rightStr = compressed ? s.substring(dbl + 2) : "";
        String[] left = tokenize(leftStr);
        String[] right = tokenize(rightStr);
        if (left == null || right == null) {
            return null;
        }

        byte[] out = new byte[16];
        if (!compressed) {
            // Exactly 8 hextets, or 6 hextets plus a trailing embedded IPv4 (16 bytes total).
            return fillGroups(left, out, 0, true) == 16 ? out : null;
        }

        // Compressed: the left side is followed by the zero gap, so it never carries a trailing
        // IPv4; the right side (after the gap) may end in an embedded IPv4.
        int head = fillGroups(left, out, 0, false);
        if (head < 0) {
            return null;
        }
        byte[] tailBuf = new byte[16];
        int tail = fillGroups(right, tailBuf, 0, true);
        if (tail < 0 || head + tail >= 16) {
            return null; // "::" must compress at least one 16-bit group
        }
        System.arraycopy(tailBuf, 0, out, 16 - tail, tail);
        return out;
    }

    /** Splits a half on ':' returning null if any empty token appears (stray colon). */
    private static String[] tokenize(String half) {
        if (half.isEmpty()) {
            return new String[0];
        }
        String[] parts = half.split(":", -1);
        for (String p : parts) {
            if (p.isEmpty()) {
                return null;
            }
        }
        return parts;
    }

    /**
     * Writes {@code tokens} into {@code out} starting at {@code offset}. A token containing a dot is
     * an embedded IPv4 address (4 bytes) and is permitted only as the final token, and only when
     * {@code allowTrailingIpv4} is set; every other token is a 1–4 digit hextet. Returns the number
     * of bytes written, or -1 on error.
     */
    private static int fillGroups(String[] tokens, byte[] out, int offset, boolean allowTrailingIpv4) {
        int pos = offset;
        for (int i = 0; i < tokens.length; i++) {
            String token = tokens[i];
            if (token.indexOf('.') >= 0) {
                if (!(allowTrailingIpv4 && i == tokens.length - 1)) {
                    return -1;
                }
                byte[] v4 = parseIpv4(token);
                if (v4 == null || pos + 4 > out.length) {
                    return -1;
                }
                System.arraycopy(v4, 0, out, pos, 4);
                pos += 4;
            } else {
                int hextet = parseHextet(token);
                if (hextet < 0 || pos + 2 > out.length) {
                    return -1;
                }
                out[pos] = (byte) (hextet >>> 8);
                out[pos + 1] = (byte) hextet;
                pos += 2;
            }
        }
        return pos - offset;
    }

    /** A hextet: 1–4 hex digits, value 0–0xFFFF. */
    private static int parseHextet(String s) {
        int len = s.length();
        if (len < 1 || len > 4) {
            return -1;
        }
        int value = 0;
        for (int i = 0; i < len; i++) {
            int d = Character.digit(s.charAt(i), 16);
            // Character.digit accepts non-ASCII digits; restrict to ASCII hex.
            char c = s.charAt(i);
            boolean asciiHex = (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F');
            if (d < 0 || !asciiHex) {
                return -1;
            }
            value = (value << 4) | d;
        }
        return value;
    }
}
