package ai.pipestream.format;

/**
 * RFC 1034 hostname syntax validation.
 *
 * <p>A hostname is a series of dot-separated labels. Each label is 1–63 characters of ASCII
 * letters, digits, and hyphens, and may not begin or end with a hyphen. The whole name is at most
 * 253 characters, a single trailing dot is permitted, and the rightmost label may not be entirely
 * numeric (so an IPv4 address is not mistaken for a hostname). Validation is purely syntactic — no
 * DNS resolution and no public-suffix awareness.
 */
public final class Hostnames {

    private static final int MAX_NAME = 253;
    private static final int MAX_LABEL = 63;

    private Hostnames() {
    }

    /** Returns whether {@code value} is a syntactically valid hostname. */
    public static boolean isHostname(String value) {
        if (value.isEmpty() || value.length() > MAX_NAME) {
            return false;
        }
        String name = value.endsWith(".") ? value.substring(0, value.length() - 1) : value;
        if (name.isEmpty()) {
            return false;
        }

        int labelStart = 0;
        boolean lastLabelAllDigits = false;
        for (int i = 0; i <= name.length(); i++) {
            if (i == name.length() || name.charAt(i) == '.') {
                if (!validLabel(name, labelStart, i)) {
                    return false;
                }
                lastLabelAllDigits = allDigits(name, labelStart, i);
                labelStart = i + 1;
            }
        }
        // The top-level label must not be entirely numeric.
        return !lastLabelAllDigits;
    }

    private static boolean validLabel(String name, int start, int end) {
        int len = end - start;
        if (len == 0 || len > MAX_LABEL) {
            return false;
        }
        if (name.charAt(start) == '-' || name.charAt(end - 1) == '-') {
            return false;
        }
        for (int i = start; i < end; i++) {
            char c = name.charAt(i);
            boolean ok = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
                    || (c >= '0' && c <= '9') || c == '-';
            if (!ok) {
                return false;
            }
        }
        return true;
    }

    private static boolean allDigits(String name, int start, int end) {
        for (int i = start; i < end; i++) {
            char c = name.charAt(i);
            if (c < '0' || c > '9') {
                return false;
            }
        }
        return true;
    }
}
