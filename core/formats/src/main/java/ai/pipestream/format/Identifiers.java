package ai.pipestream.format;

/**
 * Validation of the common identifier string formats: UUID (RFC 4122 textual form), its trimmed
 * (dash-less) variant, ULID (Crockford base-32), and protobuf fully-qualified names. Purely
 * syntactic, and implemented with direct character scanning (no regular expressions) so the checks
 * are linear-time and free of catastrophic-backtracking risk.
 */
public final class Identifiers {

    private Identifiers() {
    }

    /** Canonical dashed UUID, e.g. {@code 123e4567-e89b-12d3-a456-426614174000} (8-4-4-4-12 hex). */
    public static boolean isUuid(String value) {
        if (value.length() != 36) {
            return false;
        }
        for (int i = 0; i < 36; i++) {
            char c = value.charAt(i);
            if (i == 8 || i == 13 || i == 18 || i == 23) {
                if (c != '-') {
                    return false;
                }
            } else if (!isHex(c)) {
                return false;
            }
        }
        return true;
    }

    /** Trimmed (dash-less) UUID: exactly 32 hex digits. */
    public static boolean isTuuid(String value) {
        if (value.length() != 32) {
            return false;
        }
        for (int i = 0; i < 32; i++) {
            if (!isHex(value.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    /** ULID: 26 Crockford base-32 characters, the first in {@code 0-7} so it fits 128 bits. */
    public static boolean isUlid(String value) {
        if (value.length() != 26) {
            return false;
        }
        if (value.charAt(0) < '0' || value.charAt(0) > '7') {
            return false;
        }
        for (int i = 1; i < 26; i++) {
            if (!isCrockford(value.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    /** A fully-qualified protobuf name, e.g. {@code foo.bar.Baz} (no leading dot). */
    public static boolean isProtobufFqn(String value) {
        return isDottedIdentifiers(value, false);
    }

    /** The absolute (leading-dot) fully-qualified protobuf name, e.g. {@code .foo.bar.Baz}. */
    public static boolean isProtobufDotFqn(String value) {
        return isDottedIdentifiers(value, true);
    }

    /**
     * Dot-separated protobuf identifiers with no empty segments. Each segment is
     * {@code [A-Za-z_][A-Za-z0-9_]*}. When {@code leadingDot} is set the value must begin with a
     * dot (the absolute form) and carry at least one segment; otherwise no leading dot is allowed.
     */
    private static boolean isDottedIdentifiers(String value, boolean leadingDot) {
        int n = value.length();
        int i = 0;
        if (leadingDot) {
            if (n == 0 || value.charAt(0) != '.') {
                return false;
            }
            i = 1;
        }
        if (i >= n) {
            return false;
        }
        while (i < n) {
            if (!isIdentifierStart(value.charAt(i))) {
                return false;
            }
            i++;
            while (i < n && isIdentifierPart(value.charAt(i))) {
                i++;
            }
            if (i < n) {
                if (value.charAt(i) != '.') {
                    return false;
                }
                i++;
                if (i >= n) {
                    return false; // trailing dot
                }
            }
        }
        return true;
    }

    private static boolean isHex(char c) {
        return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F');
    }

    /** Crockford base-32: digits plus letters excluding I, L, O and U (case-insensitive). */
    private static boolean isCrockford(char c) {
        if (c >= '0' && c <= '9') {
            return true;
        }
        char u = c >= 'a' && c <= 'z' ? (char) (c - 32) : c;
        return u >= 'A' && u <= 'Z' && u != 'I' && u != 'L' && u != 'O' && u != 'U';
    }

    private static boolean isIdentifierStart(char c) {
        return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || c == '_';
    }

    private static boolean isIdentifierPart(char c) {
        return isIdentifierStart(c) || (c >= '0' && c <= '9');
    }
}
