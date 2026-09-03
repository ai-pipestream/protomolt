package ai.protomolt.proto.formats;

/**
 * Encoded-payload formats: lowercase hex digests (the repository's
 * digest convention) and standard base64. Structural checks only —
 * a digest's length says which algorithm produced it, its content is
 * never recomputed here.
 */
final class Encodings {

    private Encodings() {
    }

    /** Lowercase hexadecimal of any positive length. */
    static boolean isHex(String value) {
        if (value.isEmpty()) {
            return false;
        }
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if ((c < '0' || c > '9') && (c < 'a' || c > 'f')) {
                return false;
            }
        }
        return true;
    }

    /** Lowercase hex SHA-256 digest (64 chars). */
    static boolean isSha256Hex(String value) {
        return value.length() == 64 && isHex(value);
    }

    /** Lowercase hex SHA-1 digest (40 chars). */
    static boolean isSha1Hex(String value) {
        return value.length() == 40 && isHex(value);
    }

    /** RFC 4648 base64 (standard alphabet, correct padding). */
    static boolean isBase64(String value) {
        try {
            java.util.Base64.getDecoder().decode(value);
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }
}
