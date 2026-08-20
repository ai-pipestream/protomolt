package ai.pipestream.proto.formats;

/**
 * IANA media types per the RFC 6838 grammar: {@code type "/" subtype}
 * with restricted-name tokens, optional {@code +suffix} handled by the
 * token grammar itself, parameters not accepted (a media type field
 * carries the type, not a Content-Type header).
 */
final class MediaTypes {

    private MediaTypes() {
    }

    /** RFC 6838 {@code type/subtype}. */
    static boolean isMediaType(String value) {
        int slash = value.indexOf('/');
        if (slash <= 0 || slash != value.lastIndexOf('/')) {
            return false;
        }
        return restrictedName(value, 0, slash)
                && restrictedName(value, slash + 1, value.length());
    }

    /**
     * RFC 6838 restricted-name: 1..127 chars, first alphanumeric, rest
     * alphanumeric or {@code ! # $ & - ^ _ . +}.
     */
    private static boolean restrictedName(String value, int from, int to) {
        int length = to - from;
        if (length < 1 || length > 127) {
            return false;
        }
        for (int i = from; i < to; i++) {
            char c = value.charAt(i);
            boolean alphanumeric = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
                    || (c >= '0' && c <= '9');
            if (i == from) {
                if (!alphanumeric) {
                    return false;
                }
                continue;
            }
            if (!alphanumeric && c != '!' && c != '#' && c != '$' && c != '&'
                    && c != '-' && c != '^' && c != '_' && c != '.' && c != '+') {
                return false;
            }
        }
        return true;
    }
}
