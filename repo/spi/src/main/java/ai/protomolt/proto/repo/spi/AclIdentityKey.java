package ai.protomolt.proto.repo.spi;

/** Identity components folded independently using the repository policy's equalsIgnoreCase semantics. */
record AclIdentityKey(String type, String identity) {
    AclIdentityKey {
        type = fold(type);
        identity = fold(identity);
    }

    private static String fold(String value) {
        StringBuilder result = new StringBuilder(value.length());
        for (int i = 0; i < value.length();) {
            int cp = value.codePointAt(i);
            result.appendCodePoint(Character.toLowerCase(Character.toUpperCase(cp)));
            i += Character.charCount(cp);
        }
        return result.toString();
    }
}
