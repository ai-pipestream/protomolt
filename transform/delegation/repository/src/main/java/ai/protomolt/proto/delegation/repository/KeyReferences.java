package ai.protomolt.proto.delegation.repository;

import ai.protomolt.proto.formats.Formats;

/** The existing bounded ASCII scheme and environment-variable grammars. */
final class KeyReferences {
    private KeyReferences() {}

    static boolean variable(String name) {
        return name.length() <= 128 && Formats.isIdentifier(name);
    }

    static boolean reference(String text) {
        int colon = text.indexOf(':');
        if (colon < 1 || colon > 32 || !letter(text.charAt(0))) return false;
        for (int i = 1; i < colon; i++) {
            char c = text.charAt(i);
            if (!letter(c) && !(c >= '0' && c <= '9') && c != '+' && c != '.' && c != '-') return false;
        }
        return variable(text.substring(colon + 1));
    }

    private static boolean letter(char c) {
        return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z');
    }
}
