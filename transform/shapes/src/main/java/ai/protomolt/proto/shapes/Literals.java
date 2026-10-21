package ai.protomolt.proto.shapes;

/**
 * The literal syntax accepted on the source side of a text mapping rule: {@code null},
 * {@code true}, {@code false}, a double-quoted string, or a number — exactly what
 * {@code ProtoFieldMapperImpl} recognizes before it treats a source as a field path. Kept in
 * one place so the static {@link RuleChecker} and the runtime {@link ScopedProtoMapper} agree
 * on which sources never resolve against a message.
 */
final class Literals {

    private Literals() {
    }

    static boolean isLiteral(String source) {
        return source.equals("null") || source.equals("true") || source.equals("false")
                || (source.length() > 1 && source.startsWith("\"") && source.endsWith("\""))
                || isNumber(source);
    }

    private static boolean isNumber(String text) {
        int i = text.startsWith("-") ? 1 : 0;
        int start = i;
        while (i < text.length() && text.charAt(i) >= '0' && text.charAt(i) <= '9') i++;
        if (i == start) return false;
        if (i == text.length()) return true;
        if (text.charAt(i++) != '.') return false;
        start = i;
        while (i < text.length() && text.charAt(i) >= '0' && text.charAt(i) <= '9') i++;
        return i > start && i == text.length();
    }

    /**
     * The value of a literal, for a source {@link #isLiteral} accepts; {@code null} for the
     * {@code null} literal, which clears its target rather than assigning.
     */
    static Object valueOf(String source) {
        switch (source) {
            case "null" -> {
                return null;
            }
            case "true" -> {
                return Boolean.TRUE;
            }
            case "false" -> {
                return Boolean.FALSE;
            }
            default -> {
                // A quoted string or a number.
            }
        }
        if (source.startsWith("\"")) {
            return source.substring(1, source.length() - 1);
        }
        // Keep these branches separate. A conditional expression combining boxed
        // Double and Long values is subject to numeric unboxing/promotion and boxes
        // the integral branch back to Double (for example, 42 becomes 42.0).
        if (source.contains(".")) {
            return Double.valueOf(source);
        }
        return Long.valueOf(source);
    }
}
