package ai.protomolt.proto.asset.bridge;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A small strict JSON reader. The asset family holds itself to the JDK, and
 * two bridges need to read JSON: NDJSON schema inference walks each line's
 * object, and an Avro container's schema arrives as a JSON string in the
 * file header.
 *
 * <p>Strict on purpose. A reader that repairs malformed input would let a
 * bridge report a schema for a file the platform cannot actually read, so
 * anything outside RFC 8259 raises {@link IOException} naming the offset.
 *
 * <p>Values decode to plain JDK types: {@code Map<String, Object>} in source
 * order, {@code List<Object>}, {@code String}, {@code Long} for integral
 * numbers, {@code Double} for the rest, {@code Boolean}, and null.
 */
final class Json {

    private final String text;
    private int at;

    private Json(String text) {
        this.text = text;
    }

    /**
     * Reads one complete JSON value.
     *
     * @param text the document
     * @return the decoded value
     * @throws IOException when the text is not one well-formed JSON value
     */
    static Object read(String text) throws IOException {
        Json reader = new Json(text);
        reader.skipSpace();
        Object value = reader.value();
        reader.skipSpace();
        if (reader.at != text.length()) {
            throw reader.fail("trailing content after the JSON value");
        }
        return value;
    }

    private Object value() throws IOException {
        if (at >= text.length()) {
            throw fail("a JSON value was expected");
        }
        char c = text.charAt(at);
        return switch (c) {
            case '{' -> object();
            case '[' -> array();
            case '"' -> string();
            case 't' -> literal("true", Boolean.TRUE);
            case 'f' -> literal("false", Boolean.FALSE);
            case 'n' -> literal("null", null);
            default -> number();
        };
    }

    private Map<String, Object> object() throws IOException {
        Map<String, Object> members = new LinkedHashMap<>();
        at++;
        skipSpace();
        if (peek() == '}') {
            at++;
            return members;
        }
        while (true) {
            skipSpace();
            if (peek() != '"') {
                throw fail("an object member name was expected");
            }
            String name = string();
            skipSpace();
            if (peek() != ':') {
                throw fail("':' was expected after an object member name");
            }
            at++;
            skipSpace();
            members.put(name, value());
            skipSpace();
            char next = peek();
            at++;
            if (next == '}') {
                return members;
            }
            if (next != ',') {
                throw fail("',' or '}' was expected in an object");
            }
        }
    }

    private List<Object> array() throws IOException {
        List<Object> items = new ArrayList<>();
        at++;
        skipSpace();
        if (peek() == ']') {
            at++;
            return items;
        }
        while (true) {
            skipSpace();
            items.add(value());
            skipSpace();
            char next = peek();
            at++;
            if (next == ']') {
                return items;
            }
            if (next != ',') {
                throw fail("',' or ']' was expected in an array");
            }
        }
    }

    private String string() throws IOException {
        at++;
        StringBuilder out = new StringBuilder();
        while (true) {
            if (at >= text.length()) {
                throw fail("an unterminated string");
            }
            char c = text.charAt(at++);
            if (c == '"') {
                return out.toString();
            }
            if (c != '\\') {
                if (c < 0x20) {
                    throw fail("a raw control character in a string");
                }
                out.append(c);
                continue;
            }
            if (at >= text.length()) {
                throw fail("an unterminated escape");
            }
            char escape = text.charAt(at++);
            switch (escape) {
                case '"', '\\', '/' -> out.append(escape);
                case 'b' -> out.append('\b');
                case 'f' -> out.append('\f');
                case 'n' -> out.append('\n');
                case 'r' -> out.append('\r');
                case 't' -> out.append('\t');
                case 'u' -> {
                    if (at + 4 > text.length()) {
                        throw fail("a truncated \\u escape");
                    }
                    out.append((char) Integer.parseInt(text, at, at + 4, 16));
                    at += 4;
                }
                default -> throw fail("an unknown escape '\\" + escape + "'");
            }
        }
    }

    private Object literal(String word, Object result) throws IOException {
        if (!text.startsWith(word, at)) {
            throw fail("'" + word + "' was expected");
        }
        at += word.length();
        return result;
    }

    private Object number() throws IOException {
        int start = at;
        if (peek() == '-') {
            at++;
        }
        boolean fractional = false;
        while (at < text.length()) {
            char c = text.charAt(at);
            if (c >= '0' && c <= '9') {
                at++;
            } else if (c == '.' || c == 'e' || c == 'E' || c == '+' || c == '-') {
                fractional = true;
                at++;
            } else {
                break;
            }
        }
        String token = text.substring(start, at);
        if (token.isEmpty() || token.equals("-")) {
            throw fail("a number was expected");
        }
        try {
            return fractional ? (Object) Double.valueOf(token) : (Object) Long.valueOf(token);
        } catch (NumberFormatException e) {
            throw fail("'" + token + "' is not a JSON number");
        }
    }

    private char peek() throws IOException {
        if (at >= text.length()) {
            throw fail("the document ended early");
        }
        return text.charAt(at);
    }

    private void skipSpace() {
        while (at < text.length()) {
            char c = text.charAt(at);
            if (c == ' ' || c == '\t' || c == '\n' || c == '\r') {
                at++;
            } else {
                return;
            }
        }
    }

    private IOException fail(String what) {
        return new IOException("malformed JSON at offset " + at + ": " + what);
    }
}
