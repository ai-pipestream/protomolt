package ai.protomolt.proto.asset.bridge;

import ai.protomolt.proto.asset.v1.FieldKind;

import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * How a column's kind is concluded from the values a bridge saw. Two rules
 * hold everywhere:
 *
 * <ul>
 *   <li><b>Widening only.</b> Kinds merge upward — integers among doubles
 *   read as double, and anything mixed reads as string. A column is never
 *   narrowed by a later row, so the conclusion does not depend on row
 *   order.</li>
 *   <li><b>Inference is bounded and says so.</b> A schema concluded from a
 *   prefix of the rows reports how many it inspected, so a consumer can
 *   tell a schema the format declared from one a bridge guessed at.</li>
 * </ul>
 */
final class FieldKinds {

    /** Text a delimited table uses for a boolean, lowercased. */
    private static final List<String> BOOLEANS = List.of("true", "false");

    private FieldKinds() {
    }

    /**
     * The kind of one text value from a delimited table, where every value
     * arrives as text and only its spelling can distinguish it.
     *
     * @param value the field's text
     * @return the narrowest kind the text supports
     */
    static FieldKind ofText(String value) {
        String trimmed = value.trim();
        if (trimmed.isEmpty()) {
            return FieldKind.FIELD_KIND_UNSPECIFIED;
        }
        if (BOOLEANS.contains(trimmed.toLowerCase(Locale.ROOT))) {
            return FieldKind.FIELD_KIND_BOOLEAN;
        }
        try {
            Long.parseLong(trimmed);
            return FieldKind.FIELD_KIND_INTEGER;
        } catch (NumberFormatException ignored) {
            // Not integral; try the wider numeric spelling below.
        }
        if (isDouble(trimmed)) {
            return FieldKind.FIELD_KIND_DOUBLE;
        }
        if (isTimestamp(trimmed)) {
            return FieldKind.FIELD_KIND_TIMESTAMP;
        }
        return FieldKind.FIELD_KIND_STRING;
    }

    /**
     * The kind of one decoded JSON value, where the document already states
     * the type.
     *
     * @param value a value from {@link Json}
     * @return the kind, or UNSPECIFIED for JSON null
     */
    static FieldKind ofJson(Object value) {
        if (value == null) {
            return FieldKind.FIELD_KIND_UNSPECIFIED;
        }
        if (value instanceof Map<?, ?>) {
            return FieldKind.FIELD_KIND_RECORD;
        }
        if (value instanceof List<?>) {
            return FieldKind.FIELD_KIND_LIST;
        }
        if (value instanceof Boolean) {
            return FieldKind.FIELD_KIND_BOOLEAN;
        }
        if (value instanceof Long) {
            return FieldKind.FIELD_KIND_INTEGER;
        }
        if (value instanceof Double) {
            return FieldKind.FIELD_KIND_DOUBLE;
        }
        return FieldKind.FIELD_KIND_STRING;
    }

    /**
     * Merges what two rows concluded about one column. UNSPECIFIED is the
     * absence of evidence and yields to anything; integers widen to double;
     * any other disagreement is string, because that is the only kind every
     * value spells.
     *
     * @param left the kind so far
     * @param right the kind this row supports
     * @return the merged kind
     */
    static FieldKind merge(FieldKind left, FieldKind right) {
        if (left == FieldKind.FIELD_KIND_UNSPECIFIED) {
            return right;
        }
        if (right == FieldKind.FIELD_KIND_UNSPECIFIED || left == right) {
            return left;
        }
        boolean numeric = (left == FieldKind.FIELD_KIND_INTEGER
                || left == FieldKind.FIELD_KIND_DOUBLE)
                && (right == FieldKind.FIELD_KIND_INTEGER
                || right == FieldKind.FIELD_KIND_DOUBLE);
        return numeric ? FieldKind.FIELD_KIND_DOUBLE : FieldKind.FIELD_KIND_STRING;
    }

    private static boolean isDouble(String text) {
        try {
            Double.parseDouble(text);
            // Java accepts hex floats and a trailing d/f that no data format
            // means as a number; require plain decimal spelling.
            return text.chars().allMatch(c -> Character.isDigit(c)
                    || c == '.' || c == '-' || c == '+' || c == 'e' || c == 'E');
        } catch (NumberFormatException e) {
            return false;
        }
    }

    private static boolean isTimestamp(String text) {
        try {
            OffsetDateTime.parse(text);
            return true;
        } catch (DateTimeParseException ignored) {
            // Not an offset instant; try a plain date.
        }
        try {
            LocalDate.parse(text);
            return true;
        } catch (DateTimeParseException e) {
            return false;
        }
    }
}
