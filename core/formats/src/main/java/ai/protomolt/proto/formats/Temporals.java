package ai.protomolt.proto.formats;

import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;

/**
 * Calendar-value formats, parsed with the JDK's own strict resolvers —
 * never a regex, so "2026-02-30" is refused as an impossible date, not
 * accepted as a plausible shape.
 */
final class Temporals {

    private static final DateTimeFormatter STRICT_DATE =
            DateTimeFormatter.ISO_LOCAL_DATE.withResolverStyle(ResolverStyle.STRICT);

    private Temporals() {
    }

    /** ISO-8601 calendar date ({@code 2026-07-01}), strictly resolved. */
    static boolean isDate(String value) {
        if (value.length() != 10) {
            return false;
        }
        try {
            LocalDate.parse(value, STRICT_DATE);
            return true;
        } catch (DateTimeParseException e) {
            return false;
        }
    }

    /** RFC 3339 date-time with an offset ({@code 2026-07-01T12:00:00Z}). */
    static boolean isDateTime(String value) {
        try {
            OffsetDateTime.parse(value, DateTimeFormatter.ISO_OFFSET_DATE_TIME);
            return true;
        } catch (DateTimeParseException e) {
            return false;
        }
    }
}
