package ai.pipestream.format;

import java.util.regex.Pattern;

/**
 * Validation of the common identifier string formats: UUID (RFC 4122 textual form), its trimmed
 * (dash-less) variant, and ULID (Crockford base-32). Purely syntactic.
 */
public final class Identifiers {

    private static final Pattern UUID = Pattern.compile(
            "[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}");
    private static final Pattern TUUID = Pattern.compile("[0-9a-fA-F]{32}");
    // 26 chars, Crockford base-32 excluding I, L, O, U; first char <= 7 so it fits 128 bits.
    private static final Pattern ULID = Pattern.compile("[0-7][0-9A-HJKMNP-TV-Za-hjkmnp-tv-z]{25}");

    private Identifiers() {
    }

    /** Canonical dashed UUID, e.g. {@code 123e4567-e89b-12d3-a456-426614174000}. */
    public static boolean isUuid(String value) {
        return UUID.matcher(value).matches();
    }

    /** Trimmed (dash-less) UUID: 32 hex digits. */
    public static boolean isTuuid(String value) {
        return TUUID.matcher(value).matches();
    }

    /** ULID: 26 Crockford base-32 characters. */
    public static boolean isUlid(String value) {
        return ULID.matcher(value).matches();
    }
}
