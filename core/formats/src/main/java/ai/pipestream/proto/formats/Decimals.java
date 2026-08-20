package ai.pipestream.proto.formats;

/**
 * Unsigned decimal string validation: one or more digits with an optional single fractional
 * part. This is the money-in-a-string shape (schema.org price) — strings because binary
 * floating point cannot carry prices exactly. Deliberately unsigned and exponent-free: a sign
 * or scientific notation would loosen every converted site. Implemented with direct character
 * scanning (no regular expressions).
 */
public final class Decimals {

    private Decimals() {
    }

    /** An unsigned decimal: {@code digits} or {@code digits.digits}, nothing else. */
    public static boolean isDecimal(String value) {
        int n = value.length();
        if (n == 0) {
            return false;
        }
        int dot = -1;
        for (int i = 0; i < n; i++) {
            char c = value.charAt(i);
            if (c == '.') {
                if (dot >= 0) {
                    return false;
                }
                dot = i;
            } else if (c < '0' || c > '9') {
                return false;
            }
        }
        // At least one digit on each side of the dot when one is present.
        return dot != 0 && dot != n - 1;
    }
}
