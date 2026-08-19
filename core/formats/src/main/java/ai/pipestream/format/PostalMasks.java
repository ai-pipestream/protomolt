package ai.pipestream.format;

/**
 * Postal-code mask matching, the UPU convention: in a mask, {@code N} matches
 * one decimal digit, {@code A} matches one ASCII uppercase letter, and every
 * other character matches itself literally. A mask is a fixed-length template
 * ({@code NNNNN-NNNN}, {@code AN NAA}), so matching is one linear scan with no
 * alternation and no backtracking — which is the point: per-country postal
 * grammar arrives as operator-loaded data, and data must never smuggle in a
 * regular expression.
 */
public final class PostalMasks {

    private PostalMasks() {
    }

    /** Whether {@code value} matches {@code mask}. An empty mask matches nothing. */
    public static boolean matches(String value, String mask) {
        int n = mask.length();
        if (n == 0 || value.length() != n) {
            return false;
        }
        for (int i = 0; i < n; i++) {
            char m = mask.charAt(i);
            char c = value.charAt(i);
            if (m == 'N') {
                if (c < '0' || c > '9') {
                    return false;
                }
            } else if (m == 'A') {
                if (c < 'A' || c > 'Z') {
                    return false;
                }
            } else if (c != m) {
                return false;
            }
        }
        return true;
    }
}
