package ai.protomolt.proto.formats;

/**
 * GS1 Global Trade Item Number validation: GTIN-8, GTIN-12 (UPC-A), GTIN-13 (EAN-13), and
 * GTIN-14, including the mod-10 check digit a bare digit-count pattern cannot verify. Purely
 * syntactic over the GS1 rules — no prefix-registry awareness — and implemented with direct
 * character scanning (no regular expressions).
 */
public final class TradeItemNumbers {

    private TradeItemNumbers() {
    }

    /**
     * A GTIN: exactly 8, 12, 13, or 14 digits whose final digit is the GS1 mod-10 check digit
     * (weights alternate 3 and 1 leftward from the digit beside the check digit).
     */
    public static boolean isGtin(String value) {
        int n = value.length();
        if (n != 8 && n != 12 && n != 13 && n != 14) {
            return false;
        }
        int sum = 0;
        int weight = 3;
        for (int i = n - 2; i >= 0; i--) {
            char c = value.charAt(i);
            if (c < '0' || c > '9') {
                return false;
            }
            sum += (c - '0') * weight;
            weight = 4 - weight;
        }
        char check = value.charAt(n - 1);
        if (check < '0' || check > '9') {
            return false;
        }
        return (10 - sum % 10) % 10 == check - '0';
    }
}
