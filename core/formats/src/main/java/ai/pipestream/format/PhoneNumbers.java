package ai.pipestream.format;

/**
 * Telephone numbers, E.164-shaped but display-tolerant: spaces, dots,
 * hyphens, and parentheses are accepted as formatting, and what remains
 * must be an optional {@code +} followed by 2 to 15 digits not starting
 * with zero — the E.164 envelope. Purely structural: no carrier or
 * region knowledge, no libphonenumber.
 */
final class PhoneNumbers {

    private PhoneNumbers() {
    }

    /** Display-tolerant E.164 telephone number. */
    static boolean isPhoneNumber(String value) {
        int digits = 0;
        boolean plusSeen = false;
        boolean firstDigitZero = false;
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c == '+') {
                if (plusSeen || digits > 0) {
                    return false;
                }
                plusSeen = true;
            } else if (c >= '0' && c <= '9') {
                if (digits == 0) {
                    firstDigitZero = c == '0';
                }
                digits++;
            } else if (c != ' ' && c != '-' && c != '.' && c != '(' && c != ')') {
                return false;
            }
        }
        return digits >= 2 && digits <= 15 && !firstDigitZero;
    }
}
