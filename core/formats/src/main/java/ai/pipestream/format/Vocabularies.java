package ai.pipestream.format;

import java.util.Currency;
import java.util.IllformedLocaleException;
import java.util.Locale;

/**
 * Vocabulary formats backed by data the JDK already ships (Unicode CLDR
 * underneath), so nothing is bundled and nothing needs attribution: the
 * language-tag check is the JDK's own strict BCP 47 parser, and the
 * currency check is the JDK's ISO 4217 table.
 */
final class Vocabularies {

    private Vocabularies() {
    }

    /** Well-formed BCP 47 language tag ({@code en}, {@code pt-BR}). */
    static boolean isLanguageTag(String value) {
        try {
            new Locale.Builder().setLanguageTag(value);
            return true;
        } catch (IllformedLocaleException e) {
            return false;
        }
    }

    /** ISO 4217 alphabetic currency code known to the JDK ({@code USD}). */
    static boolean isCurrencyCode(String value) {
        if (value.length() != 3) {
            return false;
        }
        try {
            Currency.getInstance(value);
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }
}
