package ai.protomolt.proto.formats;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * The Tier-1 well-known formats: every check is a real parser, never a
 * regex — an impossible calendar date is refused, the JDK's own BCP 47
 * parser and ISO 4217 table answer the vocabulary questions with no
 * bundled data, and the E.164 envelope tolerates display formatting
 * without accepting garbage.
 */
class WellKnownFormatsTest {

    @Test
    void datesParseStrictlyNotShapedly() {
        assertThat(Formats.isDate("2026-07-01")).isTrue();
        assertThat(Formats.isDate("2024-02-29")).isTrue();
        // The shape a regex would accept; the calendar refuses.
        assertThat(Formats.isDate("2026-02-30")).isFalse();
        assertThat(Formats.isDate("2026-13-01")).isFalse();
        assertThat(Formats.isDate("2026-7-1")).isFalse();
        assertThat(Formats.isDate("2026-07-01T00:00:00Z")).isFalse();
        assertThat(Formats.isDate("")).isFalse();
    }

    @Test
    void dateTimesAreRfc3339WithAnOffset() {
        assertThat(Formats.isDateTime("2026-07-01T12:00:00Z")).isTrue();
        assertThat(Formats.isDateTime("2026-07-01T12:00:00.123+02:00")).isTrue();
        assertThat(Formats.isDateTime("2026-07-01T12:00:00")).isFalse();
        assertThat(Formats.isDateTime("2026-07-01")).isFalse();
    }

    @Test
    void mediaTypesFollowTheRfc6838Grammar() {
        assertThat(Formats.isMediaType("application/x-protobuf")).isTrue();
        assertThat(Formats.isMediaType("image/svg+xml")).isTrue();
        assertThat(Formats.isMediaType("text/plain")).isTrue();
        assertThat(Formats.isMediaType("text")).isFalse();
        assertThat(Formats.isMediaType("/plain")).isFalse();
        assertThat(Formats.isMediaType("text/")).isFalse();
        assertThat(Formats.isMediaType("text/pl ain")).isFalse();
        assertThat(Formats.isMediaType("text/plain; charset=utf-8")).isFalse();
        assertThat(Formats.isMediaType("a/b/c")).isFalse();
    }

    @Test
    void languageTagsUseTheJdksStrictBcp47Parser() {
        assertThat(Formats.isLanguageTag("en")).isTrue();
        assertThat(Formats.isLanguageTag("pt-BR")).isTrue();
        assertThat(Formats.isLanguageTag("zh-Hant-TW")).isTrue();
        assertThat(Formats.isLanguageTag("not a tag")).isFalse();
        assertThat(Formats.isLanguageTag("a")).isFalse();
        assertThat(Formats.isLanguageTag("en_US")).isFalse();
    }

    @Test
    void slugsAreLowercaseWithInteriorSingleSeparators() {
        assertThat(Formats.isSlug("orders")).isTrue();
        assertThat(Formats.isSlug("parse-routing")).isTrue();
        assertThat(Formats.isSlug("a.b_c-d")).isTrue();
        assertThat(Formats.isSlug("2fa")).isTrue();
        assertThat(Formats.isSlug("")).isFalse();
        assertThat(Formats.isSlug("Orders")).isFalse();
        assertThat(Formats.isSlug("-orders")).isFalse();
        assertThat(Formats.isSlug("orders-")).isFalse();
        assertThat(Formats.isSlug("a--b")).isFalse();
        assertThat(Formats.isSlug("a._b")).isFalse();
        assertThat(Formats.isSlug("a b")).isFalse();
        assertThat(Formats.isSlug("café")).isFalse();
    }

    @Test
    void regionCodesComeFromTheJdksIso3166Table() {
        assertThat(Formats.isRegionCode("US")).isTrue();
        assertThat(Formats.isRegionCode("BR")).isTrue();
        assertThat(Formats.isRegionCode("CH")).isTrue();
        assertThat(Formats.isRegionCode("us")).isFalse();
        assertThat(Formats.isRegionCode("XZ")).isFalse();
        assertThat(Formats.isRegionCode("USA")).isFalse();
        assertThat(Formats.isRegionCode("")).isFalse();
    }

    @Test
    void currencyCodesComeFromTheJdksIso4217Table() {
        assertThat(Formats.isCurrencyCode("USD")).isTrue();
        assertThat(Formats.isCurrencyCode("EUR")).isTrue();
        // Three uppercase letters is not enough: the table decides.
        assertThat(Formats.isCurrencyCode("ZZZ")).isFalse();
        assertThat(Formats.isCurrencyCode("usd")).isFalse();
        assertThat(Formats.isCurrencyCode("US")).isFalse();
    }

    @Test
    void phoneNumbersAreE164ShapedButDisplayTolerant() {
        assertThat(Formats.isPhoneNumber("+14155552671")).isTrue();
        assertThat(Formats.isPhoneNumber("+1 (415) 555-2671")).isTrue();
        assertThat(Formats.isPhoneNumber("415.555.2671")).isTrue();
        assertThat(Formats.isPhoneNumber("+0 415")).isFalse();
        assertThat(Formats.isPhoneNumber("+1234567890123456")).isFalse();
        assertThat(Formats.isPhoneNumber("1")).isFalse();
        assertThat(Formats.isPhoneNumber("call me")).isFalse();
        assertThat(Formats.isPhoneNumber("415+555")).isFalse();
    }

    @Test
    void digestsAreLowercaseHexOfTheirAlgorithmsLength() {
        String sha256 = "a".repeat(64);
        assertThat(Formats.isSha256Hex(sha256)).isTrue();
        assertThat(Formats.isSha256Hex("A".repeat(64))).isFalse();
        assertThat(Formats.isSha256Hex("a".repeat(63))).isFalse();
        assertThat(Formats.isSha1Hex("0123456789abcdef0123456789abcdef01234567")).isTrue();
        assertThat(Formats.isSha1Hex("a".repeat(64))).isFalse();
        assertThat(Formats.isHex("00ff")).isTrue();
        assertThat(Formats.isHex("00FF")).isFalse();
        assertThat(Formats.isHex("")).isFalse();
        assertThat(Formats.isHex("xyz")).isFalse();
    }

    @Test
    void base64DecodesOrRefuses() {
        assertThat(Formats.isBase64("aGVsbG8=")).isTrue();
        assertThat(Formats.isBase64("")).isTrue();
        assertThat(Formats.isBase64("not base64!!")).isFalse();
    }
}
