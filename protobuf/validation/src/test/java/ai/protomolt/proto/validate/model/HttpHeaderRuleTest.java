package ai.protomolt.proto.validate.model;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The RFC 7230 header rules as direct character scans: strict names are token characters with an
 * optional leading colon, loose forms reject only NUL/LF/CR, strict values reject the control
 * range but permit tab and obs-text. Empty handling differs between names (rejected) and values
 * (permitted) and is reported by the validator under the {@code _empty} companion ids.
 */
class HttpHeaderRuleTest {

    @Test
    void ruleIdsMatchProtovalidate() {
        assertThat(HttpHeaderRule.NAME_STRICT.ruleId()).isEqualTo("string.well_known_regex.header_name");
        assertThat(HttpHeaderRule.NAME_LOOSE.ruleId()).isEqualTo("string.well_known_regex.header_name");
        assertThat(HttpHeaderRule.VALUE_STRICT.ruleId()).isEqualTo("string.well_known_regex.header_value");
        assertThat(HttpHeaderRule.VALUE_LOOSE.ruleId()).isEqualTo("string.well_known_regex.header_value");
        assertThat(HttpHeaderRule.NAME_STRICT.emptyRuleId())
                .isEqualTo("string.well_known_regex.header_name_empty");
        assertThat(HttpHeaderRule.VALUE_STRICT.emptyRuleId())
                .isEqualTo("string.well_known_regex.header_value_empty");
    }

    @Test
    void onlyNamesRejectEmpty() {
        assertThat(HttpHeaderRule.NAME_STRICT.rejectEmpty()).isTrue();
        assertThat(HttpHeaderRule.NAME_LOOSE.rejectEmpty()).isTrue();
        assertThat(HttpHeaderRule.VALUE_STRICT.rejectEmpty()).isFalse();
        assertThat(HttpHeaderRule.VALUE_LOOSE.rejectEmpty()).isFalse();
    }

    @Test
    void strictNameTokenCharacters() {
        assertThat(HttpHeaderRule.NAME_STRICT.matches("X-Custom-Header")).isTrue();
        // Every specials character from protovalidate's class, including the backtick.
        assertThat(HttpHeaderRule.NAME_STRICT.matches("!#$%&'*+-.^_|~`")).isTrue();
        // protovalidate 63347b8 moved the hyphen to the end of the class, so "+-." is no
        // longer a range and the comma it used to admit is refused.
        assertThat(HttpHeaderRule.NAME_STRICT.matches("a,b")).isFalse();
        assertThat(HttpHeaderRule.NAME_STRICT.matches("foo,bar")).isFalse();
        assertThat(HttpHeaderRule.NAME_STRICT.matches(
                "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ!#$%&'*+-.^_|~`"))
                .isTrue();
        assertThat(HttpHeaderRule.NAME_STRICT.matches("has space")).isFalse();
        assertThat(HttpHeaderRule.NAME_STRICT.matches("trail:")).isFalse();
        assertThat(HttpHeaderRule.NAME_STRICT.matches("")).isFalse();
    }

    @Test
    void strictNamePermitsOneLeadingColon() {
        // Pseudo-header form: an optional single leading colon, then a non-empty token run.
        assertThat(HttpHeaderRule.NAME_STRICT.matches(":authority")).isTrue();
        assertThat(HttpHeaderRule.NAME_STRICT.matches(":")).isFalse();
        assertThat(HttpHeaderRule.NAME_STRICT.matches("::double")).isFalse();
    }

    @Test
    void looseNameRejectsOnlyNulLfCr() {
        assertThat(HttpHeaderRule.NAME_LOOSE.matches("any thing: goes")).isTrue();
        assertThat(HttpHeaderRule.NAME_LOOSE.matches("")).isFalse();
        assertThat(HttpHeaderRule.NAME_LOOSE.matches("a\0b")).isFalse();
        assertThat(HttpHeaderRule.NAME_LOOSE.matches("a\nb")).isFalse();
        assertThat(HttpHeaderRule.NAME_LOOSE.matches("a\rb")).isFalse();
    }

    @Test
    void strictValuePermitsTabAndObsText() {
        assertThat(HttpHeaderRule.VALUE_STRICT.matches("")).isTrue();
        assertThat(HttpHeaderRule.VALUE_STRICT.matches("a\tb")).isTrue();
        // obs-text (>= 0x80) is allowed by the protovalidate character class.
        assertThat(HttpHeaderRule.VALUE_STRICT.matches("caf\u00E9")).isTrue();
    }

    @Test
    void strictValueRejectsControlCharacters() {
        assertThat(HttpHeaderRule.VALUE_STRICT.matches("a\0b")).isFalse();
        assertThat(HttpHeaderRule.VALUE_STRICT.matches("a\bb")).isFalse();
        assertThat(HttpHeaderRule.VALUE_STRICT.matches("a\nb")).isFalse();
        assertThat(HttpHeaderRule.VALUE_STRICT.matches("a\rb")).isFalse();
        assertThat(HttpHeaderRule.VALUE_STRICT.matches("a\u001Fb")).isFalse();
        assertThat(HttpHeaderRule.VALUE_STRICT.matches("a\u007Fb")).isFalse();
    }

    @Test
    void looseValueRejectsOnlyNulLfCr() {
        assertThat(HttpHeaderRule.VALUE_LOOSE.matches("")).isTrue();
        assertThat(HttpHeaderRule.VALUE_LOOSE.matches("a\u0001b")).isTrue();
        assertThat(HttpHeaderRule.VALUE_LOOSE.matches("a\0b")).isFalse();
        assertThat(HttpHeaderRule.VALUE_LOOSE.matches("a\nb")).isFalse();
        assertThat(HttpHeaderRule.VALUE_LOOSE.matches("a\rb")).isFalse();
    }
}
