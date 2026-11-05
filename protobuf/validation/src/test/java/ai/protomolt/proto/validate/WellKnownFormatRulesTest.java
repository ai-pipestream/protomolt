package ai.protomolt.proto.validate;

import static org.assertj.core.api.Assertions.assertThat;

import ai.protomolt.proto.validate.testdata.WellKnownDoc;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * The well-known format flags through the whole dialect: declared in
 * validate.v1, read by the rule source, enforced by real parsers with
 * stable rule ids — and ignore_if_zero turning "empty or valid" into a
 * declaration instead of a regex alternation.
 */
class WellKnownFormatRulesTest {

    private static final ProtoValidator VALIDATOR = ProtoValidator.create();

    private static List<String> ruleIds(WellKnownDoc doc) {
        return VALIDATOR.validate(doc).violations().stream()
                .map(ValidationResult.Violation::ruleId)
                .toList();
    }

    @Test
    void everyFormatFlagEnforcesItsParser() {
        WellKnownDoc valid = WellKnownDoc.newBuilder()
                .setDate("2026-07-01")
                .setDateTime("2026-07-01T12:00:00Z")
                .setMimeType("application/x-protobuf")
                .setLanguage("pt-BR")
                .setCurrency("USD")
                .setTelephone("+1 (415) 555-2671")
                .setDigest("a".repeat(64))
                .setLegacyDigest("b".repeat(40))
                .setEncoded("aGVsbG8=")
                .setAmount("19.99")
                .build();
        assertThat(VALIDATOR.validate(valid).valid()).isTrue();

        WellKnownDoc invalid = WellKnownDoc.newBuilder()
                .setDate("2026-02-30")
                .setDateTime("2026-07-01")
                .setMimeType("not a type")
                .setLanguage("not a tag")
                .setCurrency("ZZZ")
                .setTelephone("call me")
                .setDigest("A".repeat(64))
                .setLegacyDigest("b".repeat(41))
                .setEncoded("!!")
                .setAmount("1e3")
                .build();
        assertThat(ruleIds(invalid)).containsExactlyInAnyOrder(
                "string.date", "string.date_time", "string.mime_type",
                "string.language_tag", "string.currency_code", "string.phone_number",
                "string.sha256_hex", "string.sha1_hex", "string.base64",
                "string.decimal");
    }

    @Test
    void decimalIsUnsignedAndExponentFree() {
        assertThat(ruleIds(WellKnownDoc.newBuilder().setAmount("42").build()))
                .doesNotContain("string.decimal", "string.decimal_empty");
        // Boundary: longer than any binary numeric type, still a decimal string.
        assertThat(ruleIds(WellKnownDoc.newBuilder()
                        .setAmount("12345678901234567890.5").build()))
                .doesNotContain("string.decimal", "string.decimal_empty");
        for (String bad : List.of("-5", "+5", "1e3", "19.99.", "12.34.56", " 42")) {
            assertThat(ruleIds(WellKnownDoc.newBuilder().setAmount(bad).build()))
                    .as("decimal must refuse %s", bad)
                    .contains("string.decimal");
        }
        assertThat(ruleIds(WellKnownDoc.getDefaultInstance()))
                .contains("string.decimal_empty");
    }

    @Test
    void decimalClassificationProducesOrdinaryViolationsWithoutCompilationOrEvaluationExceptions() {
        // StringFormat.DECIMAL is backed by the deterministic Decimals character scanner and has
        // no compilation or runtime evaluation exception path: malformed inputs always classify
        // as ordinary ValidationResult violations rather than throwing RuleCompilationException
        // or RuleEvaluationException.
        for (String input : List.of("", "-5", "1e3", "not-a-decimal", "\u0000")) {
            ValidationResult result = VALIDATOR.validate(
                    WellKnownDoc.newBuilder().setAmount(input).build());
            assertThat(result.valid()).isFalse();
            assertThat(result.violations())
                    .extracting(ValidationResult.Violation::ruleId)
                    .contains(input.isEmpty() ? "string.decimal_empty" : "string.decimal");
        }
    }

    @Test
    void ignoreIfZeroIsTheDeclaredFormOfEmptyOrValid() {
        // Empty everywhere: the ignore_if_zero fields pass, the plain
        // format fields report their _empty companions.
        WellKnownDoc empty = WellKnownDoc.getDefaultInstance();
        assertThat(ruleIds(empty))
                .doesNotContain("string.uuid", "string.uuid_empty",
                        "string.hex", "string.hex_empty");

        WellKnownDoc set = WellKnownDoc.newBuilder()
                .setOptionalUuid("not-a-uuid")
                .setTraceId("zzz")
                .build();
        assertThat(ruleIds(set)).contains("string.uuid", "string.hex", "string.len");

        WellKnownDoc good = WellKnownDoc.newBuilder()
                .setOptionalUuid("123e4567-e89b-12d3-a456-426614174000")
                .setTraceId("0af7651916cd43dd8448eb211c80319c")
                .build();
        assertThat(ruleIds(good))
                .doesNotContain("string.uuid", "string.hex", "string.len");
    }
}
