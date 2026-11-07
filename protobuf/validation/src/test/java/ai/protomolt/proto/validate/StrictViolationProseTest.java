package ai.protomolt.proto.validate;

import static org.assertj.core.api.Assertions.assertThat;

import ai.protomolt.proto.validate.testdata.Person;
import ai.protomolt.proto.validate.testdata.WellKnownDoc;
import java.time.Duration;
import org.junit.jupiter.api.Test;

/**
 * Regression coverage for upstream {@code --strict_message} violation prose and formatting helpers.
 */
class StrictViolationProseTest {

    private static final ProtoValidator VALIDATOR = ProtoValidator.create();

    @Test
    void fmtDoubleOmitsDecimalPointOnIntegralBoundsAndPreservesNonIntegral() {
        assertThat(ValueChecks.fmtDouble(0.0)).isEqualTo("0");
        assertThat(ValueChecks.fmtDouble(-0.0)).isEqualTo("0");
        assertThat(ValueChecks.fmtDouble(123.0)).isEqualTo("123");
        assertThat(ValueChecks.fmtDouble(-42.0)).isEqualTo("-42");
        assertThat(ValueChecks.fmtDouble(1.5)).isEqualTo("1.5");
        assertThat(ValueChecks.fmtDouble(Double.NaN)).isEqualTo("NaN");
        assertThat(ValueChecks.fmtDouble(Double.POSITIVE_INFINITY)).isEqualTo("Infinity");
    }

    @Test
    void fmtDurationUsesCanonicalSecondsFormatting() {
        assertThat(ValueChecks.fmtDuration(Duration.ZERO)).isEqualTo("0s");
        assertThat(ValueChecks.fmtDuration(Duration.ofSeconds(3))).isEqualTo("3s");
        assertThat(ValueChecks.fmtDuration(Duration.ofMillis(1))).isEqualTo("0.001s");
        assertThat(ValueChecks.fmtDuration(Duration.ofMillis(-250))).isEqualTo("-0.25s");
        assertThat(ValueChecks.fmtDuration(Duration.ofSeconds(-1, -500_000_000))).isEqualTo("-1.5s");
    }

    @Test
    void validatorEmitsStrictAlignedProseForRepresentativeRules() {
        WellKnownDoc badDoc = WellKnownDoc.newBuilder()
                .setOptionalUuid("not-a-uuid")
                .setTraceId("zzz")
                .build();
        var docViolations = VALIDATOR.validate(badDoc).violations();
        assertThat(docViolations)
                .filteredOn(v -> "string.uuid".equals(v.ruleId()))
                .extracting(ValidationResult.Violation::message)
                .containsExactly("must be a valid UUID");
        assertThat(docViolations)
                .filteredOn(v -> "string.len".equals(v.ruleId()))
                .extracting(ValidationResult.Violation::message)
                .containsExactly("must be 32 characters");

        Person shortName = Person.newBuilder()
                .setName("a")
                .build();
        var personViolations = VALIDATOR.validate(shortName).violations();
        assertThat(personViolations)
                .filteredOn(v -> "string.min_len".equals(v.ruleId()))
                .extracting(ValidationResult.Violation::message)
                .containsExactly("must be at least 2 characters");
    }
}
