package ai.protomolt.proto.validate;

import ai.protomolt.proto.validate.testdata.CelShapeGauntlet;
import ai.protomolt.proto.validate.testdata.EnumCelGauntlet;
import ai.protomolt.proto.validate.testdata.Mood;
import ai.protomolt.proto.validate.testdata.NowGauntlet;
import ai.protomolt.proto.validate.testdata.UintCelGauntlet;
import ai.protomolt.proto.validate.testdata.WrapperGauntlet;
import com.google.protobuf.Message;
import com.google.protobuf.StringValue;
import com.google.protobuf.Timestamp;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Field-level CEL rules through the dialect: string-returning rules carry their own failure text,
 * blank ids fall back to the expression, rule paths index as {@code cel[N]}, and the scalar
 * conversion feeds CEL the right types (uint, enum number, unwrapped wrapper, timestamp vs now).
 */
class CelFieldRulesTest {

    private static final ProtoValidator VALIDATOR = ProtoValidator.create();

    private static void assertValid(Message message) {
        assertThat(VALIDATOR.validate(message).valid())
                .as("expected no violations, got %s", VALIDATOR.validate(message).violations())
                .isTrue();
    }

    @Test
    void stringResultCarriesTheFailureText() {
        // The rule returns '' when fine and 'too short' otherwise; blank id falls back to the
        // expression text, and as the field's first cel rule its rule path is cel[0].
        assertThat(VALIDATOR.validate(CelShapeGauntlet.newBuilder().setWord("x").build())
                        .violations())
                .singleElement()
                .satisfies(v -> {
                    assertThat(v.path()).isEqualTo("word");
                    assertThat(v.ruleId()).isEqualTo("this.size() >= 2 ? '' : 'too short'");
                    assertThat(v.message()).isEqualTo("too short");
                    assertThat(v.rulePath()).isEqualTo("cel[0]");
                });
    }

    @Test
    void booleanResultUsesTheDeclaredIdAndMessage() {
        // Second cel rule on the field: rule path cel[1].
        assertThat(VALIDATOR.validate(CelShapeGauntlet.newBuilder().setWord("a b").build())
                        .violations())
                .singleElement()
                .satisfies(v -> {
                    assertThat(v.ruleId()).isEqualTo("word.no_space");
                    assertThat(v.message()).isEqualTo("no spaces allowed");
                    assertThat(v.rulePath()).isEqualTo("cel[1]");
                });
    }

    @Test
    void emptyStringResultIsNotAViolation() {
        assertValid(CelShapeGauntlet.newBuilder().setWord("ab").build());
        assertValid(CelShapeGauntlet.getDefaultInstance());
    }

    @Test
    void formatFunctionRendersTheFailureMessage() {
        assertThat(VALIDATOR.validate(CelShapeGauntlet.newBuilder().setWidgets(11).build())
                        .violations())
                .singleElement()
                .satisfies(v -> {
                    assertThat(v.ruleId()).isEqualTo("widgets.count");
                    assertThat(v.message()).isEqualTo("11 widgets is too many");
                });
        assertValid(CelShapeGauntlet.newBuilder().setWidgets(10).build());
    }

    @Test
    void uintFieldReachesCelAsUnsigned() {
        assertValid(UintCelGauntlet.newBuilder().setPort(8080).build());
        // -1 is 4294967295 as uint32 — comfortably >= 1024u, proving the unsigned conversion.
        assertValid(UintCelGauntlet.newBuilder().setPort(-1).build());
        assertThat(VALIDATOR.validate(UintCelGauntlet.newBuilder().setPort(80).build()).violations())
                .anyMatch(v -> v.path().equals("port") && v.ruleId().equals("port.high"));
        assertValid(UintCelGauntlet.getDefaultInstance());
    }

    @Test
    void enumFieldReachesCelAsItsNumber() {
        assertValid(EnumCelGauntlet.newBuilder().setShade(Mood.MOOD_GOOD).build());
        // An explicitly set zero enum value is present, so the rule runs and fails.
        assertThat(VALIDATOR.validate(EnumCelGauntlet.newBuilder()
                        .setShade(Mood.MOOD_UNSPECIFIED).build()).violations())
                .anyMatch(v -> v.path().equals("shade") && v.ruleId().equals("shade.set"));
        assertValid(EnumCelGauntlet.getDefaultInstance());
    }

    @Test
    void timestampFieldComparesAgainstNow() {
        assertValid(NowGauntlet.newBuilder()
                .setDeadline(Timestamp.newBuilder()
                        .setSeconds(Instant.now().plusSeconds(3600).getEpochSecond()))
                .build());
        assertThat(VALIDATOR.validate(NowGauntlet.newBuilder()
                        .setDeadline(Timestamp.getDefaultInstance()).build()).violations())
                .anyMatch(v -> v.path().equals("deadline") && v.ruleId().equals("deadline.future"));
        assertValid(NowGauntlet.getDefaultInstance());
    }

    @Test
    void wrapperFieldBindsTheUnwrappedScalar() {
        assertValid(WrapperGauntlet.newBuilder()
                .setTagged(StringValue.of("xy")).build());
        assertThat(VALIDATOR.validate(WrapperGauntlet.newBuilder()
                        .setTagged(StringValue.of("x")).build()).violations())
                .anyMatch(v -> v.path().equals("tagged") && v.ruleId().equals("wrapper.tagged"));
    }
}
