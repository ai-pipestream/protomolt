package ai.pipestream.proto.validate;

import ai.pipestream.proto.validate.testdata.NumberGauntlet;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class NumericRulesTest {

    private static final ProtoValidator VALIDATOR = ProtoValidator.create();

    private static void assertViolation(Message message, String path, String ruleId) {
        assertThat(VALIDATOR.validate(message).violations())
                .as("expected %s at %s", ruleId, path)
                .anyMatch(v -> v.path().equals(path) && v.ruleId().equals(ruleId));
    }

    private static void assertValid(Message message) {
        assertThat(VALIDATOR.validate(message).valid())
                .as("expected no violations, got %s", VALIDATOR.validate(message).violations())
                .isTrue();
    }

    @Test
    void uint32BoundsAreUnsigned() {
        assertValid(NumberGauntlet.newBuilder().setSmall(10).build());
        assertViolation(NumberGauntlet.newBuilder().setSmall(11).build(),
                "small", "uint32.lte");
        // -1 is 4294967295 as uint32 and must fail lte: 10, not pass as negative.
        assertViolation(NumberGauntlet.newBuilder().setSmall(-1).build(),
                "small", "uint32.lte");
    }

    @Test
    void uint64BoundsAreUnsigned() {
        assertValid(NumberGauntlet.newBuilder().setBig(6).build());
        assertViolation(NumberGauntlet.newBuilder().setBig(3).build(),
                "big", "uint64.gt");
        // -1 is 18446744073709551615 as uint64, comfortably > 5.
        assertValid(NumberGauntlet.newBuilder().setBig(-1L).build());
    }

    @Test
    void int32Const() {
        assertValid(NumberGauntlet.newBuilder().setExact(7).build());
        assertViolation(NumberGauntlet.newBuilder().setExact(8).build(),
                "exact", "int32.const");
    }

    @Test
    void int64In() {
        assertValid(NumberGauntlet.newBuilder().setChoice(2).build());
        assertViolation(NumberGauntlet.newBuilder().setChoice(9).build(),
                "choice", "int64.in");
    }

    @Test
    void int32NotIn() {
        assertValid(NumberGauntlet.newBuilder().setVeto(12).build());
        assertViolation(NumberGauntlet.newBuilder().setVeto(13).build(),
                "veto", "int32.not_in");
    }

    @Test
    void doubleFinite() {
        assertValid(NumberGauntlet.newBuilder().setFinite(1.5).build());
        assertViolation(NumberGauntlet.newBuilder().setFinite(Double.NaN).build(),
                "finite", "double.finite");
        assertViolation(NumberGauntlet.newBuilder().setFinite(Double.POSITIVE_INFINITY).build(),
                "finite", "double.finite");
    }

    @Test
    void floatRange() {
        assertValid(NumberGauntlet.newBuilder().setRanged(1.0f).build());
        assertViolation(NumberGauntlet.newBuilder().setRanged(0.25f).build(),
                "ranged", "float.gte");
        assertViolation(NumberGauntlet.newBuilder().setRanged(3.0f).build(),
                "ranged", "float.lt");
    }

    @Test
    void sint32UsesInt32Rules() {
        assertValid(NumberGauntlet.newBuilder().setNegative(-5).build());
        assertViolation(NumberGauntlet.newBuilder().setNegative(4).build(),
                "negative", "int32.lt");
    }

    @Test
    void fixed64UsesUint64Rules() {
        assertValid(NumberGauntlet.newBuilder().setFixed(50).build());
        assertViolation(NumberGauntlet.newBuilder().setFixed(200).build(),
                "fixed", "uint64.lte");
    }

    @Test
    void unsetNumericFieldsAreSkipped() {
        assertValid(NumberGauntlet.getDefaultInstance());
    }
}
