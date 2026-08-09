package ai.pipestream.proto.validate;

import ai.pipestream.proto.validate.testdata.UniqueGauntlet;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code repeated.unique} duplicate detection uses CEL/IEEE numeric equality: {@code -0.0} equals
 * {@code 0.0}, and {@code NaN} equals nothing — a NaN element can never be a duplicate.
 */
class RepeatedUniqueTest {

    private static final ProtoValidator VALIDATOR = ProtoValidator.create();

    private static void assertValid(Message message) {
        assertThat(VALIDATOR.validate(message).valid())
                .as("expected no violations, got %s", VALIDATOR.validate(message).violations())
                .isTrue();
    }

    @Test
    void distinctValuesPass() {
        assertValid(UniqueGauntlet.getDefaultInstance());
        assertValid(UniqueGauntlet.newBuilder().addSamples(1.0).addSamples(2.0).build());
    }

    @Test
    void exactDuplicatesFail() {
        assertThat(VALIDATOR.validate(UniqueGauntlet.newBuilder()
                        .addSamples(1.5).addSamples(1.5).build()).violations())
                .anyMatch(v -> v.path().equals("samples") && v.ruleId().equals("repeated.unique"));
    }

    @Test
    void negativeZeroEqualsPositiveZero() {
        // Boxed Double.equals would call these distinct; CEL numeric equality does not.
        assertThat(VALIDATOR.validate(UniqueGauntlet.newBuilder()
                        .addSamples(-0.0).addSamples(0.0).build()).violations())
                .anyMatch(v -> v.path().equals("samples") && v.ruleId().equals("repeated.unique"));
    }

    @Test
    void nanIsNeverADuplicate() {
        // NaN equals nothing, not even NaN: two NaN samples must not trip unique.
        assertValid(UniqueGauntlet.newBuilder().addSamples(Double.NaN).addSamples(Double.NaN).build());
        assertValid(UniqueGauntlet.newBuilder()
                .addSamples(Double.NaN).addSamples(1.0).addSamples(Double.NaN).build());
    }
}
