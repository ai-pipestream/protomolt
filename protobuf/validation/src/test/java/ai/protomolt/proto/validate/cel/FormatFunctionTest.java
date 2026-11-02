package ai.protomolt.proto.validate.cel;

import ai.protomolt.proto.cel.CelEnvironmentFactory;
import ai.protomolt.proto.cel.CelEvaluationException;
import ai.protomolt.proto.cel.CelEvaluator;
import dev.cel.runtime.CelFunctionBinding;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The printf-style {@code <string>.format(<list>)} function protovalidate's message templates use.
 * Since cel-java 0.15.0 it comes from CEL's own {@code strings} extension, so every case here runs
 * through the compiled validation environment rather than a Java helper.
 *
 * <p>Compared with the custom implementation this module carried before 0.15.0, the CEL version
 * follows the CEL spec strictly: a malformed template (unknown verb, trailing {@code %}, empty
 * precision), a missing argument, or an argument of the wrong type for a verb is an evaluation
 * error instead of lenient pass-through text. Those cases are asserted as errors below.
 */
class FormatFunctionTest {

    private static final CelEvaluator EVAL = new CelEvaluator(CelEnvironmentFactory.builder()
            .addFunctions(ValidationCelFunctions.declarations(), ValidationCelFunctions.bindings())
            .build());

    private static Object format(String expression) {
        return EVAL.evaluateValue(expression, Map.of());
    }

    private static void assertFormatError(String expression, String messageFragment) {
        assertThatThrownBy(() -> format(expression))
                .isInstanceOf(CelEvaluationException.class)
                .hasStackTraceContaining(messageFragment);
    }

    @Test
    void signedAndUnsignedIntegers() {
        assertThat(format("'%d'.format([42])")).isEqualTo("42");
        assertThat(format("'%d'.format([-42])")).isEqualTo("-42");
        // uint64 max renders unsigned; a signed long rendering would go negative.
        assertThat(format("'%d'.format([18446744073709551615u])"))
                .isEqualTo("18446744073709551615");
        // A double with a whole value renders without a fraction.
        assertThat(format("'%d'.format([3.0])")).isEqualTo("3");
        // Strings are not numbers, even when they spell one.
        assertFormatError("'%d'.format(['42'])", "decimal clause can only be used on numbers");
    }

    @Test
    void floatingPointWithPrecision() {
        assertThat(format("'%f'.format([1.5])")).isEqualTo("1.500000");
        assertThat(format("'%.2f'.format([3.14159])")).isEqualTo("3.14");
        assertThat(format("'%.0f'.format([2.5])")).isEqualTo("2");
        assertThat(format("'%f'.format([2])")).isEqualTo("2.000000");
        assertThat(format("'%e'.format([12345.678])")).isEqualTo("1.234568e+04");
        assertThat(format("'%.1e'.format([12345.678])")).isEqualTo("1.2e+04");
        // A bare "%." with no digits is a malformed template.
        assertFormatError("'%.f'.format([1.5])", "empty precision is not allowed");
    }

    @Test
    void integerBases() {
        assertThat(format("'%x'.format([255])")).isEqualTo("ff");
        assertThat(format("'%X'.format([255])")).isEqualTo("FF");
        assertThat(format("'%x'.format([255u])")).isEqualTo("ff");
        assertThat(format("'%o'.format([8])")).isEqualTo("10");
        assertThat(format("'%b'.format([5])")).isEqualTo("101");
        assertThat(format("'%b'.format([true])")).isEqualTo("1");
        // Bytes and strings hex-encode their contents.
        assertThat(format("'%x'.format([b'\\xde\\xad'])")).isEqualTo("dead");
        assertThat(format("'%X'.format([b'\\xde\\xad'])")).isEqualTo("DEAD");
        assertThat(format("'%x'.format(['hi'])")).isEqualTo("6869");
        assertFormatError("'%o'.format(['8'])", "octal clause can only be used on integers");
    }

    @Test
    void stringDirectiveConversions() {
        assertThat(format("'%s'.format(['plain'])")).isEqualTo("plain");
        assertThat(format("'%s'.format([b'bytes'])")).isEqualTo("bytes");
        assertThat(format("'%s'.format([true])")).isEqualTo("true");
        assertThat(format("'%s'.format([7u])")).isEqualTo("7");
        // Whole doubles print without a fraction.
        assertThat(format("'%s'.format([3.0])")).isEqualTo("3");
        assertThat(format("'%s'.format([-3.0])")).isEqualTo("-3");
        assertThat(format("'%s'.format([3.5])")).isEqualTo("3.5");
        // cel-java renders a large whole double in plain digits (cel-go would print 1e+100).
        assertThat(format("'%s'.format([1e100])")).isEqualTo("1" + "0".repeat(100));
        // Containers render element by element; protovalidate's in/not_in messages rely on this.
        assertThat(format("'%s'.format([[1, 2, 3]])")).isEqualTo("[1, 2, 3]");
        assertThat(format("'%s'.format([['a', 'b']])")).isEqualTo("[a, b]");
        assertThat(format("'%s'.format([{'k': 1}])")).isEqualTo("{k: 1}");
        // Duration and timestamp values render in their CEL string forms, not as proto messages.
        assertThat(format("'%s'.format([duration('90s')])")).isEqualTo("90s");
        assertThat(format("'%s'.format([timestamp('2024-01-02T03:04:05Z')])"))
                .isEqualTo("2024-01-02T03:04:05Z");
        assertThat(format("'%s'.format([null])")).isEqualTo("null");
    }

    @Test
    void percentEscapesAndMalformedTemplates() {
        assertThat(format("'%%'.format([])")).isEqualTo("%");
        assertThat(format("'100%%'.format([])")).isEqualTo("100%");
        // A trailing '%' with no verb is a malformed template.
        assertFormatError("'end%'.format([])", "unexpected end of string");
        // An unrecognized verb is rejected rather than copied through.
        assertFormatError("'%q %s'.format(['kept'])", "unrecognized formatting clause");
        // Every directive needs an argument.
        assertFormatError("'%s %s'.format(['only'])", "index 1 out of range");
    }

    @Test
    void argumentOrderIsPositional() {
        assertThat(format("'%s-%d-%s'.format(['a', 1, 'b'])")).isEqualTo("a-1-b");
        // Surplus arguments are ignored.
        assertThat(format("'%s'.format(['a', 'b'])")).isEqualTo("a");
    }

    @Test
    void bindingOverloadIdsAreDistinct() {
        assertThat(ValidationCelFunctions.bindings().stream()
                .map(CelFunctionBinding::getOverloadId))
                .doesNotHaveDuplicates();
        // One declaration family per format function name, including the member-call helpers.
        // format() itself is the strings extension's, so it is not counted here.
        assertThat(ValidationCelFunctions.declarations()).hasSize(9);
        assertThat(ValidationCelFunctions.bindings()).hasSize(14);
    }
}
