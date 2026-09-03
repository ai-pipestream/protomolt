package ai.protomolt.proto.validate.cel;

import com.google.common.primitives.UnsignedLong;
import com.google.protobuf.ByteString;
import dev.cel.runtime.CelFunctionBinding;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The printf-style {@code format} standard-library function: directive rendering, precision,
 * argument conversion (unsigned, bytes, whole doubles), and the pass-through rules for
 * unrecognized or truncated directives.
 */
class FormatFunctionTest {

    private static String format(String fmt, Object... args) {
        return ValidationCelFunctions.formatString(fmt, List.of(args));
    }

    @Test
    void signedAndUnsignedIntegers() {
        assertThat(format("%d", 42L)).isEqualTo("42");
        assertThat(format("%d", "42")).isEqualTo("42");
        // UnsignedLong renders through its own toString; longValue() would go negative.
        assertThat(format("%d", UnsignedLong.fromLongBits(-1L)))
                .isEqualTo("18446744073709551615");
    }

    @Test
    void floatingPointWithPrecision() {
        assertThat(format("%f", 1.5)).isEqualTo("1.500000");
        assertThat(format("%.2f", 3.14159)).isEqualTo("3.14");
        assertThat(format("%e", 12345.678)).isEqualTo("1.234568e+04");
        assertThat(format("%.1e", 12345.678)).isEqualTo("1.2e+04");
        // A bare "%." with no digits falls back to the default precision.
        assertThat(format("%.f", 1.5)).isEqualTo("1.500000");
    }

    @Test
    void integerBases() {
        assertThat(format("%x", 255L)).isEqualTo("ff");
        assertThat(format("%X", 255L)).isEqualTo("FF");
        assertThat(format("%o", 8L)).isEqualTo("10");
        assertThat(format("%b", 5L)).isEqualTo("101");
        // Bytes and strings hex-encode their contents.
        assertThat(format("%x", ByteString.copyFrom(new byte[] {(byte) 0xde, (byte) 0xad})))
                .isEqualTo("dead");
        assertThat(format("%x", "hi")).isEqualTo("6869");
    }

    @Test
    void stringDirectiveConversions() {
        assertThat(format("%s", "plain")).isEqualTo("plain");
        assertThat(format("%s", ByteString.copyFromUtf8("bytes"))).isEqualTo("bytes");
        // Whole doubles print without a fraction.
        assertThat(format("%s", 3.0)).isEqualTo("3");
        assertThat(format("%s", -3.0)).isEqualTo("-3");
        assertThat(format("%s", 3.5)).isEqualTo("3.5");
        // Beyond long range a whole double switches to CEL's exponent form.
        assertThat(format("%s", 1e100)).isEqualTo("1e+100");
        // A missing argument renders as "null" without consuming anything.
        assertThat(format("%s %s", "only")).isEqualTo("only null");
    }

    @Test
    void percentEscapesAndTruncation() {
        assertThat(format("%%")).isEqualTo("%");
        assertThat(format("100%%")).isEqualTo("100%");
        // A trailing '%' with no verb is copied through.
        assertThat(format("end%")).isEqualTo("end%");
        // An unrecognized verb passes through and consumes no argument.
        assertThat(format("%q %s", "kept")).isEqualTo("%q kept");
    }

    @Test
    void argumentOrderIsPositional() {
        assertThat(format("%s-%d-%s", "a", 1L, "b")).isEqualTo("a-1-b");
    }

    @Test
    void bindingOverloadIdsAreDistinct() {
        assertThat(ValidationCelFunctions.bindings().stream()
                .map(CelFunctionBinding::getOverloadId))
                .doesNotHaveDuplicates();
        // One declaration family per format function name, including the member-call helpers.
        assertThat(ValidationCelFunctions.declarations()).hasSize(10);
        assertThat(ValidationCelFunctions.bindings()).hasSize(15);
    }
}
