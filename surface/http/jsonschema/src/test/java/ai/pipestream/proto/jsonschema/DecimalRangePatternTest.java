package ai.pipestream.proto.jsonschema;

import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

class DecimalRangePatternTest {

    @Test
    void boundedRangesMatchExactlyTheirMembers() {
        // Brute force: every bounded range inside [-130, 130] must match exactly its members.
        for (int lo = -130; lo <= 130; lo += 13) {
            for (int hi = lo; hi <= 130; hi += 7) {
                Pattern pattern = compile(lo, hi);
                for (int n = -140; n <= 140; n++) {
                    boolean expected = n >= lo && n <= hi;
                    assertThat(pattern.matcher(Integer.toString(n)).matches())
                            .as("[%d, %d] against %d", lo, hi, n)
                            .isEqualTo(expected);
                }
            }
        }
    }

    @Test
    void multiDigitLengthTransitionsAreExact() {
        Pattern pattern = compile(87, 12345);
        assertThat(pattern.matcher("87").matches()).isTrue();
        assertThat(pattern.matcher("86").matches()).isFalse();
        assertThat(pattern.matcher("99").matches()).isTrue();
        assertThat(pattern.matcher("100").matches()).isTrue();
        assertThat(pattern.matcher("9999").matches()).isTrue();
        assertThat(pattern.matcher("12345").matches()).isTrue();
        assertThat(pattern.matcher("12346").matches()).isFalse();
        assertThat(pattern.matcher("99999").matches()).isFalse();
    }

    @Test
    void unboundedAboveMatchesEverythingFromTheLowerBound() {
        Pattern pattern = Pattern.compile(
                DecimalRangePattern.range(BigInteger.valueOf(250), null));
        assertThat(pattern.matcher("249").matches()).isFalse();
        assertThat(pattern.matcher("250").matches()).isTrue();
        assertThat(pattern.matcher("999").matches()).isTrue();
        assertThat(pattern.matcher("1000").matches()).isTrue();
        assertThat(pattern.matcher("18446744073709551615").matches()).isTrue();
        assertThat(pattern.matcher("-1").matches()).isFalse();
    }

    @Test
    void unboundedBelowMatchesEverythingUpToTheUpperBound() {
        Pattern pattern = Pattern.compile(
                DecimalRangePattern.range(null, BigInteger.valueOf(-3)));
        assertThat(pattern.matcher("-3").matches()).isTrue();
        assertThat(pattern.matcher("-100").matches()).isTrue();
        assertThat(pattern.matcher("-9223372036854775808").matches()).isTrue();
        assertThat(pattern.matcher("-2").matches()).isFalse();
        assertThat(pattern.matcher("0").matches()).isFalse();
        assertThat(pattern.matcher("3").matches()).isFalse();
    }

    @Test
    void int64ExtremesAreHandled() {
        Pattern pattern = Pattern.compile(DecimalRangePattern.range(
                BigInteger.valueOf(Long.MIN_VALUE), BigInteger.valueOf(Long.MAX_VALUE)));
        assertThat(pattern.matcher("-9223372036854775808").matches()).isTrue();
        assertThat(pattern.matcher("9223372036854775807").matches()).isTrue();
        assertThat(pattern.matcher("-9223372036854775809").matches()).isFalse();
        assertThat(pattern.matcher("9223372036854775808").matches()).isFalse();
    }

    @Test
    void uint64ExtremesAreHandled() {
        Pattern pattern = Pattern.compile(DecimalRangePattern.range(
                BigInteger.ZERO, new BigInteger("18446744073709551615")));
        assertThat(pattern.matcher("0").matches()).isTrue();
        assertThat(pattern.matcher("18446744073709551615").matches()).isTrue();
        assertThat(pattern.matcher("18446744073709551616").matches()).isFalse();
        assertThat(pattern.matcher("-1").matches()).isFalse();
    }

    @Test
    void neverMatchesNonCanonicalSpellings() {
        Pattern pattern = compile(-100, 100);
        assertThat(pattern.matcher("007").matches()).isFalse();
        assertThat(pattern.matcher("-0").matches()).isFalse();
        assertThat(pattern.matcher("+5").matches()).isFalse();
        assertThat(pattern.matcher("5 ").matches()).isFalse();
        assertThat(pattern.matcher("").matches()).isFalse();
    }

    @Test
    void emptyRangeMatchesNothing() {
        Pattern pattern = compile(10, 10);
        assertThat(pattern.matcher("10").matches()).isTrue();
        Pattern empty = Pattern.compile(
                DecimalRangePattern.range(BigInteger.TEN, BigInteger.ONE));
        assertThat(empty.matcher("5").matches()).isFalse();
        assertThat(empty.matcher("").matches()).isFalse();
    }

    private static Pattern compile(long lo, long hi) {
        return Pattern.compile(
                DecimalRangePattern.range(BigInteger.valueOf(lo), BigInteger.valueOf(hi)));
    }
}
