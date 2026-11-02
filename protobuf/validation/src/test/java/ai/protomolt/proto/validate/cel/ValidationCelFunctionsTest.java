package ai.protomolt.proto.validate.cel;

import ai.protomolt.proto.cel.CelEnvironmentFactory;
import ai.protomolt.proto.cel.CelEvaluationException;
import ai.protomolt.proto.cel.CelEvaluator;
import dev.cel.common.types.SimpleType;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifies the format standard-library functions are usable as member calls in a CEL environment
 * built through {@link CelEnvironmentFactory#addFunctions} — the wiring protovalidate's
 * {@code cel} rules rely on.
 */
class ValidationCelFunctionsTest {

    private static final CelEvaluator EVAL = new CelEvaluator(CelEnvironmentFactory.builder()
            .addVar("s", SimpleType.STRING)
            .addVar("d", SimpleType.DOUBLE)
            .addFunctions(ValidationCelFunctions.declarations(), ValidationCelFunctions.bindings())
            .build());

    private static boolean eval(String expr, Map<String, Object> vars) {
        return EVAL.evaluateBoolean(expr, vars);
    }

    @Test
    void stringFormatFunctions() {
        assertThat(eval("s.isHostname()", Map.of("s", "example.com"))).isTrue();
        assertThat(eval("s.isHostname()", Map.of("s", "-bad.com"))).isFalse();
        assertThat(eval("s.isEmail()", Map.of("s", "user@example.com"))).isTrue();
        assertThat(eval("s.isEmail()", Map.of("s", "nope"))).isFalse();
        assertThat(eval("s.isUri()", Map.of("s", "https://example.com/p?q=1"))).isTrue();
        assertThat(eval("s.isUriRef()", Map.of("s", "../relative"))).isTrue();
        assertThat(eval("s.isHostAndPort(true)", Map.of("s", "example.com:8080"))).isTrue();
        assertThat(eval("s.isHostAndPort(true)", Map.of("s", "example.com"))).isFalse();
    }

    @Test
    void ipFunctions() {
        assertThat(eval("s.isIp()", Map.of("s", "::1"))).isTrue();
        assertThat(eval("s.isIp(4)", Map.of("s", "127.0.0.1"))).isTrue();
        assertThat(eval("s.isIp(6)", Map.of("s", "127.0.0.1"))).isFalse();
        assertThat(eval("s.isIpPrefix()", Map.of("s", "192.168.0.0/24"))).isTrue();
        assertThat(eval("s.isIpPrefix(4, true)", Map.of("s", "192.168.0.1/24"))).isFalse();
        assertThat(eval("s.isIpPrefix(4, true)", Map.of("s", "192.168.0.0/24"))).isTrue();
    }

    @Test
    void floatFunctions() {
        assertThat(eval("d.isNan()", Map.of("d", Double.NaN))).isTrue();
        assertThat(eval("d.isNan()", Map.of("d", 1.0))).isFalse();
        assertThat(eval("d.isInf()", Map.of("d", Double.POSITIVE_INFINITY))).isTrue();
        assertThat(eval("d.isInf(1)", Map.of("d", Double.NEGATIVE_INFINITY))).isFalse();
        assertThat(eval("d.isInf(-1)", Map.of("d", Double.NEGATIVE_INFINITY))).isTrue();
    }

    /**
     * {@code format} comes from CEL's own {@code strings} extension (cel-java 0.15.0+), not from
     * this class, so it is exercised through the compiled environment the way a rule would use it.
     */
    @Test
    void formatSubstitutesDirectivesInOrder() {
        assertThat(EVAL.evaluateValue("'%s = %d'.format(['count', 3])", Map.of()))
                .isEqualTo("count = 3");
        assertThat(EVAL.evaluateValue("'100%% of %s'.format(['it'])", Map.of()))
                .isEqualTo("100% of it");
        assertThat(EVAL.evaluateValue("'must be %x'.format([b'\\x01\\xff'])", Map.of()))
                .isEqualTo("must be 01ff");
        assertThat(EVAL.evaluateValue("'in %s'.format([['a', 'b']])", Map.of()))
                .isEqualTo("in [a, b]");
    }

    /**
     * The CEL spec rejects an unknown verb instead of copying it through (the pre-0.15.0 custom
     * implementation here used to pass {@code %q} along literally).
     */
    @Test
    void unknownFormatVerbIsAnEvaluationError() {
        assertThatThrownBy(() -> EVAL.evaluateValue("'%q %s'.format(['kept'])", Map.of()))
                .isInstanceOf(CelEvaluationException.class)
                .hasStackTraceContaining("unrecognized formatting clause");
    }

    @Test
    void standardMacrosAreEnabled() {
        // 'has' and comprehension macros must be available for protovalidate's cel expressions.
        assertThat(eval("[1, 2, 3].all(x, x > 0)", Map.of())).isTrue();
        assertThat(eval("[1, 2, 3].exists(x, x == 2)", Map.of())).isTrue();
    }
}
