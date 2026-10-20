package ai.protomolt.proto.repo.service;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The environment reader behind the repository service configuration: an omitted
 * variable takes its default, a present one must parse, and a refusal names the
 * variable without repeating its value.
 */
class RepositoryEnvironmentTest {

    private static final String NAME = "REPO_TEST_SETTING";

    @Test
    void omittedValuesTakeTheirDefaults() {
        RepositoryEnvironment env = new RepositoryEnvironment(Map.of());

        assertThat(env.text(NAME, "fallback")).isEqualTo("fallback");
        assertThat(env.number(NAME, 7, 0, 10)).isEqualTo(7);
        assertThat(env.flag(NAME, true)).isTrue();
        assertThat(env.httpPort(NAME, 8080)).isEqualTo(8080);
    }

    @Test
    void presentTextIsReturnedUntrimmed() {
        assertThat(new RepositoryEnvironment(Map.of(NAME, " value ")).text(NAME, "fallback"))
                .isEqualTo(" value ");
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "   "})
    void blankTextIsRefusedWhenPresent(String blank) {
        assertThatThrownBy(() -> new RepositoryEnvironment(Map.of(NAME, blank)).text(NAME, "x"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(NAME + " must not be blank when present");
    }

    @Test
    void aNullTextValueIsRefusedLikeABlankOne() {
        Map<String, String> values = new HashMap<>();
        values.put(NAME, null);

        assertThatThrownBy(() -> new RepositoryEnvironment(values).text(NAME, "x"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(NAME + " must not be blank when present");
    }

    @Test
    void numbersAreTrimmedAndAcceptedAtBothBounds() {
        assertThat(new RepositoryEnvironment(Map.of(NAME, " 0 ")).number(NAME, 5, 0, 10)).isZero();
        assertThat(new RepositoryEnvironment(Map.of(NAME, "10")).number(NAME, 5, 0, 10)).isEqualTo(10);
    }

    @ParameterizedTest
    @ValueSource(strings = {"-1", "11", "secret-token", "1.5", "", "99999999999999999999"})
    void malformedOrOutOfRangeNumbersAreRefusedWithoutEchoingTheValue(String value) {
        assertThatThrownBy(() -> new RepositoryEnvironment(Map.of(NAME, value)).number(NAME, 5, 0, 10))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(NAME + " must be an integer between 0 and 10")
                .hasNoCause();
    }

    @Test
    void aNullNumberIsRefused() {
        Map<String, String> values = new HashMap<>();
        values.put(NAME, null);

        assertThatThrownBy(() -> new RepositoryEnvironment(values).number(NAME, 5, 0, 10))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(NAME + " must be an integer between 0 and 10");
    }

    @ParameterizedTest
    @ValueSource(strings = {"true", "1", "yes", "on", " TRUE ", "Yes"})
    void trueFlagForms(String value) {
        assertThat(new RepositoryEnvironment(Map.of(NAME, value)).flag(NAME, false)).isTrue();
    }

    @ParameterizedTest
    @ValueSource(strings = {"false", "0", "no", "off", " OFF ", "No"})
    void falseFlagForms(String value) {
        assertThat(new RepositoryEnvironment(Map.of(NAME, value)).flag(NAME, true)).isFalse();
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "enabled", "2", "y"})
    void anyOtherFlagValueIsRefused(String value) {
        assertThatThrownBy(() -> new RepositoryEnvironment(Map.of(NAME, value)).flag(NAME, true))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(NAME + " must be true/false, 1/0, yes/no or on/off");
    }

    @ParameterizedTest
    @ValueSource(strings = {"off", "OFF", " Off "})
    void anHttpPortOfOffDisablesTheListener(String value) {
        assertThat(new RepositoryEnvironment(Map.of(NAME, value)).httpPort(NAME, 8080)).isZero();
    }

    @Test
    void anHttpPortIsAnIntegerFromZeroTo65535() {
        assertThat(new RepositoryEnvironment(Map.of(NAME, "65535")).httpPort(NAME, 8080)).isEqualTo(65535);
        assertThatThrownBy(() -> new RepositoryEnvironment(Map.of(NAME, "65536")).httpPort(NAME, 8080))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(NAME + " must be an integer between 0 and 65535");
        assertThatThrownBy(() -> new RepositoryEnvironment(Map.of(NAME, "none")).httpPort(NAME, 8080))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(NAME + " must be an integer between 0 and 65535");
    }

    @Test
    void aNullMapIsRefused() {
        assertThatThrownBy(() -> new RepositoryEnvironment(null))
                .isInstanceOf(NullPointerException.class);
    }
}
