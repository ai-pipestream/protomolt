package ai.protomolt.proto.registry;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The Confluent compatibility vocabulary: seven exact, uppercase mode names. */
class CompatibilityModesTest {

    private static final String VALID_LIST = "BACKWARD, BACKWARD_TRANSITIVE, FORWARD, "
            + "FORWARD_TRANSITIVE, FULL, FULL_TRANSITIVE, NONE";

    @ParameterizedTest
    @ValueSource(strings = {"NONE", "BACKWARD", "FORWARD", "FULL",
            "BACKWARD_TRANSITIVE", "FORWARD_TRANSITIVE", "FULL_TRANSITIVE"})
    void theSevenConfluentModesAreReturnedUnchanged(String mode) {
        assertThat(CompatibilityModes.isValid(mode)).isTrue();
        assertThat(CompatibilityModes.requireValid(mode)).isSameAs(mode);
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = {"", "backward", "Backward", " BACKWARD", "TRANSITIVE", "FULL-TRANSITIVE"})
    void anythingElseIsRefusedWithTheSortedVocabulary(String mode) {
        assertThat(CompatibilityModes.isValid(mode)).isFalse();
        assertThatThrownBy(() -> CompatibilityModes.requireValid(mode))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Invalid compatibility mode: " + mode + " (valid: " + VALID_LIST + ")");
    }

    @Test
    void theDefaultsAreThemselvesValid() {
        assertThat(CompatibilityModes.isValid(CompatibilityModes.NONE)).isTrue();
        assertThat(CompatibilityModes.isValid(CompatibilityModes.DEFAULT_GLOBAL)).isTrue();
        assertThat(CompatibilityModes.DEFAULT_GLOBAL).isEqualTo("BACKWARD");
    }
}
