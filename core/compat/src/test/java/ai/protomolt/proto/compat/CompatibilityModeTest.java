package ai.protomolt.proto.compat;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** The direction and transitivity flags behind every mode constant. */
class CompatibilityModeTest {

    @Test
    void noneChecksNothing() {
        assertThat(CompatibilityMode.NONE.checksBackward()).isFalse();
        assertThat(CompatibilityMode.NONE.checksForward()).isFalse();
        assertThat(CompatibilityMode.NONE.isTransitive()).isFalse();
    }

    @Test
    void directionFlagsMatchTheConfluentDefinitions() {
        assertThat(CompatibilityMode.BACKWARD.checksBackward()).isTrue();
        assertThat(CompatibilityMode.BACKWARD.checksForward()).isFalse();

        assertThat(CompatibilityMode.FORWARD.checksBackward()).isFalse();
        assertThat(CompatibilityMode.FORWARD.checksForward()).isTrue();

        assertThat(CompatibilityMode.FULL.checksBackward()).isTrue();
        assertThat(CompatibilityMode.FULL.checksForward()).isTrue();
    }

    @Test
    void transitiveVariantsKeepTheirBaseDirection() {
        assertThat(CompatibilityMode.BACKWARD_TRANSITIVE.checksBackward()).isTrue();
        assertThat(CompatibilityMode.BACKWARD_TRANSITIVE.checksForward()).isFalse();
        assertThat(CompatibilityMode.BACKWARD_TRANSITIVE.isTransitive()).isTrue();

        assertThat(CompatibilityMode.FORWARD_TRANSITIVE.checksBackward()).isFalse();
        assertThat(CompatibilityMode.FORWARD_TRANSITIVE.checksForward()).isTrue();
        assertThat(CompatibilityMode.FORWARD_TRANSITIVE.isTransitive()).isTrue();

        assertThat(CompatibilityMode.FULL_TRANSITIVE.checksBackward()).isTrue();
        assertThat(CompatibilityMode.FULL_TRANSITIVE.checksForward()).isTrue();
        assertThat(CompatibilityMode.FULL_TRANSITIVE.isTransitive()).isTrue();
    }

    @Test
    void onlyTheTransitiveVariantsAreTransitive() {
        assertThat(CompatibilityMode.BACKWARD.isTransitive()).isFalse();
        assertThat(CompatibilityMode.FORWARD.isTransitive()).isFalse();
        assertThat(CompatibilityMode.FULL.isTransitive()).isFalse();
    }

    @Test
    void everyModeIsAccountedFor() {
        // Seven modes is the Confluent parity set; a new constant must make a deliberate
        // choice here rather than silently falling through the other tests.
        assertThat(CompatibilityMode.values()).hasSize(7);
    }
}
