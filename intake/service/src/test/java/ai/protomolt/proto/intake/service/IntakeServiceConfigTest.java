package ai.protomolt.proto.intake.service;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** {@link IntakeServiceConfig}'s numeric environment values: defaults when unset, refusal by name when malformed. */
class IntakeServiceConfigTest {

    @Test
    void malformedNumbersAreRefusedByVariableName() {
        assertThatThrownBy(() -> IntakeServiceConfig.intValue(IntakeServiceConfig.ENV_GRPC_PORT, "grpc", 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(IntakeServiceConfig.ENV_GRPC_PORT + " must be an integer: grpc")
                .hasCauseInstanceOf(NumberFormatException.class);
        assertThatThrownBy(() -> IntakeServiceConfig.longValue(IntakeServiceConfig.ENV_MAX_PAYLOAD_BYTES, "1.5", 1L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(IntakeServiceConfig.ENV_MAX_PAYLOAD_BYTES + " must be an integer: 1.5")
                .hasCauseInstanceOf(NumberFormatException.class);
    }

    @Test
    void unsetOrBlankKeepsTheDefault() {
        assertThat(IntakeServiceConfig.intValue(IntakeServiceConfig.ENV_GRPC_PORT, null, 7)).isEqualTo(7);
        assertThat(IntakeServiceConfig.intValue(IntakeServiceConfig.ENV_GRPC_PORT, "", 7)).isEqualTo(7);
        assertThat(IntakeServiceConfig.intValue(IntakeServiceConfig.ENV_GRPC_PORT, "9095", 7)).isEqualTo(9095);
        assertThat(IntakeServiceConfig.longValue(IntakeServiceConfig.ENV_MAX_PAYLOAD_BYTES, "  ", 3L)).isEqualTo(3L);
        assertThat(IntakeServiceConfig.longValue(IntakeServiceConfig.ENV_MAX_PAYLOAD_BYTES, "120", 3L)).isEqualTo(120L);
    }
}
