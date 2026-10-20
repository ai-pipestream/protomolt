package ai.protomolt.proto.parse.service;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** {@link ParseCoordinatorConfig}'s numeric environment values: defaults when unset, refusal by name when malformed. */
class ParseCoordinatorConfigTest {

    @Test
    void malformedNumbersAreRefusedByVariableName() {
        assertThatThrownBy(() -> ParseCoordinatorConfig.intValue(ParseCoordinatorConfig.ENV_GRPC_PORT, "grpc", 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(ParseCoordinatorConfig.ENV_GRPC_PORT + " must be an integer: grpc")
                .hasCauseInstanceOf(NumberFormatException.class);
        assertThatThrownBy(() -> ParseCoordinatorConfig.longValue(ParseCoordinatorConfig.ENV_PARSE_DEADLINE_SECONDS, "1.5", 1L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(ParseCoordinatorConfig.ENV_PARSE_DEADLINE_SECONDS + " must be an integer: 1.5")
                .hasCauseInstanceOf(NumberFormatException.class);
    }

    @Test
    void unsetOrBlankKeepsTheDefault() {
        assertThat(ParseCoordinatorConfig.intValue(ParseCoordinatorConfig.ENV_GRPC_PORT, null, 7)).isEqualTo(7);
        assertThat(ParseCoordinatorConfig.intValue(ParseCoordinatorConfig.ENV_GRPC_PORT, "", 7)).isEqualTo(7);
        assertThat(ParseCoordinatorConfig.intValue(ParseCoordinatorConfig.ENV_GRPC_PORT, "9095", 7)).isEqualTo(9095);
        assertThat(ParseCoordinatorConfig.longValue(ParseCoordinatorConfig.ENV_PARSE_DEADLINE_SECONDS, "  ", 3L)).isEqualTo(3L);
        assertThat(ParseCoordinatorConfig.longValue(ParseCoordinatorConfig.ENV_PARSE_DEADLINE_SECONDS, "120", 3L)).isEqualTo(120L);
    }
}
