package ai.protomolt.proto.platform;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

/** {@link DocumentPlatformConfig}'s numeric environment values: defaults when unset, refusal by name when malformed. */
class DocumentPlatformConfigTest {

    @Test
    void malformedNumberIsRefusedByVariableName() {
        assertThatThrownBy(() -> DocumentPlatformConfig.intValue(
                DocumentPlatformConfig.ENV_REGISTRY_PORT, "80 80", 9093))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(DocumentPlatformConfig.ENV_REGISTRY_PORT + " must be an integer: 80 80")
                .hasCauseInstanceOf(NumberFormatException.class);
    }

    @Test
    void unsetOrBlankKeepsTheDefault() {
        assertThat(DocumentPlatformConfig.intValue(DocumentPlatformConfig.ENV_REGISTRY_PORT, null, 9093))
                .isEqualTo(9093);
        assertThat(DocumentPlatformConfig.intValue(DocumentPlatformConfig.ENV_REGISTRY_PORT, " ", 9093))
                .isEqualTo(9093);
        assertThat(DocumentPlatformConfig.intValue(DocumentPlatformConfig.ENV_REGISTRY_PORT, " 8080 ", 9093))
                .isEqualTo(8080);
    }
}
