package ai.protomolt.proto.platform;

import java.util.Map;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ApiTokenConfigurationTest {
    @Test
    void suppliedBlankSecretsDoNotOpenTheNode() {
        for (String token : new String[] {"", " ", "\t"}) {
            assertThatThrownBy(() -> PlatformEnvironment.apiTokenFromEnvironment(
                    Map.of(DocumentPlatformConfig.ENV_API_TOKEN, token)))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("PROTOMOLT_API_TOKEN");
        }
    }

    @Test
    void absentTokensAndExactSecretValuesRetainTheirMeaning() {
        assertThat(PlatformEnvironment.apiTokenFromEnvironment(Map.of())).isNull();
        assertThat(PlatformEnvironment.apiTokenFromEnvironment(
                Map.of(DocumentPlatformConfig.ENV_API_TOKEN, " secret "))).isEqualTo(" secret ");
    }
}
