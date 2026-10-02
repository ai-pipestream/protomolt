package ai.protomolt.proto.serve;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ApiTokenConfigurationTest {
    @Test
    void blankTokensCannotDisableAuthenticationThroughOptionsOrCli() {
        for (String token : new String[] {"", " ", "\t"}) {
            assertThatThrownBy(() -> new ProtoMoltServe.Options("127.0.0.1", 0, 0, null, 0, token))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("API token");
            assertThatThrownBy(() -> ProtoMoltServe.Options.parse(new String[] {"--api-token", token}))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("API token");
        }
    }

    @Test
    void explicitNullAndExactNonblankTokensRetainTheirMeaning() {
        assertThat(new ProtoMoltServe.Options("127.0.0.1", 0, 0, null, 0, null).apiToken()).isNull();
        assertThat(new ProtoMoltServe.Options("127.0.0.1", 0, 0, null, 0, "secret").apiToken())
                .isEqualTo("secret");
    }
}
