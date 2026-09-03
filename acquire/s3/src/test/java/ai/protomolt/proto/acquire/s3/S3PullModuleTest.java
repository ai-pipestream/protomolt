package ai.protomolt.proto.acquire.s3;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Env config: missing identity refused loudly by variable name, nothing defaulted. */
class S3PullModuleTest {

    @Test
    void missingApiKeyIsRefusedByVariableName() {
        assertThatThrownBy(() -> S3PullModule.Config.fromEnvironment(
                Map.of(S3PullModule.ENV_REGION, "us-east-1")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("PROTOMOLT_ACQUIRE_API_KEY");
    }

    @Test
    void missingRegionIsRefusedByVariableName() {
        assertThatThrownBy(() -> S3PullModule.Config.fromEnvironment(
                Map.of(S3PullModule.ENV_API_KEY, "key")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("PROTOMOLT_ACQUIRE_S3_REGION");
    }

    @Test
    void endpointAndStaticCredentialsAreOptional() {
        S3PullModule.Config config = S3PullModule.Config.fromEnvironment(Map.of(
                S3PullModule.ENV_API_KEY, "key",
                S3PullModule.ENV_REGION, "us-east-1"));
        assertThat(config.endpoint()).isEmpty();
        assertThat(config.accessKey()).isEmpty();
        assertThat(config.region()).isEqualTo("us-east-1");
        assertThat(config.apiKey()).isEqualTo("key");
    }
}
