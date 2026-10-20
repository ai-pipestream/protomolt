package ai.protomolt.proto.acquire.pull;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for the {@code PROTOMOLT_ACQUIRE_INTAKE_PLAINTEXT} opt-out parsing. */
class GrpcIntakeFeedPlaintextTest {

    @Test
    void unsetBlankOrNullEnvironmentKeepsTls() {
        assertThat(GrpcIntakeFeed.plaintextRequested(null)).isFalse();
        assertThat(GrpcIntakeFeed.plaintextRequested(Map.of())).isFalse();
        assertThat(GrpcIntakeFeed.plaintextRequested(
                Map.of(GrpcIntakeFeed.ENV_PLAINTEXT, ""))).isFalse();
        assertThat(GrpcIntakeFeed.plaintextRequested(
                Map.of(GrpcIntakeFeed.ENV_PLAINTEXT, "   "))).isFalse();
    }

    @Test
    void recognizedFormsParse() {
        assertThat(GrpcIntakeFeed.plaintextRequested(
                Map.of(GrpcIntakeFeed.ENV_PLAINTEXT, "true"))).isTrue();
        assertThat(GrpcIntakeFeed.plaintextRequested(
                Map.of(GrpcIntakeFeed.ENV_PLAINTEXT, "TRUE"))).isTrue();
        assertThat(GrpcIntakeFeed.plaintextRequested(
                Map.of(GrpcIntakeFeed.ENV_PLAINTEXT, " 1 "))).isTrue();
        assertThat(GrpcIntakeFeed.plaintextRequested(
                Map.of(GrpcIntakeFeed.ENV_PLAINTEXT, "false"))).isFalse();
        assertThat(GrpcIntakeFeed.plaintextRequested(
                Map.of(GrpcIntakeFeed.ENV_PLAINTEXT, "0"))).isFalse();
    }

    @Test
    void unrecognizedValuesAreRefusedByName() {
        for (String bad : new String[] {"yes", "no", "on", "off", "ture", "plaintext"}) {
            assertThatThrownBy(() -> GrpcIntakeFeed.plaintextRequested(
                    Map.of(GrpcIntakeFeed.ENV_PLAINTEXT, bad)))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("PROTOMOLT_ACQUIRE_INTAKE_PLAINTEXT");
        }
    }
}
