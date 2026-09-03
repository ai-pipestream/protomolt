package ai.protomolt.proto.grpc.invoke;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ReflectionClientTest {

    @Test
    void rejectsUnboundedTimeoutBeforeOpeningAReflectionStream() {
        assertThatThrownBy(() -> ReflectionClient.discover(null,
                ReflectionClient.MAX_TIMEOUT_MS + 1))
                .isInstanceOf(ReflectionException.class)
                .hasMessageContaining("must be from 1 to");
    }
}
