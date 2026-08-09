package ai.pipestream.proto.grpc.invoke;

import ai.pipestream.proto.grpc.policy.OutboundChannelPolicy;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Verifies the standard invoke factory exposes the host policy without changing the open API. */
class ChannelFactoryPolicyTest {

    @Test
    void standardFactoryValidatesTargetsAndDeadlinesThroughTheCompatibilitySeam() {
        OutboundChannelPolicy policy = OutboundChannelPolicy.builder()
                .allowedHosts(Set.of("api.example.com"))
                .allowPlaintext(false)
                .maxDeadline(Duration.ofSeconds(5))
                .build();
        ChannelFactory factory = ChannelFactory.standard(policy);

        assertThat(factory.policy()).isSameAs(policy);
        factory.validateTarget("api.example.com:443", true);
        factory.validateDeadline(5_000);
        assertThatThrownBy(() -> factory.validateTarget("other.example.com:443", true))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("host");
        assertThatThrownBy(() -> factory.validateTarget("api.example.com:443", false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("plaintext");
        assertThatThrownBy(() -> factory.validateDeadline(5_001))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maximum");
    }
}
