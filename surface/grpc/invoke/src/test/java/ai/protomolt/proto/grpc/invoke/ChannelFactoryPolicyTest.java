package ai.protomolt.proto.grpc.invoke;

import ai.protomolt.proto.grpc.policy.OutboundChannelPolicy;
import io.grpc.ChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.TlsChannelCredentials;
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

    @Test
    void credentialedChannelsGoThroughTheSamePolicy() {
        OutboundChannelPolicy policy = OutboundChannelPolicy.builder()
                .allowedHosts(Set.of("api.example.com"))
                .allowPlaintext(false)
                .build();
        ChannelFactory factory = ChannelFactory.standard(policy);
        ChannelCredentials credentials = TlsChannelCredentials.create();

        assertThatThrownBy(() -> factory.open("other.example.com:443", credentials))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("host");
        assertThatThrownBy(() -> factory.open("api.example.com:443", (ChannelCredentials) null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("credentials");
        ManagedChannel channel = factory.open("api.example.com:443", credentials);
        channel.shutdownNow();
    }

    @Test
    void aCustomFactoryWithoutTlsSupportRefusesCredentials() {
        ChannelFactory plaintextOnly = (target, tls) -> {
            throw new AssertionError("not reached");
        };
        assertThatThrownBy(() -> plaintextOnly.open("api.example.com:443",
                TlsChannelCredentials.create()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cannot present custom TLS credentials");
    }

    @Test
    void upstreamStatusWithholdsDetailsWhenChannelCredentialsArePresent() {
        Status detailed = Status.PERMISSION_DENIED
                .withDescription("client CN=service-a denied")
                .withCause(new IllegalStateException("peer detail"));

        Status uncredentialed = UpstreamStatus.reply(detailed, null, null);
        assertThat(uncredentialed.getDescription()).isEqualTo("client CN=service-a denied");
        assertThat(uncredentialed.getCause()).isNotNull();

        Status tlsCredentialed = UpstreamStatus.reply(
                detailed, null, TlsChannelCredentials.create());
        assertThat(tlsCredentialed.getCode()).isEqualTo(Status.Code.PERMISSION_DENIED);
        assertThat(tlsCredentialed.getDescription()).isNull();
        assertThat(tlsCredentialed.getCause()).isNull();
    }
}
