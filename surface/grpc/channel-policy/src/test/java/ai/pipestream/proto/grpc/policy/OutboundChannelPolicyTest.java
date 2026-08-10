package ai.pipestream.proto.grpc.policy;

import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OutboundChannelPolicyTest {

    @Test
    void parsesPlainDnsAndExplicitIpTargetsWithoutResolvingThem() {
        OutboundChannelPolicy policy = OutboundChannelPolicy.defaults();

        assertThat(policy.validateTarget("localhost:9090", false))
                .extracting(OutboundChannelPolicy.ValidatedTarget::scheme,
                        OutboundChannelPolicy.ValidatedTarget::host,
                        OutboundChannelPolicy.ValidatedTarget::port,
                        OutboundChannelPolicy.ValidatedTarget::canonicalTarget)
                .containsExactly("dns", "localhost", 9090, "dns:///localhost:9090");
        assertThat(policy.validateTarget("dns:///api.example:443", true).canonicalTarget())
                .isEqualTo("dns:///api.example:443");
        assertThat(policy.validateTarget("ipv4:127.0.0.1:50051", false).scheme())
                .isEqualTo("ipv4");
        assertThat(policy.validateTarget("[::1]:50051", false).canonicalTarget())
                .isEqualTo("dns:///[::1]:50051");
    }

    @Test
    void classifiesPlainIpLiteralsForSchemeAllowlists() {
        OutboundChannelPolicy ipv4Only = OutboundChannelPolicy.builder()
                .allowedSchemes(Set.of("ipv4")).build();
        assertThat(ipv4Only.validateTarget("127.0.0.1:50051", false).scheme())
                .isEqualTo("ipv4");
        assertThatThrownBy(() -> ipv4Only.validateTarget("localhost:50051", false))
                .isInstanceOf(OutboundChannelPolicyException.class);

        OutboundChannelPolicy ipv6Only = OutboundChannelPolicy.builder()
                .allowedSchemes(Set.of("ipv6")).build();
        assertThat(ipv6Only.validateTarget("[::1]:50051", false).scheme())
                .isEqualTo("ipv6");
        assertThatThrownBy(() -> ipv6Only.validateTarget("127.0.0.1:50051", false))
                .isInstanceOf(OutboundChannelPolicyException.class);
    }

    @Test
    void rejectsResolverEscapeSyntaxAndMalformedHostsBeforeOpeningAnything() {
        OutboundChannelPolicy policy = OutboundChannelPolicy.defaults();

        assertThatThrownBy(() -> policy.validateTarget("http://example:443", true))
                .isInstanceOf(OutboundChannelPolicyException.class);
        assertThatThrownBy(() -> policy.validateTarget("dns:///other/path:443", true))
                .isInstanceOf(OutboundChannelPolicyException.class);
        assertThatThrownBy(() -> policy.validateTarget("example.invalid", false))
                .isInstanceOf(OutboundChannelPolicyException.class);
        assertThatThrownBy(() -> policy.validateTarget("127.0.0.1:0", false))
                .isInstanceOf(OutboundChannelPolicyException.class);
        assertThatThrownBy(() -> policy.validateTarget("ipv4:-1.2.3.4:50051", false))
                .isInstanceOf(OutboundChannelPolicyException.class);
        assertThatThrownBy(() -> policy.validateTarget("ipv4:+1.2.3.4:50051", false))
                .isInstanceOf(OutboundChannelPolicyException.class);
        assertThatThrownBy(() -> policy.validateTarget("ipv6:[::::]:50051", false))
                .isInstanceOf(OutboundChannelPolicyException.class);
        assertThatThrownBy(() -> policy.validateTarget("ipv6:[1:::2]:50051", false))
                .isInstanceOf(OutboundChannelPolicyException.class);
    }

    @Test
    void enforcesSchemeHostPortAndTransportAllowlists() {
        OutboundChannelPolicy policy = OutboundChannelPolicy.builder()
                .allowedSchemes(Set.of("dns"))
                .allowedHosts(Set.of("*.example.com"))
                .allowedPorts(Set.of(443))
                .allowPlaintext(false)
                .allowTls(true)
                .build();

        assertThat(policy.validateTarget("dns:///api.example.com:443", true).host())
                .isEqualTo("api.example.com");
        assertThatThrownBy(() -> policy.validateTarget("dns:///example.com:443", true))
                .isInstanceOf(OutboundChannelPolicyException.class);
        assertThatThrownBy(() -> policy.validateTarget("dns:///api.example.com:80", true))
                .isInstanceOf(OutboundChannelPolicyException.class);
        assertThatThrownBy(() -> policy.validateTarget("dns:///api.example.com:443", false))
                .isInstanceOf(OutboundChannelPolicyException.class);
    }

    @Test
    void enforcesDeadlineAndActiveChannelBudget() {
        OutboundChannelPolicy policy = OutboundChannelPolicy.builder()
                .maxDeadline(Duration.ofSeconds(2))
                .maxActiveChannels(1)
                .build();
        AtomicReference<String> seen = new AtomicReference<>();
        ManagedChannel first = policy.open("localhost:9090", false, target -> {
            seen.set(target);
            return InProcessChannelBuilder.forName("policy-test").build();
        });
        assertThat(seen).hasValue("dns:///localhost:9090");
        assertThatThrownBy(() -> policy.open("localhost:9090", false,
                target -> InProcessChannelBuilder.forName("policy-test-2").build()))
                .isInstanceOf(OutboundChannelPolicyException.class)
                .hasMessageContaining("concurrency");
        assertThatThrownBy(() -> policy.validateDeadline(2_001))
                .isInstanceOf(OutboundChannelPolicyException.class)
                .hasMessageContaining("maximum");

        first.shutdownNow();
        ManagedChannel second = policy.open("localhost:9090", false,
                target -> InProcessChannelBuilder.forName("policy-test-3").build());
        second.shutdownNow();
    }

    @Test
    void failedOpenReleasesTheChannelLease() {
        OutboundChannelPolicy policy = OutboundChannelPolicy.builder()
                .maxActiveChannels(1).build();
        assertThatThrownBy(() -> policy.open("localhost:9090", false,
                target -> { throw new IllegalStateException("factory failed"); }))
                .isInstanceOf(IllegalStateException.class);
        ManagedChannel channel = policy.open("localhost:9090", false,
                target -> InProcessChannelBuilder.forName("policy-test-4").build());
        channel.shutdownNow();
    }

    @Test
    void leasedChannelsDelegateOptionalManagedChannelOperations() {
        OutboundChannelPolicy policy = OutboundChannelPolicy.defaults();
        ManagedChannel channel = policy.open("localhost:9090", false,
                target -> InProcessChannelBuilder.forName("policy-test-5").build());
        try {
            assertThat(channel.getState(false)).isNotNull();
            channel.resetConnectBackoff();
            channel.enterIdle();
        } finally {
            channel.shutdownNow();
        }
    }
}
