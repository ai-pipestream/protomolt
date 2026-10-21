package ai.protomolt.proto.grpc.policy;

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

    @Test
    void validatesDnsLabelIpv4AndIpv6EdgeCases() {
        OutboundChannelPolicy policy = OutboundChannelPolicy.defaults();

        // Valid DNS label boundaries: 1-char, 63-char, and interior hyphens.
        assertThat(policy.validateTarget("a:443", true).host()).isEqualTo("a");
        String maxLabel = "a" + "b".repeat(61) + "9";
        assertThat(policy.validateTarget(maxLabel + ".example-1.com:443", true).host())
                .isEqualTo(maxLabel + ".example-1.com");

        // Invalid DNS labels: 64-char label, leading/trailing hyphen, empty labels, underscore, non-ASCII.
        for (String invalidDns : new String[] {
                "a".repeat(64) + ":443",
                "-bad.example:443",
                "bad-.example:443",
                "example.com.:443",
                ".example.com:443",
                "a..b.com:443",
                "a_b.example.com:443",
                "éxample.com:443"
        }) {
            assertThatThrownBy(() -> policy.validateTarget(invalidDns, true))
                    .as("invalid DNS target %s", invalidDns)
                    .isInstanceOf(OutboundChannelPolicyException.class);
        }

        // Valid IPv4 boundaries and looksLikeIpv4Literal classification.
        assertThat(policy.validateTarget("0.0.0.0:443", true).scheme()).isEqualTo("ipv4");
        assertThat(policy.validateTarget("255.255.255.255:443", true).scheme()).isEqualTo("ipv4");
        for (String sample : new String[] {
                "127.0.0.1", "0.0.0.0", "999.999.999.999", "1.2.3", "1.2.3.4.5",
                "1.2.3.", ".1.2.3", "1..2.3", "1.2.3.a", "\u0661.\u0662.\u0663.\u0664", ""
        }) {
            assertThat(OutboundChannelPolicy.looksLikeIpv4Literal(sample))
                    .as("looksLikeIpv4Literal(%s)", sample)
                    .isEqualTo(sample.matches("\\d+(?:\\.\\d+){3}"));
        }
        for (String invalidIpv4 : new String[] {
                "ipv4:1.2.3:443",
                "ipv4:1.2.3.4.5:443",
                "ipv4:1.2.3.:443",
                "ipv4:.1.2.3:443",
                "ipv4:1..2.3:443",
                "ipv4:256.0.0.1:443",
                "ipv4:1.2.3.1000:443",
                "ipv4:1.2.3.4a:443"
        }) {
            assertThatThrownBy(() -> policy.validateTarget(invalidIpv4, true))
                    .as("invalid IPv4 target %s", invalidIpv4)
                    .isInstanceOf(OutboundChannelPolicyException.class);
        }

        // Valid and invalid IPv6 group boundaries.
        assertThat(policy.validateTarget("[2001:0db8:0000:0000:0000:ff00:0042:8329]:443", true).scheme())
                .isEqualTo("ipv6");
        assertThat(policy.validateTarget("[2001:db8::1]:443", true).scheme()).isEqualTo("ipv6");
        assertThat(policy.validateTarget("[::]:443", true).scheme()).isEqualTo("ipv6");
        assertThat(policy.validateTarget("[1::]:443", true).scheme()).isEqualTo("ipv6");
        for (String invalidIpv6 : new String[] {
                "[1:2:3:4:5:6:7]:443",
                "[1:2:3:4:5:6:7:8:9]:443",
                "[1:2:3:4:5:6:7::8]:443",
                "[1::2:]:443",
                "[:1::2]:443",
                "[12345::1]:443",
                "[1::g]:443"
        }) {
            assertThatThrownBy(() -> policy.validateTarget(invalidIpv6, true))
                    .as("invalid IPv6 target %s", invalidIpv6)
                    .isInstanceOf(OutboundChannelPolicyException.class);
        }
    }
}
