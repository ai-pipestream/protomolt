package ai.pipestream.proto.server;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Normalization and validation corners of {@link ProtoToolsServerConfig} beyond the
 * defaults covered by {@code ProtoToolsServerConfigTest}.
 */
class ProtoToolsServerConfigNormalizationTest {

    @Test
    void nullComponentsFallBackToDefaults() {
        ProtoToolsServerConfig config = new ProtoToolsServerConfig(null, 1, null, null, null);
        assertThat(config.host()).isEqualTo("0.0.0.0");
        assertThat(config.restPathPrefix()).isEqualTo("/grpc-json");
        assertThat(config.openApiPath()).isEqualTo("/openapi.json");
        assertThat(config.healthPath()).isEqualTo("/health");
    }

    @Test
    void prefixNormalizationAddsLeadingSlashAndStripsTrailingSlashes() {
        assertThat(new ProtoToolsServerConfig("h", 1, "api", "/o", "/x").restPathPrefix())
                .isEqualTo("/api");
        assertThat(new ProtoToolsServerConfig("h", 1, "/api//", "/o", "/x").restPathPrefix())
                .isEqualTo("/api");
        // The root path is preserved as-is rather than stripped to empty.
        assertThat(new ProtoToolsServerConfig("h", 1, "/", "/o", "/x").restPathPrefix())
                .isEqualTo("/");
    }

    @Test
    void openApiAndHealthPathsAreNormalizedLikePrefixes() {
        ProtoToolsServerConfig config =
                new ProtoToolsServerConfig("h", 1, "/p", "openapi.json/", "health/", 1024);
        assertThat(config.openApiPath()).isEqualTo("/openapi.json");
        assertThat(config.healthPath()).isEqualTo("/health");
        assertThat(config.maxRequestBytes()).isEqualTo(1024);
    }

    @Test
    void portBoundariesAreValidated() {
        assertThat(new ProtoToolsServerConfig("h", 0, "/p", "/o", "/x").port()).isZero();
        assertThat(new ProtoToolsServerConfig("h", 65535, "/p", "/o", "/x").port()).isEqualTo(65535);
        assertThatThrownBy(() -> new ProtoToolsServerConfig("h", 65536, "/p", "/o", "/x"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("65536");
    }

    @Test
    void negativeMaxRequestBytesRejected() {
        assertThatThrownBy(() -> ProtoToolsServerConfig.defaults().withMaxRequestBytes(-1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("-1");
    }

    @Test
    void withersPreserveUntouchedFields() {
        ProtoToolsServerConfig base = new ProtoToolsServerConfig("h", 1234, "/api", "/spec.json", "/live", 64);

        ProtoToolsServerConfig rehosted = base.withHost("example.com");
        assertThat(rehosted.host()).isEqualTo("example.com");
        assertThat(rehosted.port()).isEqualTo(1234);
        assertThat(rehosted.restPathPrefix()).isEqualTo("/api");
        assertThat(rehosted.openApiPath()).isEqualTo("/spec.json");
        assertThat(rehosted.healthPath()).isEqualTo("/live");
        assertThat(rehosted.maxRequestBytes()).isEqualTo(64);

        ProtoToolsServerConfig reprefixed = base.withRestPathPrefix("v2/");
        assertThat(reprefixed.restPathPrefix()).isEqualTo("/v2");
        assertThat(reprefixed.host()).isEqualTo("h");
        assertThat(reprefixed.port()).isEqualTo(1234);
    }

    @Test
    void fiveArgConstructorUsesDefaultMaxRequestBytes() {
        ProtoToolsServerConfig config = new ProtoToolsServerConfig("h", 1, "/p", "/o", "/x");
        assertThat(config.maxRequestBytes()).isEqualTo(ProtoToolsServerConfig.DEFAULT_MAX_REQUEST_BYTES);
    }

    @Test
    void recordEqualityFollowsAllComponents() {
        ProtoToolsServerConfig a = new ProtoToolsServerConfig("h", 1, "/p", "/o", "/x", 10);
        ProtoToolsServerConfig same = new ProtoToolsServerConfig("h", 1, "/p", "/o", "/x", 10);
        ProtoToolsServerConfig different = new ProtoToolsServerConfig("h", 2, "/p", "/o", "/x", 10);
        assertThat(a).isEqualTo(same).hasSameHashCodeAs(same);
        assertThat(a).isNotEqualTo(different);
    }
}
