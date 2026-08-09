package ai.pipestream.proto.registry.server;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Validation and normalization of {@link SchemaRegistryServerConfig} beyond
 * the native-path-prefix rules pinned by {@link SchemaRegistryServerConfigTest}:
 * port range, body cap, host/health-path normalization, the token default, and
 * the withers.
 */
class SchemaRegistryServerConfigValidationTest {

    @Test
    void defaultsMatchTheDocumentedConventions() {
        SchemaRegistryServerConfig config = SchemaRegistryServerConfig.defaults();
        assertThat(config.host()).isEqualTo("0.0.0.0");
        assertThat(config.port()).isEqualTo(8081);
        assertThat(config.healthPath()).isEqualTo("/health");
        assertThat(config.nativePathPrefix()).isEqualTo("/protomolt");
        assertThat(config.maxRequestBytes())
                .isEqualTo(SchemaRegistryServerConfig.DEFAULT_MAX_REQUEST_BYTES);
        assertThat(config.apiToken()).isNull();
    }

    @Test
    void aPortOutsideTheValidRangeIsRejected() {
        for (int port : new int[]{-1, 65536, Integer.MAX_VALUE}) {
            assertThatThrownBy(() -> new SchemaRegistryServerConfig("127.0.0.1", port, "/health",
                    "/protomolt", SchemaRegistryServerConfig.DEFAULT_MAX_REQUEST_BYTES))
                    .as("port " + port)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("port out of range");
        }
        // Both boundaries are legal.
        new SchemaRegistryServerConfig("127.0.0.1", 0, "/health", "/protomolt", 1024);
        new SchemaRegistryServerConfig("127.0.0.1", 65535, "/health", "/protomolt", 1024);
    }

    @Test
    void aNonPositiveBodyCapIsRejected() {
        for (int cap : new int[]{0, -1}) {
            assertThatThrownBy(() -> new SchemaRegistryServerConfig("127.0.0.1", 0, "/health",
                    "/protomolt", cap))
                    .as("maxRequestBytes " + cap)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("maxRequestBytes must be positive");
        }
    }

    @Test
    void aBlankHostFallsBackToTheWildcard() {
        assertThat(new SchemaRegistryServerConfig(null, 0, "/health", "/protomolt", 1024).host())
                .isEqualTo("0.0.0.0");
        assertThat(new SchemaRegistryServerConfig("  ", 0, "/health", "/protomolt", 1024).host())
                .isEqualTo("0.0.0.0");
    }

    @Test
    void theHealthPathIsNormalizedLikeThePrefix() {
        assertThat(new SchemaRegistryServerConfig("h", 0, null, "/protomolt", 1024).healthPath())
                .isEqualTo("/health");
        assertThat(new SchemaRegistryServerConfig("h", 0, "livez", "/protomolt", 1024).healthPath())
                .isEqualTo("/livez");
        assertThat(new SchemaRegistryServerConfig("h", 0, "/livez/", "/protomolt", 1024).healthPath())
                .isEqualTo("/livez");
    }

    @Test
    void aBlankApiTokenMeansUnauthenticated() {
        assertThat(new SchemaRegistryServerConfig("h", 0, "/health", "/protomolt", 1024, null)
                .apiToken()).isNull();
        assertThat(new SchemaRegistryServerConfig("h", 0, "/health", "/protomolt", 1024, "  ")
                .apiToken()).isNull();
        assertThat(new SchemaRegistryServerConfig("h", 0, "/health", "/protomolt", 1024, "sekret")
                .apiToken()).isEqualTo("sekret");
    }

    @Test
    void theWithersChangeOnlyTheirOwnField() {
        SchemaRegistryServerConfig base = SchemaRegistryServerConfig.defaults()
                .withHost("127.0.0.1")
                .withPort(0)
                .withApiToken("sekret");
        assertThat(base.host()).isEqualTo("127.0.0.1");
        assertThat(base.port()).isZero();
        assertThat(base.apiToken()).isEqualTo("sekret");
        // Untouched fields survive the wither chain.
        assertThat(base.healthPath()).isEqualTo("/health");
        assertThat(base.nativePathPrefix()).isEqualTo("/protomolt");
        assertThat(base.maxRequestBytes())
                .isEqualTo(SchemaRegistryServerConfig.DEFAULT_MAX_REQUEST_BYTES);

        SchemaRegistryServerConfig rehosted = base.withHost("0.0.0.0");
        assertThat(rehosted.host()).isEqualTo("0.0.0.0");
        assertThat(rehosted.apiToken()).isEqualTo("sekret");
    }
}
