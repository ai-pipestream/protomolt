package ai.pipestream.proto.registry.server;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Validation of {@link SchemaRegistryServerConfig}.
 */
class SchemaRegistryServerConfigTest {

    @Test
    void aPrefixIsNormalizedToOneLeadingSlashWithNoTrailingSlash() {
        assertThat(SchemaRegistryServerConfig.defaults().nativePathPrefix()).isEqualTo("/protomolt");
        assertThat(new SchemaRegistryServerConfig("127.0.0.1", 0, "/health", "extras/",
                SchemaRegistryServerConfig.DEFAULT_MAX_REQUEST_BYTES).nativePathPrefix())
                .isEqualTo("/extras");
    }

    /**
     * The router compares the prefix against a single decoded path segment, so a nested prefix
     * would be accepted and then match nothing — every native endpoint would 404 with no
     * indication that the configuration was at fault.
     */
    @Test
    void aNestedOrEmptyNativePathPrefixIsRejected() {
        for (String prefix : new String[]{"/api/protomolt", "api/protomolt", "/", ""}) {
            assertThatThrownBy(() -> new SchemaRegistryServerConfig("127.0.0.1", 0, "/health",
                    prefix, SchemaRegistryServerConfig.DEFAULT_MAX_REQUEST_BYTES))
                    .as(prefix)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("nativePathPrefix must be a single path segment");
        }
    }
}
