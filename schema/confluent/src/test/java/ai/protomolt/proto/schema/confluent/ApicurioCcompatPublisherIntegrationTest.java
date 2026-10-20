package ai.protomolt.proto.schema.confluent;

import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Runs the Confluent publisher suite against Apicurio Registry's ccompat v7 facade, served by
 * a Testcontainers Apicurio Registry; Docker is required.
 *
 * <p>To run against an external registry instead (for example the compose stack's), override
 * with {@code -Dprotomolt.it.apicurio.url=...} or env {@code PROTOMOLT_IT_APICURIO_URL};
 * the {@code /apis/ccompat/v7} path is appended here.</p>
 */
@Testcontainers
class ApicurioCcompatPublisherIntegrationTest extends AbstractConfluentPublisherIntegrationTest {

    @Container
    static final ApicurioRegistryContainer REGISTRY = new ApicurioRegistryContainer();

    @Override
    String registryBaseUrl() {
        return AbstractConfluentCompatIntegrationTest.configuredUrl(
                "protomolt.it.apicurio.url", "PROTOMOLT_IT_APICURIO_URL",
                REGISTRY.getUrl()) + "/apis/ccompat/v7";
    }
}
