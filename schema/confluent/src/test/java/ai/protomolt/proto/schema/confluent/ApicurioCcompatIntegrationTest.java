package ai.protomolt.proto.schema.confluent;

import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Runs the Confluent-compat suite against Apicurio Registry's ccompat v7 facade, served by a
 * Testcontainers Apicurio Registry; the suite skips when Docker is unavailable.
 *
 * <p>To run against an external registry instead (for example the compose stack's), override
 * with {@code -Dpipestream.it.apicurio.url=...} or env {@code PIPESTREAM_IT_APICURIO_URL};
 * the {@code /apis/ccompat/v7} path is appended here.</p>
 */
@Testcontainers(disabledWithoutDocker = true)
class ApicurioCcompatIntegrationTest extends AbstractConfluentCompatIntegrationTest {

    @Container
    static final ApicurioRegistryContainer REGISTRY = new ApicurioRegistryContainer();

    @Override
    String registryBaseUrl() {
        return configuredUrl("pipestream.it.apicurio.url", "PIPESTREAM_IT_APICURIO_URL",
                REGISTRY.getUrl()) + "/apis/ccompat/v7";
    }
}
