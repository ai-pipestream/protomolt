package ai.protomolt.proto.schema.confluent;

import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.redpanda.RedpandaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Runs the Confluent-compat suite against a Testcontainers Redpanda, which serves the
 * Confluent Schema Registry API on 8081; the suite skips when Docker is unavailable.
 */
@Testcontainers(disabledWithoutDocker = true)
class ConfluentSchemaRegistryIntegrationTest extends AbstractConfluentCompatIntegrationTest {

    // Protobuf schema references (which this suite registers) were only added to Redpanda's
    // registry after the connector lane's v22.2.1 baseline; this is the tag
    // docker-compose.integration.yml pins for its Redpanda.
    @Container
    static final RedpandaContainer REGISTRY = new RedpandaContainer(
            DockerImageName.parse("docker.redpanda.com/redpandadata/redpanda:v24.3.11"));

    @Override
    String registryBaseUrl() {
        return REGISTRY.getSchemaRegistryAddress();
    }
}
