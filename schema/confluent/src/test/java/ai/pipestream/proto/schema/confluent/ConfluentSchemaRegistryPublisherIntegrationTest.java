package ai.pipestream.proto.schema.confluent;

/**
 * Runs the Confluent publisher suite against a real Confluent Schema Registry.
 *
 * <p>Override with {@code -Dpipestream.it.confluent.url=...} or env
 * {@code PIPESTREAM_IT_CONFLUENT_URL} (default {@code http://localhost:18781}).</p>
 */
class ConfluentSchemaRegistryPublisherIntegrationTest extends AbstractConfluentPublisherIntegrationTest {

    @Override
    String registryBaseUrl() {
        return AbstractConfluentCompatIntegrationTest.configuredUrl(
                "pipestream.it.confluent.url", "PIPESTREAM_IT_CONFLUENT_URL",
                "http://localhost:18781");
    }
}
