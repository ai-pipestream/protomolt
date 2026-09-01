package ai.pipestream.proto.schema.confluent;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/**
 * Apicurio Registry 3.x as a Testcontainer, modeled on Quarkus dev services'
 * ApicurioRegistryContainer: the registry listens on 8080 inside the container and runs
 * under the prod Quarkus profile. No official testcontainers module exists for Apicurio,
 * so a plain GenericContainer it is. The wait probes the native v3 API directly: the 3.3.x
 * image dropped the /health/ready endpoint the compose stack's 3.0.13 healthcheck polls,
 * while system/info answers 200 once the app serves either facade (v3 or ccompat).
 */
final class ApicurioRegistryContainer extends GenericContainer<ApicurioRegistryContainer> {

    static final String IMAGE = "apicurio/apicurio-registry:3.3.2";

    private static final int REGISTRY_PORT = 8080; // inside the container

    ApicurioRegistryContainer() {
        super(DockerImageName.parse(IMAGE));
        withExposedPorts(REGISTRY_PORT);
        withEnv("QUARKUS_PROFILE", "prod");
        waitingFor(Wait.forHttp("/apis/registry/v3/system/info").forPort(REGISTRY_PORT));
    }

    /** Base URL on the host; append {@code /apis/registry/v3} or {@code /apis/ccompat/v7}. */
    String getUrl() {
        return String.format("http://%s:%s", getHost(), getMappedPort(REGISTRY_PORT));
    }
}
