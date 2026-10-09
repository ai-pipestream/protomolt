package ai.protomolt.proto.repo.container.ledger;

import java.net.URI;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.localstack.LocalStackContainer;

/** Explicit real-provider selection for the native production-JAR correctness gate. */
final class AssessmentStorageBackend implements AutoCloseable {
    private static final String RUSTFS_IMAGE = "rustfs/rustfs:1.0.0-beta.11-preview.1";
    private final String kind;
    private final GenericContainer<?> container;
    private final LocalStackContainer localstack;

    AssessmentStorageBackend(String kind) {
        this.kind = kind;
        switch (kind) {
            case "localstack" -> {
                localstack = new LocalStackContainer("localstack/localstack:3.8").withServices("s3");
                container = localstack;
            }
            case "rustfs" -> {
                localstack = null;
                container = new GenericContainer<>(RUSTFS_IMAGE).withCommand("/data")
                        .withEnv("RUSTFS_VOLUMES", "/data").withEnv("RUSTFS_ADDRESS", ":9000")
                        .withEnv("RUSTFS_CONSOLE_ENABLE", "false")
                        .withEnv("RUSTFS_ACCESS_KEY", "native-test")
                        .withEnv("RUSTFS_SECRET_KEY", "native-test-secret")
                        .withExposedPorts(9000).waitingFor(Wait.forHttp("/health").forPort(9000));
            }
            default -> throw new IllegalArgumentException("Unsupported native test storage: " + kind);
        }
    }

    void start() {
        container.start();
        System.out.println("NATIVE_STORAGE_BACKEND=" + kind + " image=" + container.getDockerImageName()
                + " image_id=" + container.getContainerInfo().getImageId());
    }
    URI getEndpoint() {
        return localstack != null ? localstack.getEndpoint()
                : URI.create("http://" + container.getHost() + ":" + container.getMappedPort(9000));
    }
    String getRegion() { return localstack != null ? localstack.getRegion() : "us-east-1"; }
    String getAccessKey() { return localstack != null ? localstack.getAccessKey() : "native-test"; }
    String getSecretKey() { return localstack != null ? localstack.getSecretKey() : "native-test-secret"; }
    @Override public void close() { container.close(); }
}
