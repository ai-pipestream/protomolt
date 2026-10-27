package ai.protomolt.proto.repo.service.eval;

import java.net.URI;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

/**
 * The deployment's object store for the evaluation: the same RustFS image the
 * compose files pin, not LocalStack, so the host talks to the S3 implementation
 * it runs against in production.
 */
final class RustFsContainer extends GenericContainer<RustFsContainer> {
    /** Keep in step with deploy/document-platform/compose-roles.yml. */
    static final String IMAGE = "rustfs/rustfs:1.0.0-beta.11-preview.1";

    RustFsContainer() {
        super(IMAGE);
        withCommand("/data");
        withEnv("RUSTFS_VOLUMES", "/data");
        withEnv("RUSTFS_ADDRESS", ":9000");
        withEnv("RUSTFS_CONSOLE_ENABLE", "false");
        withEnv("RUSTFS_ACCESS_KEY", accessKey());
        withEnv("RUSTFS_SECRET_KEY", secretKey());
        withExposedPorts(9000);
        waitingFor(Wait.forHttp("/health").forPort(9000));
    }

    URI endpoint() {
        return URI.create("http://" + getHost() + ":" + getMappedPort(9000));
    }

    String region() {
        return "us-east-1";
    }

    String accessKey() {
        return "eval-access-key";
    }

    String secretKey() {
        return "eval-secret-key-value";
    }
}
