package ai.protomolt.proto.repo.service;

import java.net.URI;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

/** Disposable local-performance store, using the repository's existing deployment pin. */
final class RustFsBenchmarkStore extends GenericContainer<RustFsBenchmarkStore> {
    static final String IMAGE = "rustfs/rustfs:1.0.0-beta.11-preview.1";

    RustFsBenchmarkStore() {
        super(IMAGE);
        withCommand("/data");
        withEnv("RUSTFS_VOLUMES", "/data");
        withEnv("RUSTFS_ADDRESS", ":9000");
        withEnv("RUSTFS_CONSOLE_ENABLE", "false");
        withEnv("RUSTFS_ACCESS_KEY", getAccessKey());
        withEnv("RUSTFS_SECRET_KEY", getSecretKey());
        withExposedPorts(9000);
        waitingFor(Wait.forHttp("/health").forPort(9000));
    }

    URI getEndpoint() { return URI.create("http://" + getHost() + ":" + getMappedPort(9000)); }
    String getRegion() { return "us-east-1"; }
    String getAccessKey() { return "benchmark-test"; }
    String getSecretKey() { return "benchmark-test-secret"; }

    String environment() {
        var info = getContainerInfo();
        var mounts = info.getMounts();
        return "object_store=RustFS\nobject_store_image=" + IMAGE
                + "\nobject_store_image_id=" + info.getImageId()
                + "\nobject_store_endpoint=" + getEndpoint()
                + "\nobject_store_mounts=" + (mounts == null ? "[]" : mounts.stream()
                        .map(mount -> "source=" + mount.getSource() + ",destination=" + mount.getDestination()
                                + ",driver=" + mount.getDriver() + ",writable=" + mount.getRW()).toList())
                + "\nNo explicit CPU/memory limit or production disk topology is configured for the store.\n";
    }
}
