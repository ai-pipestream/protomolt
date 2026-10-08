package ai.protomolt.proto.schema.apicurio;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Consumer;

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
    private final BoundedLogTail logTail = new BoundedLogTail(64 * 1024);

    ApicurioRegistryContainer() {
        super(DockerImageName.parse(IMAGE));
        withExposedPorts(REGISTRY_PORT);
        withEnv("QUARKUS_PROFILE", "prod");
        waitingFor(Wait.forHttp("/apis/registry/v3/system/info").forPort(REGISTRY_PORT));
        withLogConsumer(logTail);
    }

    /** Base URL on the host; append {@code /apis/registry/v3} or {@code /apis/ccompat/v7}. */
    String getUrl() {
        return String.format("http://%s:%s", getHost(), getMappedPort(REGISTRY_PORT));
    }

    void writeBoundedLogTail(Path destination) throws IOException {
        logTail.writeTo(destination);
    }

    /** Retains only the last {@code capacity} bytes of container output. */
    static final class BoundedLogTail implements Consumer<OutputFrame> {
        private final byte[] bytes;
        private long received;

        BoundedLogTail(int capacity) {
            if (capacity <= 0) {
                throw new IllegalArgumentException("capacity must be positive");
            }
            bytes = new byte[capacity];
        }

        @Override
        public synchronized void accept(OutputFrame frame) {
            if (frame == null || frame.getBytes() == null) {
                return;
            }
            byte[] incoming = frame.getBytes();
            for (byte value : incoming) {
                bytes[(int) (received % bytes.length)] = value;
                received++;
            }
        }

        synchronized void writeTo(Path destination) throws IOException {
            int length = (int) Math.min(received, bytes.length);
            byte[] tail = new byte[length];
            long first = received - length;
            for (int i = 0; i < length; i++) {
                tail[i] = bytes[(int) ((first + i) % bytes.length)];
            }
            Path parent = destination.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            Files.write(destination, tail);
        }
    }
}
