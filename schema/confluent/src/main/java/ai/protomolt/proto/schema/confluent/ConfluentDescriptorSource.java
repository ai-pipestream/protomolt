package ai.protomolt.proto.schema.confluent;

import ai.protomolt.proto.descriptors.DescriptorLoader;
import ai.protomolt.proto.descriptors.GoogleDescriptorLoader;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.FileDescriptor;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Objects;

/**
 * Loads a pre-compiled binary protobuf FileDescriptorSet served over plain HTTP or from the
 * classpath (it does <em>not</em> speak the Schema Registry subjects REST API).
 *
 * <p>Confluent Schema Registry's native protobuf APIs return schema text and
 * references; to load those directly from a registry, use
 * {@link ConfluentSchemaRegistryLoader}. This source remains the right choice for
 * endpoints that supply a compiled FileDescriptorSet, or for a classpath
 * descriptor-set export produced from the registered schema.</p>
 *
 * <p>HTTP requests use configurable timeouts (defaults: 10s connect, 30s per request), so an
 * unresponsive endpoint cannot hang startup forever. The source owns its {@link HttpClient};
 * call {@link #close()} when done with an HTTP-backed instance (a no-op for classpath-backed
 * instances).</p>
 */
public final class ConfluentDescriptorSource implements DescriptorLoader, AutoCloseable {

    /** Default connect timeout for the HTTP client. */
    public static final Duration DEFAULT_CONNECT_TIMEOUT = Duration.ofSeconds(10);

    /** Default per-request timeout. */
    public static final Duration DEFAULT_REQUEST_TIMEOUT = Duration.ofSeconds(30);

    private final URI endpoint;
    private final String classpathResource;
    private final HttpClient client;
    private final Duration requestTimeout;

    public ConfluentDescriptorSource(URI endpoint) {
        this(endpoint, DEFAULT_CONNECT_TIMEOUT, DEFAULT_REQUEST_TIMEOUT);
    }

    /**
     * Creates an HTTP-backed source with explicit timeouts.
     *
     * @param endpoint URL serving a binary {@code FileDescriptorSet}
     * @param connectTimeout TCP connect timeout for the underlying {@link HttpClient}
     * @param requestTimeout per-request timeout
     */
    public ConfluentDescriptorSource(URI endpoint, Duration connectTimeout, Duration requestTimeout) {
        this.endpoint = Objects.requireNonNull(endpoint, "endpoint");
        this.classpathResource = null;
        this.requestTimeout = Objects.requireNonNull(requestTimeout, "requestTimeout");
        this.client = HttpClient.newBuilder()
                .connectTimeout(Objects.requireNonNull(connectTimeout, "connectTimeout"))
                .build();
    }

    public ConfluentDescriptorSource(String classpathResource) {
        this.endpoint = null;
        this.classpathResource = Objects.requireNonNull(classpathResource, "classpathResource");
        this.requestTimeout = DEFAULT_REQUEST_TIMEOUT;
        this.client = null;
    }

    @Override
    public List<FileDescriptor> loadDescriptors() throws DescriptorLoadException {
        try {
            return GoogleDescriptorLoader.fromDescriptorSet(FileDescriptorSet.parseFrom(readBytes()));
        } catch (DescriptorLoadException e) {
            throw e;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new DescriptorLoadException("Failed to load Confluent descriptor set", e);
        } catch (IOException e) {
            throw new DescriptorLoadException("Failed to load Confluent descriptor set", e);
        } catch (Exception e) {
            throw new DescriptorLoadException("Invalid Confluent descriptor set", e);
        }
    }

    @Override
    public FileDescriptor loadDescriptor(String fileName) throws DescriptorLoadException {
        return loadDescriptors().stream().filter(descriptor -> descriptor.getName().equals(fileName)).findFirst().orElse(null);
    }

    @Override
    public boolean isAvailable() {
        return endpoint != null || Thread.currentThread().getContextClassLoader().getResource(classpathResource) != null;
    }

    @Override
    public String getLoaderType() {
        return "Confluent Schema Registry descriptor set";
    }

    /** Closes the underlying {@link HttpClient} (no-op for classpath-backed sources). */
    @Override
    public void close() {
        if (client != null) {
            client.close();
        }
    }

    private byte[] readBytes() throws IOException, InterruptedException, DescriptorLoadException {
        if (endpoint != null) {
            HttpRequest request = HttpRequest.newBuilder(endpoint)
                    .timeout(requestTimeout)
                    .GET()
                    .build();
            HttpResponse<byte[]> response = client.send(request, HttpResponse.BodyHandlers.ofByteArray());
            if (response.statusCode() / 100 != 2) {
                throw new DescriptorLoadException("Confluent endpoint returned HTTP " + response.statusCode());
            }
            return response.body();
        }
        try (InputStream input = Thread.currentThread().getContextClassLoader().getResourceAsStream(classpathResource)) {
            if (input == null) {
                throw new DescriptorLoadException("Classpath descriptor set not found: " + classpathResource);
            }
            return input.readAllBytes();
        }
    }
}
