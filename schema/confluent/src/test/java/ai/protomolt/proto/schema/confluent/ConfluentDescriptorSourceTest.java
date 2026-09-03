package ai.protomolt.proto.schema.confluent;

import ai.protomolt.proto.descriptors.DescriptorLoader;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ConfluentDescriptorSourceTest {

    @Test
    void reportsLoaderType() {
        assertThat(new ConfluentDescriptorSource("confluent/demo.fds").getLoaderType())
                .contains("Confluent");
    }

    @Test
    void loadsClasspathDescriptorSet() throws Exception {
        ConfluentDescriptorSource source = new ConfluentDescriptorSource("confluent/demo.fds");
        assertThat(source.isAvailable()).isTrue();
        assertThat(source.loadDescriptors()).isNotEmpty();
        assertThat(source.loadDescriptor("demo.proto")).isNotNull();
        assertThat(source.loadDescriptor("missing.proto")).isNull();
    }

    @Test
    void missingClasspathResourceFails() {
        ConfluentDescriptorSource source = new ConfluentDescriptorSource("confluent/does-not-exist.fds");
        assertThat(source.isAvailable()).isFalse();
        assertThatThrownBy(source::loadDescriptors)
                .isInstanceOf(DescriptorLoader.DescriptorLoadException.class)
                .hasMessageContaining("Classpath descriptor set not found")
                .hasNoCause();
    }

    @Test
    void loadsFromHttpEndpoint() throws Exception {
        byte[] payload = ConfluentDescriptorSourceTest.class
                .getClassLoader()
                .getResourceAsStream("confluent/demo.fds")
                .readAllBytes();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/fds", exchange -> {
            exchange.sendResponseHeaders(200, payload.length);
            exchange.getResponseBody().write(payload);
            exchange.close();
        });
        server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        server.start();
        try {
            URI uri = URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "/fds");
            ConfluentDescriptorSource source = new ConfluentDescriptorSource(uri);
            assertThat(source.isAvailable()).isTrue();
            assertThat(source.loadDescriptors()).isNotEmpty();
        } finally {
            server.stop(0);
        }
    }

    @Test
    void httpErrorBecomesLoadException() throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/fds", exchange -> {
            exchange.sendResponseHeaders(500, 0);
            exchange.close();
        });
        server.start();
        try {
            URI uri = URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "/fds");
            assertThatThrownBy(() -> new ConfluentDescriptorSource(uri).loadDescriptors())
                    .isInstanceOf(DescriptorLoader.DescriptorLoadException.class)
                    .hasMessageContaining("HTTP 500")
                    .hasNoCause();
        } finally {
            server.stop(0);
        }
    }

    @Test
    void ioFailureDoesNotSetInterruptFlag() throws IOException {
        byte[] truncated = {0x0A, 0x05}; // claims a 5-byte nested message, then EOF
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/fds", exchange -> {
            exchange.sendResponseHeaders(200, truncated.length);
            exchange.getResponseBody().write(truncated);
            exchange.close();
        });
        server.start();
        try {
            URI uri = URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "/fds");
            assertThatThrownBy(() -> new ConfluentDescriptorSource(uri).loadDescriptors())
                    .isInstanceOf(DescriptorLoader.DescriptorLoadException.class)
                    .hasMessageContaining("Failed to load Confluent descriptor set");
            assertThat(Thread.interrupted())
                    .as("IOException must not set the interrupt flag")
                    .isFalse();
        } finally {
            server.stop(0);
        }
    }
}
