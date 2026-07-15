package ai.pipestream.proto.registry.server;

import ai.pipestream.proto.registry.SchemaReference;
import ai.pipestream.proto.registry.SchemaRegistryStore;
import ai.pipestream.proto.registry.StoredSchema;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A backend failure must never echo its exception text to clients — messages can name
 * filesystem paths, Git remotes, or credentials. Clients get a stable error code and a
 * correlation id; the detail belongs in the server log.
 */
class InternalErrorSanitizationTest {

    private static final String SECRET_DETAIL =
            "/very/secret/path/schemas.git credential=hunter2";

    /** A store whose reads blow up with sensitive detail in the message. */
    private static final class ExplodingStore implements SchemaRegistryStore {
        @Override
        public List<String> subjects() {
            throw new IllegalStateException(SECRET_DETAIL);
        }

        @Override
        public List<Integer> versions(String subject) {
            throw new IllegalStateException(SECRET_DETAIL);
        }

        @Override
        public Optional<StoredSchema> version(String subject, int version) {
            throw new IllegalStateException(SECRET_DETAIL);
        }

        @Override
        public Optional<StoredSchema> latest(String subject) {
            throw new IllegalStateException(SECRET_DETAIL);
        }

        @Override
        public Optional<StoredSchema> byGlobalId(int globalId) {
            throw new IllegalStateException(SECRET_DETAIL);
        }

        @Override
        public Optional<StoredSchema> findByContent(String subject, String schemaText,
                                                    List<SchemaReference> references) {
            throw new IllegalStateException(SECRET_DETAIL);
        }

        @Override
        public StoredSchema register(String subject, String schemaText,
                                     List<SchemaReference> references) {
            throw new IllegalStateException(SECRET_DETAIL);
        }

        @Override
        public Optional<String> compatibilityMode(String subject) {
            throw new IllegalStateException(SECRET_DETAIL);
        }

        @Override
        public void setCompatibilityMode(String subject, String mode) {
            throw new IllegalStateException(SECRET_DETAIL);
        }

        @Override
        public String globalCompatibilityMode() {
            throw new IllegalStateException(SECRET_DETAIL);
        }

        @Override
        public void setGlobalCompatibilityMode(String mode) {
            throw new IllegalStateException(SECRET_DETAIL);
        }

        @Override
        public void close() {
        }
    }

    @Test
    void backendFailuresAreGenericWithACorrelationId() throws Exception {
        SchemaRegistryServer server = new SchemaRegistryServer(
                SchemaRegistryServerConfig.defaults().withHost("127.0.0.1").withPort(0),
                new ExplodingStore());
        int port = server.start();
        try (HttpClient client = HttpClient.newHttpClient()) {
            HttpResponse<String> response = client.send(
                    HttpRequest.newBuilder(URI.create(
                            "http://127.0.0.1:" + port + "/subjects")).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            assertThat(response.statusCode()).isEqualTo(500);
            assertThat(response.body())
                    .contains("correlation id")
                    .contains("50001")
                    .doesNotContain("secret")
                    .doesNotContain("hunter2")
                    .doesNotContain("IllegalStateException");
        } finally {
            server.close();
        }
    }
}
