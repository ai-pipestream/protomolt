package ai.pipestream.proto.registry.server;

import ai.pipestream.proto.registry.SchemaReference;
import ai.pipestream.proto.registry.SchemaRegistryStore;
import ai.pipestream.proto.registry.StoredSchema;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * With an API token configured, the registry sits behind the same boundary as the other
 * operational surfaces: every route requires the shared secret except health. The registry
 * carries schema, config, and chain writes plus action execution — it must not be the one
 * listener an operator forgets.
 */
class TokenAuthTest {

    private static final String TOKEN = "reg-sekret";

    private static SchemaRegistryServer server;
    private static HttpClient http;
    private static String base;

    /** An empty but functional store. */
    private static final class EmptyStore implements SchemaRegistryStore {
        @Override
        public List<String> subjects() {
            return List.of();
        }

        @Override
        public List<Integer> versions(String subject) {
            return List.of();
        }

        @Override
        public Optional<StoredSchema> version(String subject, int version) {
            return Optional.empty();
        }

        @Override
        public Optional<StoredSchema> latest(String subject) {
            return Optional.empty();
        }

        @Override
        public Optional<StoredSchema> byGlobalId(int globalId) {
            return Optional.empty();
        }

        @Override
        public Optional<StoredSchema> findByContent(String subject, String schemaText,
                                                    List<SchemaReference> references) {
            return Optional.empty();
        }

        @Override
        public StoredSchema register(String subject, String schemaText,
                                     List<SchemaReference> references) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<String> compatibilityMode(String subject) {
            return Optional.empty();
        }

        @Override
        public void setCompatibilityMode(String subject, String mode) {
        }

        @Override
        public String globalCompatibilityMode() {
            return "BACKWARD";
        }

        @Override
        public void setGlobalCompatibilityMode(String mode) {
        }

        @Override
        public void close() {
        }
    }

    @BeforeAll
    static void start() {
        server = new SchemaRegistryServer(
                SchemaRegistryServerConfig.defaults()
                        .withHost("127.0.0.1")
                        .withPort(0)
                        .withApiToken(TOKEN),
                new EmptyStore());
        base = "http://127.0.0.1:" + server.start();
        http = HttpClient.newHttpClient();
    }

    @AfterAll
    static void stop() {
        server.close();
        http.close();
    }

    private static HttpResponse<String> get(String path, String... headers) throws Exception {
        HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(base + path)).GET();
        for (int i = 0; i < headers.length; i += 2) {
            request.header(headers[i], headers[i + 1]);
        }
        return http.send(request.build(), HttpResponse.BodyHandlers.ofString());
    }

    @Test
    void everyRouteRequiresTheTokenExceptHealth() throws Exception {
        assertThat(get("/health").statusCode()).isEqualTo(200);

        for (String path : List.of("/subjects", "/config", "/schemas/ids/1",
                "/protomolt/chains", "/protomolt/subjects/x/descriptor-set")) {
            assertThat(get(path).statusCode())
                    .as("%s without a token", path)
                    .isEqualTo(401);
        }
    }

    @Test
    void theTokenOpensTheDoorAsHeaderOrBearer() throws Exception {
        assertThat(get("/subjects", "api_token", TOKEN).statusCode()).isEqualTo(200);
        assertThat(get("/subjects", "authorization", "Bearer " + TOKEN).statusCode())
                .isEqualTo(200);
        assertThat(get("/subjects", "api_token", "wrong").statusCode()).isEqualTo(401);
    }
}
