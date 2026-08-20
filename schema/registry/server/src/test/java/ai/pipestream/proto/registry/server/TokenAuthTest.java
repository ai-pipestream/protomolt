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
 * carries schema, config, and workflow writes plus action execution — it must not be the one
 * listener an operator forgets.
 */
class TokenAuthTest {

    private static final String TOKEN = "reg-sekret";

    private static SchemaRegistryServer server;
    private static HttpClient http;
    private static String base;


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
                "/protomolt/workflows", "/protomolt/subjects/x/descriptor-set")) {
            assertThat(get(path).statusCode())
                    .as("%s without a token", path)
                    .isEqualTo(401);
        }
    }

    @Test
    void theTokenAdmitsAsHeaderOrBearer() throws Exception {
        assertThat(get("/subjects", "api_token", TOKEN).statusCode()).isEqualTo(200);
        assertThat(get("/subjects", "authorization", "Bearer " + TOKEN).statusCode())
                .isEqualTo(200);
        assertThat(get("/subjects", "api_token", "wrong").statusCode()).isEqualTo(401);
    }
}
