package ai.pipestream.proto.index.opensearch;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@link OpenSearchSearch} against a fake engine: the request shape (path, knn clause, k,
 * vector values), the hit parsing (id, score, source fields), and the non-2xx error surface.
 */
class OpenSearchSearchTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    private HttpServer server;
    private String baseUrl;
    private FakeSearchHandler fake;

    @BeforeEach
    void startServer() throws IOException {
        fake = new FakeSearchHandler();
        server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        server.createContext("/", fake);
        server.start();
        baseUrl = "http://localhost:" + server.getAddress().getPort();
    }

    @AfterEach
    void stopServer() {
        server.stop(0);
    }

    @Test
    void knnRequestCarriesPathFieldKAndVector() throws IOException {
        fake.responseBody = "{\"hits\":{\"hits\":[]}}";
        OpenSearchSearch search = new OpenSearchSearch(baseUrl);

        search.knn("books", "embedding", List.of(0.5f, -1.25f, 2f), 7);

        assertThat(fake.requests).singleElement();
        FakeSearchHandler.RecordedRequest request = fake.requests.get(0);
        assertThat(request.method()).isEqualTo("POST");
        assertThat(request.path()).isEqualTo("/books/_search");
        assertThat(request.contentType()).isEqualTo("application/json");
        JsonNode body = JSON.readTree(request.body());
        assertThat(body.path("size").asInt()).isEqualTo(7);
        JsonNode clause = body.path("query").path("knn").path("embedding");
        assertThat(clause.path("k").asInt()).isEqualTo(7);
        List<Float> vector = new java.util.ArrayList<>();
        clause.path("vector").forEach(component -> vector.add((float) component.asDouble()));
        assertThat(vector).containsExactly(0.5f, -1.25f, 2f);
    }

    @Test
    void hitsParseIntoIdScoreAndSource() throws IOException {
        fake.responseBody = "{\"hits\":{\"hits\":["
                + "{\"_id\":\"doc-1\",\"_score\":0.9,\"_source\":{\"sentence\":\"the dog sat\",\"rank\":3}},"
                + "{\"_id\":\"doc-2\",\"_score\":0.4,\"_source\":{\"sentence\":\"the cat sat\",\"rank\":1}}"
                + "]}}";
        OpenSearchSearch search = new OpenSearchSearch(baseUrl);

        List<OpenSearchHit> hits = search.knn("books", "embedding", List.of(1f, 0f), 2);

        assertThat(hits).hasSize(2);
        assertThat(hits.get(0).id()).isEqualTo("doc-1");
        assertThat(hits.get(0).score()).isEqualTo(0.9);
        assertThat(hits.get(0).source())
                .containsEntry("sentence", "the dog sat")
                .containsEntry("rank", 3);
        assertThat(hits.get(1).id()).isEqualTo("doc-2");
        assertThat(hits.get(1).score()).isEqualTo(0.4);
        assertThat(hits.get(1).source()).containsEntry("sentence", "the cat sat");
    }

    @Test
    void non2xxNamesTheIndexStatusAndBody() {
        fake.status = 400;
        fake.responseBody = "{\"error\":{\"type\":\"illegal_argument_exception\","
                + "\"reason\":\"field 'embedding' is not a knn_vector\"}}";
        OpenSearchSearch search = new OpenSearchSearch(baseUrl);

        assertThatThrownBy(() -> search.knn("books", "embedding", List.of(1f), 1))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("books")
                .hasMessageContaining("400")
                .hasMessageContaining("not a knn_vector");
    }

    /**
     * Fake engine: records requests and answers with a pinned status and body, a bare
     * {@code {"hits":{"hits":[]}}} by default.
     */
    private static final class FakeSearchHandler implements HttpHandler {

        private record RecordedRequest(String method, String path, String contentType,
                String body) {
        }

        private final List<RecordedRequest> requests = new CopyOnWriteArrayList<>();
        private volatile int status = 200;
        private volatile String responseBody = "{\"hits\":{\"hits\":[]}}";

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String body = new String(exchange.getRequestBody().readAllBytes(),
                    StandardCharsets.UTF_8);
            requests.add(new RecordedRequest(exchange.getRequestMethod(),
                    exchange.getRequestURI().getPath(),
                    exchange.getRequestHeaders().getFirst("Content-Type"), body));
            byte[] bytes = responseBody.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(status, bytes.length);
            try (var out = exchange.getResponseBody()) {
                out.write(bytes);
            }
        }
    }
}
