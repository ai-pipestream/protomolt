package ai.pipestream.proto.rerank.ovms;

import ai.pipestream.proto.rerank.RerankProvider;
import ai.pipestream.proto.rerank.RerankProviders;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

class OvmsRerankProviderTest {

    private static final String MODEL = "test-reranker";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private HttpServer server;
    private String baseUrl;
    private FakeRerankHandler fake;
    private String savedUrlProperty;
    private String savedModelProperty;

    @BeforeEach
    void startServer() throws IOException {
        savedUrlProperty = System.clearProperty(OvmsRerankProvider.URL_PROPERTY);
        savedModelProperty = System.clearProperty(OvmsRerankProvider.MODEL_PROPERTY);
        fake = new FakeRerankHandler();
        server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        server.createContext("/v3/rerank", fake);
        server.start();
        baseUrl = "http://localhost:" + server.getAddress().getPort();
    }

    @AfterEach
    void stopServer() {
        server.stop(0);
        restore(OvmsRerankProvider.URL_PROPERTY, savedUrlProperty);
        restore(OvmsRerankProvider.MODEL_PROPERTY, savedModelProperty);
    }

    private static void restore(String property, String saved) {
        if (saved == null) {
            System.clearProperty(property);
        } else {
            System.setProperty(property, saved);
        }
    }

    @Test
    void requestCarriesMethodPathContentTypeAndJsonBody() throws IOException {
        OvmsRerankProvider provider = new OvmsRerankProvider(baseUrl, MODEL);

        provider.score("bamboo", List.of("bb", "ddddd", "a", "ccc"));

        assertThat(fake.requests).singleElement();
        FakeRerankHandler.RecordedRequest request = fake.requests.get(0);
        assertThat(request.method()).isEqualTo("POST");
        assertThat(request.path()).isEqualTo("/v3/rerank");
        assertThat(request.contentType()).isEqualTo("application/json");
        JsonNode body = MAPPER.readTree(request.body());
        assertThat(body.path("model").asText()).isEqualTo(MODEL);
        assertThat(body.path("query").asText()).isEqualTo("bamboo");
        List<String> documents = new ArrayList<>();
        body.path("documents").forEach(node -> documents.add(node.asText()));
        assertThat(documents).containsExactly("bb", "ddddd", "a", "ccc");
    }

    @Test
    void shuffledResultsScatterBackIntoInputOrder() {
        OvmsRerankProvider provider = new OvmsRerankProvider(baseUrl, MODEL);

        List<Double> scores = provider.score("bamboo", List.of("bb", "ddddd", "a", "ccc"));

        // The fake scores by document length and answers sorted by descending score, so the
        // results arrive out of index order and prove the scatter realigns.
        assertThat(fake.lastResultOrder).containsExactly(1, 3, 0, 2);
        assertThat(scores).containsExactly(2.0, 5.0, 1.0, 3.0);
    }

    @Test
    void trailingSlashOnTheBaseUrlIsStripped() {
        OvmsRerankProvider provider = new OvmsRerankProvider(baseUrl + "/", MODEL);

        provider.score("bamboo", List.of("a"));

        assertThat(fake.requests).singleElement().satisfies(
                request -> assertThat(request.path()).isEqualTo("/v3/rerank"));
    }

    @Test
    void non2xxSurfacesStatusAndBodyNamingUrlAndModel() {
        fake.status = 500;
        fake.responseBody = "servable exploded";
        OvmsRerankProvider provider = new OvmsRerankProvider(baseUrl, MODEL);

        assertThatThrownBy(() -> provider.score("bamboo", List.of("boom")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("500")
                .hasMessageContaining("servable exploded")
                .hasMessageContaining(MODEL)
                .hasMessageContaining(baseUrl);
    }

    @Test
    void missingIndexThrows() {
        fake.responseBody = "{\"results\":[{\"index\":0,\"relevance_score\":0.5}]}";
        OvmsRerankProvider provider = new OvmsRerankProvider(baseUrl, MODEL);

        assertThatThrownBy(() -> provider.score("bamboo", List.of("a", "b")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("no result for document 1");
    }

    @Test
    void duplicatedIndexThrows() {
        fake.responseBody = "{\"results\":[{\"index\":0,\"relevance_score\":0.5},"
                + "{\"index\":0,\"relevance_score\":0.6},"
                + "{\"index\":1,\"relevance_score\":0.1}]}";
        OvmsRerankProvider provider = new OvmsRerankProvider(baseUrl, MODEL);

        assertThatThrownBy(() -> provider.score("bamboo", List.of("a", "b")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("twice");
    }

    @Test
    void outOfRangeIndexThrows() {
        fake.responseBody = "{\"results\":[{\"index\":0,\"relevance_score\":0.5},"
                + "{\"index\":9,\"relevance_score\":0.1}]}";
        OvmsRerankProvider provider = new OvmsRerankProvider(baseUrl, MODEL);

        assertThatThrownBy(() -> provider.score("bamboo", List.of("a", "b")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("result index 9");
    }

    @Test
    void emptyTextsReturnEmptyWithoutARequest() {
        OvmsRerankProvider provider = new OvmsRerankProvider(baseUrl, MODEL);

        assertThat(provider.score("bamboo", List.of())).isEmpty();
        assertThat(fake.requests).isEmpty();
    }

    @Test
    void serviceLoaderFindsTheProviderById() {
        // The no-arg constructor must not touch the knobs, or an unconfigured provider would
        // break discovery of every other provider on the classpath.
        assertThat(RerankProviders.all()).containsKey("ovms");
    }

    @Test
    void firstUseWithoutConfigurationNamesAllKnobs() {
        assumeThat(System.getenv(OvmsRerankProvider.URL_ENVIRONMENT_VARIABLE)).isNull();
        assumeThat(System.getenv(OvmsRerankProvider.MODEL_ENVIRONMENT_VARIABLE)).isNull();

        OvmsRerankProvider provider = new OvmsRerankProvider();

        assertThatThrownBy(() -> provider.score("bamboo", List.of("a")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(OvmsRerankProvider.URL_PROPERTY)
                .hasMessageContaining(OvmsRerankProvider.URL_ENVIRONMENT_VARIABLE)
                .hasMessageContaining(OvmsRerankProvider.MODEL_PROPERTY)
                .hasMessageContaining(OvmsRerankProvider.MODEL_ENVIRONMENT_VARIABLE);
    }

    @Test
    void noArgConstructorResolvesTheUrlAndModelProperties() {
        System.setProperty(OvmsRerankProvider.URL_PROPERTY, baseUrl);
        System.setProperty(OvmsRerankProvider.MODEL_PROPERTY, MODEL);

        RerankProvider provider = RerankProviders.byId("ovms");

        assertThat(provider).isInstanceOf(OvmsRerankProvider.class);
        assertThat(provider.score("bamboo", List.of("wired"))).containsExactly(5.0);
    }

    /**
     * Fake OVMS rerank endpoint: records requests, and by default scores each document by its
     * length and answers with results sorted by descending score (out of index order for the
     * fixture documents, proving the provider realigns them). Tests can pin a status and body
     * instead.
     */
    private static final class FakeRerankHandler implements HttpHandler {

        private record RecordedRequest(String method, String path, String contentType,
                String body) {
        }

        private final List<RecordedRequest> requests = new CopyOnWriteArrayList<>();
        private final List<Integer> lastResultOrder = new CopyOnWriteArrayList<>();
        private volatile int status = 200;
        private volatile String responseBody;

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String body = new String(exchange.getRequestBody().readAllBytes(),
                    StandardCharsets.UTF_8);
            requests.add(new RecordedRequest(exchange.getRequestMethod(),
                    exchange.getRequestURI().getPath(),
                    exchange.getRequestHeaders().getFirst("Content-Type"), body));
            String response = responseBody != null ? responseBody : defaultResponse(body);
            byte[] bytes = response.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(status, bytes.length);
            try (var out = exchange.getResponseBody()) {
                out.write(bytes);
            }
        }

        /** Scores each requested document by its length and emits results best first. */
        private String defaultResponse(String requestBody) throws IOException {
            JsonNode documents = MAPPER.readTree(requestBody).path("documents");
            List<Integer> order = new ArrayList<>();
            for (int i = 0; i < documents.size(); i++) {
                order.add(i);
            }
            order.sort(Comparator
                    .comparingInt((Integer i) -> documents.get(i).asText().length())
                    .reversed());
            ObjectNode response = MAPPER.createObjectNode();
            ArrayNode results = response.putArray("results");
            lastResultOrder.clear();
            for (int index : order) {
                lastResultOrder.add(index);
                ObjectNode result = results.addObject();
                result.put("index", index);
                result.put("relevance_score", documents.get(index).asText().length());
            }
            return response.toString();
        }
    }
}
