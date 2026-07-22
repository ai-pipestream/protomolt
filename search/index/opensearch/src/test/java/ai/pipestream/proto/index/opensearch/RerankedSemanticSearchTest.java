package ai.pipestream.proto.index.opensearch;

import ai.pipestream.proto.embeddings.EmbeddingProvider;
import ai.pipestream.proto.rerank.RerankProvider;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@link RerankedSemanticSearch} against a fake engine and fixture providers: the kNN list is
 * recalled in engine order, the reranker's scores reorder it, k truncates, and the fail-fast
 * guards name the offending hit id or provider id.
 */
class RerankedSemanticSearchTest {

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
    void rerankScoresReorderTheKnnList() throws IOException {
        fake.respondWithHits(
                hit("a", 0.9, "the dog sat on the mat"),
                hit("b", 0.8, "the cat sat on the mat"),
                hit("c", 0.7, "the puppy played in the garden"));
        RerankedSemanticSearch search = search(
                scoringBy(text -> text.contains("puppy") ? 0.99 : 0.1));

        List<RankedHit> hits = search.search("books", "embedding", "sentence",
                "a young dog", 3, 3);

        // The engine recalled [a, b, c]; the fixture reranker promotes the puppy text to first.
        assertThat(hits).extracting(RankedHit::id).containsExactly("c", "a", "b");
        RankedHit first = hits.get(0);
        assertThat(first.text()).isEqualTo("the puppy played in the garden");
        assertThat(first.relevanceScore()).isEqualTo(0.99);
        assertThat(first.knnScore()).isEqualTo(0.7);
    }

    @Test
    void kTruncatesTheRerankedList() throws IOException {
        fake.respondWithHits(
                hit("a", 0.9, "the dog sat on the mat"),
                hit("b", 0.8, "the cat sat on the mat"),
                hit("c", 0.7, "the puppy played in the garden"));
        RerankedSemanticSearch search = search(
                scoringBy(text -> text.contains("puppy") ? 0.99 : 0.1));

        List<RankedHit> hits = search.search("books", "embedding", "sentence",
                "a young dog", 2, 3);

        assertThat(hits).extracting(RankedHit::id).containsExactly("c", "a");
    }

    @Test
    void candidatesSetTheKnnDepthInTheRequestBody() throws IOException {
        fake.respondWithHits(hit("a", 0.9, "the dog sat on the mat"));
        RerankedSemanticSearch search = search(scoringBy(text -> 0.5));

        search.search("books", "embedding", "sentence", "a young dog", 1, 6);

        JsonNode body = JSON.readTree(fake.requests.get(0).body());
        assertThat(body.path("size").asInt()).isEqualTo(6);
        assertThat(body.path("query").path("knn").path("embedding").path("k").asInt())
                .isEqualTo(6);
    }

    @Test
    void candidatesBelowKFail() {
        RerankedSemanticSearch search = search(scoringBy(text -> 0.5));

        assertThatThrownBy(() -> search.search("books", "embedding", "sentence",
                "a young dog", 4, 3))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("candidates")
                .hasMessageContaining("k");
    }

    @Test
    void aHitWithoutTheTextFieldFailsNamingTheId() {
        fake.respondWithHits(
                hit("a", 0.9, "the dog sat on the mat"),
                "{\"_id\":\"b\",\"_score\":0.8,\"_source\":{\"title\":\"no sentence here\"}}");
        RerankedSemanticSearch search = search(scoringBy(text -> 0.5));

        assertThatThrownBy(() -> search.search("books", "embedding", "sentence",
                "a young dog", 2, 2))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("'b'")
                .hasMessageContaining("sentence");
    }

    @Test
    void aShortScoreListFailsNamingTheProvider() {
        fake.respondWithHits(
                hit("a", 0.9, "the dog sat on the mat"),
                hit("b", 0.8, "the cat sat on the mat"),
                hit("c", 0.7, "the puppy played in the garden"));
        RerankedSemanticSearch search = search(scoring(texts -> List.of(0.9, 0.1)));

        assertThatThrownBy(() -> search.search("books", "embedding", "sentence",
                "a young dog", 2, 3))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("fixture-reranker")
                .hasMessageContaining("2 scores for 3");
    }

    // ---- fixtures ----

    private RerankedSemanticSearch search(RerankProvider reranker) {
        return new RerankedSemanticSearch(new OpenSearchSearch(baseUrl), embedder(), reranker);
    }

    /** A two-dimensional fixture embedder; the fake engine ignores the vector anyway. */
    private static EmbeddingProvider embedder() {
        return new EmbeddingProvider() {
            @Override
            public String providerId() {
                return "fixture-embedder";
            }

            @Override
            public int dimension() {
                return 2;
            }

            @Override
            public float[] embed(String text) {
                return new float[]{1f, 0f};
            }
        };
    }

    private static RerankProvider scoringBy(Function<String, Double> textScore) {
        return scoring(texts -> {
            List<Double> scores = new ArrayList<>(texts.size());
            for (String text : texts) {
                scores.add(textScore.apply(text));
            }
            return scores;
        });
    }

    private static RerankProvider scoring(Function<List<String>, List<Double>> batchScore) {
        return new RerankProvider() {
            @Override
            public String providerId() {
                return "fixture-reranker";
            }

            @Override
            public List<Double> score(String query, List<String> texts) {
                return batchScore.apply(texts);
            }
        };
    }

    private static String hit(String id, double score, String sentence) {
        return "{\"_id\":\"" + id + "\",\"_score\":" + score
                + ",\"_source\":{\"sentence\":\"" + sentence + "\"}}";
    }

    /**
     * Fake engine: records requests and answers with pinned hit JSON fragments wrapped in a
     * {@code hits.hits} envelope.
     */
    private static final class FakeSearchHandler implements HttpHandler {

        private record RecordedRequest(String method, String path, String body) {
        }

        private final List<RecordedRequest> requests = new CopyOnWriteArrayList<>();
        private volatile String responseBody = "{\"hits\":{\"hits\":[]}}";

        private void respondWithHits(String... hits) {
            responseBody = "{\"hits\":{\"hits\":[" + String.join(",", hits) + "]}}";
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            requests.add(new RecordedRequest(exchange.getRequestMethod(),
                    exchange.getRequestURI().getPath(),
                    new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8)));
            byte[] bytes = responseBody.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, bytes.length);
            try (var out = exchange.getResponseBody()) {
                out.write(bytes);
            }
        }
    }
}
