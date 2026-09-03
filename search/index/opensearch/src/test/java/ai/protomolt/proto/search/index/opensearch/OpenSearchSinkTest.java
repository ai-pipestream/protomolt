package ai.protomolt.proto.search.index.opensearch;

import ai.protomolt.proto.search.index.spi.IndexFieldKind;
import ai.protomolt.proto.search.index.spi.IndexMapping;
import ai.protomolt.proto.search.index.spi.ResolvedFieldHint;
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
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@link OpenSearchSink} against a fake engine: the ensure-index protocol (HEAD, then PUT with
 * the generated properties and the knn setting for vector mappings, the lost-create race), the bulk
 * NDJSON wire shape with and without caller ids, and the failure surfaces (non-2xx, per-item
 * errors).
 */
class OpenSearchSinkTest {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final IndexMapping TEXT_MAPPING = new IndexMapping("ai.pipestream.test.Doc",
            List.of(new IndexMapping.IndexedField("title", "title",
                    ResolvedFieldHint.of(IndexFieldKind.TEXT))));
    private static final IndexMapping VECTOR_MAPPING = new IndexMapping("ai.pipestream.test.Doc",
            List.of(new IndexMapping.IndexedField("embedding", "embedding",
                    ResolvedFieldHint.builder(IndexFieldKind.VECTOR).vectorDims(4).build())));

    private HttpServer server;
    private String baseUrl;
    private FakeEngineHandler fake;

    @BeforeEach
    void startServer() throws IOException {
        fake = new FakeEngineHandler();
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
    void ensureIndexReturnsFalseWithoutPutWhenTheIndexExists() throws IOException {
        fake.stub("HEAD", "/books", 200, "");
        OpenSearchSink sink = new OpenSearchSink(baseUrl);

        assertThat(sink.ensureIndex("books", TEXT_MAPPING)).isFalse();

        assertThat(fake.requests).singleElement()
                .satisfies(request -> assertThat(request.method()).isEqualTo("HEAD"));
    }

    @Test
    void ensureIndexCreatesMissingIndexWithTheGeneratedMappings() throws IOException {
        fake.stub("HEAD", "/books", 404, "");
        fake.stub("PUT", "/books", 200, "{\"acknowledged\":true}");
        OpenSearchSink sink = new OpenSearchSink(baseUrl);

        assertThat(sink.ensureIndex("books", TEXT_MAPPING)).isTrue();

        FakeEngineHandler.RecordedRequest put = fake.requests.get(1);
        assertThat(put.method()).isEqualTo("PUT");
        assertThat(put.path()).isEqualTo("/books");
        assertThat(put.contentType()).isEqualTo("application/json");
        JsonNode body = JSON.readTree(put.body());
        assertThat(body.path("mappings").path("properties").path("title").path("type").asText())
                .isEqualTo("text");
        // no VECTOR field in the mapping: the knn setting is not enabled
        assertThat(body.has("settings")).isFalse();
    }

    @Test
    void ensureIndexEnablesKnnWhenTheMappingHasAVectorField() throws IOException {
        fake.stub("HEAD", "/books", 404, "");
        fake.stub("PUT", "/books", 200, "{\"acknowledged\":true}");
        OpenSearchSink sink = new OpenSearchSink(baseUrl);

        assertThat(sink.ensureIndex("books", VECTOR_MAPPING)).isTrue();

        JsonNode body = JSON.readTree(fake.requests.get(1).body());
        // knn_vector mappings fail the index create unless the index enables knn
        assertThat(body.path("settings").path("index.knn").asBoolean()).isTrue();
        assertThat(body.path("mappings").path("properties").path("embedding").path("type").asText())
                .isEqualTo("knn_vector");
    }

    @Test
    void ensureIndexThrowsOnAnUnexpectedHeadStatus() {
        fake.stub("HEAD", "/books", 500, "");
        OpenSearchSink sink = new OpenSearchSink(baseUrl);

        assertThatThrownBy(() -> sink.ensureIndex("books", TEXT_MAPPING))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("books")
                .hasMessageContaining("500");
    }

    @Test
    void ensureIndexTreatsALostCreateRaceAsAlreadyExisting() throws IOException {
        fake.stub("HEAD", "/books", 404, "");
        fake.stub("PUT", "/books", 400,
                "{\"error\":{\"type\":\"resource_already_exists_exception\"}}");
        OpenSearchSink sink = new OpenSearchSink(baseUrl);

        // another writer created the index between the HEAD and the PUT
        assertThat(sink.ensureIndex("books", TEXT_MAPPING)).isFalse();
    }

    @Test
    void ensureIndexThrowsWhenTheEngineRefusesTheCreate() {
        fake.stub("HEAD", "/books", 404, "");
        fake.stub("PUT", "/books", 400,
                "{\"error\":{\"type\":\"invalid_index_name_exception\",\"reason\":\"bad name\"}}");
        OpenSearchSink sink = new OpenSearchSink(baseUrl);

        assertThatThrownBy(() -> sink.ensureIndex("books", TEXT_MAPPING))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("books")
                .hasMessageContaining("400")
                .hasMessageContaining("invalid_index_name_exception");
    }

    @Test
    void bulkWriteWithIdsSendsOneNdjsonActionPerDocument() throws IOException {
        fake.stub("POST", "/books/_bulk", 200, "{\"errors\":false,\"items\":[]}");
        OpenSearchSink sink = new OpenSearchSink(baseUrl);
        Map<String, Map<String, Object>> documents = new LinkedHashMap<>();
        documents.put("1", Map.of("title", "the dog sat"));
        documents.put("2", Map.of("title", "the cat sat"));

        sink.bulkWrite("books", documents, false);

        assertThat(fake.requests).singleElement();
        FakeEngineHandler.RecordedRequest request = fake.requests.get(0);
        assertThat(request.method()).isEqualTo("POST");
        assertThat(request.path()).isEqualTo("/books/_bulk");
        assertThat(request.query()).isNull();
        assertThat(request.contentType()).isEqualTo("application/x-ndjson");
        // action line plus source line per document, each pair newline-terminated
        assertThat(request.body()).isEqualTo(
                "{\"index\":{\"_id\":\"1\"}}\n{\"title\":\"the dog sat\"}\n"
                        + "{\"index\":{\"_id\":\"2\"}}\n{\"title\":\"the cat sat\"}\n");
    }

    @Test
    void bulkWriteWithRefreshAppendsTheRefreshParameter() throws IOException {
        fake.stub("POST", "/books/_bulk", 200, "{\"errors\":false,\"items\":[]}");
        OpenSearchSink sink = new OpenSearchSink(baseUrl);

        sink.bulkWrite("books", Map.of("1", Map.of("title", "the dog sat")), true);

        assertThat(fake.requests.get(0).query()).isEqualTo("refresh=true");
    }

    @Test
    void bulkWriteWithoutIdsLeavesTheActionIdOut() throws IOException {
        fake.stub("POST", "/books/_bulk", 200, "{\"errors\":false,\"items\":[]}");
        OpenSearchSink sink = new OpenSearchSink(baseUrl);

        sink.bulkWrite("books", List.of(Map.of("title", "the dog sat")), false);

        assertThat(fake.requests.get(0).body())
                .isEqualTo("{\"index\":{}}\n{\"title\":\"the dog sat\"}\n");
    }

    @Test
    void bulkWriteWithNoDocumentsSendsNoRequest() throws IOException {
        OpenSearchSink sink = new OpenSearchSink(baseUrl);

        sink.bulkWrite("books", Map.of(), true);
        sink.bulkWrite("books", List.of(), false);

        assertThat(fake.requests).isEmpty();
    }

    @Test
    void bulkItemFailuresAreCollectedIntoOneException() {
        fake.stub("POST", "/books/_bulk", 200, "{\"errors\":true,\"items\":["
                + "{\"index\":{\"_id\":\"good\",\"status\":201}},"
                + "{\"index\":{\"_id\":\"bad\",\"status\":400,\"error\":{"
                + "\"type\":\"mapper_parsing_exception\",\"reason\":\"failed to parse [rank]\"}}}"
                + "]}");
        OpenSearchSink sink = new OpenSearchSink(baseUrl);
        Map<String, Map<String, Object>> documents = new LinkedHashMap<>();
        documents.put("good", Map.of("title", "Bulk Survivor"));
        documents.put("bad", Map.of("rank", "not-a-number"));

        assertThatThrownBy(() -> sink.bulkWrite("books", documents, true))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("1 of 2")
                .hasMessageContaining("bad")
                .hasMessageContaining("mapper_parsing_exception")
                .hasMessageContaining("failed to parse [rank]")
                .hasMessageNotContaining("good");
    }

    @Test
    void bulkNon2xxNamesTheIndexStatusAndBody() {
        fake.stub("POST", "/books/_bulk", 500, "boom");
        OpenSearchSink sink = new OpenSearchSink(baseUrl);

        assertThatThrownBy(() -> sink.bulkWrite("books", Map.of("1", Map.of()), false))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("books")
                .hasMessageContaining("500")
                .hasMessageContaining("boom");
    }

    @Test
    void nullArgumentsAreRejected() {
        OpenSearchSink sink = new OpenSearchSink(baseUrl);

        assertThatNullPointerException().isThrownBy(() -> sink.ensureIndex(null, TEXT_MAPPING));
        assertThatNullPointerException().isThrownBy(() -> sink.ensureIndex("books", null));
        assertThatNullPointerException()
                .isThrownBy(() -> sink.bulkWrite(null, Map.of("1", Map.of()), false));
        assertThatNullPointerException()
                .isThrownBy(() -> sink.bulkWrite("books", (Map<String, Map<String, Object>>) null, false));
        assertThatNullPointerException()
                .isThrownBy(() -> sink.bulkWrite("books", (List<Map<String, Object>>) null, false));
        assertThatNullPointerException().isThrownBy(() -> new OpenSearchSink(null));
        assertThatNullPointerException().isThrownBy(() -> new OpenSearchSink(baseUrl, null));
    }

    @Test
    void aTrailingSlashOnTheBaseUrlIsStripped() throws IOException {
        fake.stub("HEAD", "/books", 200, "");
        OpenSearchSink sink = new OpenSearchSink(baseUrl + "/");

        sink.ensureIndex("books", TEXT_MAPPING);

        assertThat(fake.requests.get(0).path()).isEqualTo("/books");
    }

    @Test
    void closeLeavesACallerSuppliedClientOpen() throws IOException {
        fake.stub("HEAD", "/books", 200, "");
        HttpClient client = HttpClient.newHttpClient();
        OpenSearchSink sink = new OpenSearchSink(baseUrl, client);

        sink.close();

        // the caller owns the client: it still answers after the sink is closed
        OpenSearchSink second = new OpenSearchSink(baseUrl, client);
        assertThat(second.ensureIndex("books", TEXT_MAPPING)).isFalse();
    }

    /**
     * Fake engine: records requests and answers from stubs keyed on {@code "METHOD path"}
     * (query excluded); anything unstubbed gets a 404.
     */
    private static final class FakeEngineHandler implements HttpHandler {

        private record RecordedRequest(String method, String path, String query,
                String contentType, String body) {
        }

        private record Stub(int status, String body) {
        }

        private final List<RecordedRequest> requests = new CopyOnWriteArrayList<>();
        private final Map<String, Stub> stubs = new ConcurrentHashMap<>();

        private void stub(String method, String path, int status, String body) {
            stubs.put(method + " " + path, new Stub(status, body));
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String body = new String(exchange.getRequestBody().readAllBytes(),
                    StandardCharsets.UTF_8);
            requests.add(new RecordedRequest(exchange.getRequestMethod(),
                    exchange.getRequestURI().getPath(),
                    exchange.getRequestURI().getQuery(),
                    exchange.getRequestHeaders().getFirst("Content-Type"), body));
            Stub stub = stubs.getOrDefault(
                    exchange.getRequestMethod() + " " + exchange.getRequestURI().getPath(),
                    new Stub(404, "{\"error\":\"unstubbed\"}"));
            byte[] bytes = stub.body().getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            if ("HEAD".equals(exchange.getRequestMethod()) || bytes.length == 0) {
                exchange.sendResponseHeaders(stub.status(), -1);
                exchange.close();
                return;
            }
            exchange.sendResponseHeaders(stub.status(), bytes.length);
            try (var out = exchange.getResponseBody()) {
                out.write(bytes);
            }
        }
    }
}
