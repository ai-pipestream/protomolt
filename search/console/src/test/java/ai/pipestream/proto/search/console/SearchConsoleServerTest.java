package ai.pipestream.proto.search.console;

import ai.pipestream.proto.search.v1.ListSubjectsRequest;
import ai.pipestream.proto.search.v1.ListSubjectsResponse;
import ai.pipestream.proto.search.v1.SearchHit;
import ai.pipestream.proto.search.v1.SearchLane;
import ai.pipestream.proto.search.v1.SearchRequest;
import ai.pipestream.proto.search.v1.SearchResponse;
import ai.pipestream.proto.search.v1.SearchServiceGrpc;
import ai.pipestream.proto.search.v1.StoredValue;
import ai.pipestream.proto.search.v1.SubjectInfo;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The console's HTTP-to-gRPC bridge against a fake door and a fake actions route: page and
 * subjects serving, the search round trip, verbatim refusal pass-through with honest status
 * codes, the operations proxy, and the disabled-panel answer.
 */
class SearchConsoleServerTest {

    static final AtomicReference<SearchRequest> lastSearch = new AtomicReference<>();

    static Server door;
    static HttpServer actions;
    static SearchConsoleServer console;
    static SearchConsoleServer consoleWithoutOps;
    static HttpClient client;
    static ObjectMapper json;

    /** A door that refuses like the real one and answers one canned hit otherwise. */
    static final class FakeDoor extends SearchServiceGrpc.SearchServiceImplBase {

        @Override
        public void search(SearchRequest request, StreamObserver<SearchResponse> observer) {
            lastSearch.set(request);
            if (!"repo-document".equals(request.getMappingSubject())) {
                observer.onError(Status.INVALID_ARGUMENT.withDescription(
                        "unknown mapping subject '" + request.getMappingSubject()
                                + "'; served subjects: repo-document").asRuntimeException());
                return;
            }
            if (request.getLane() == SearchLane.SEARCH_LANE_VECTOR) {
                observer.onError(Status.FAILED_PRECONDITION.withDescription(
                        "subject 'repo-document' serves no vector lane").asRuntimeException());
                return;
            }
            observer.onNext(SearchResponse.newBuilder()
                    .addHits(SearchHit.newBuilder()
                            .setDocId("doc-1").setScore(0.5f)
                            .putStored("title", StoredValue.newBuilder()
                                    .setStringValue("First Document").build()))
                    .build());
            observer.onCompleted();
        }

        @Override
        public void listSubjects(ListSubjectsRequest request,
                StreamObserver<ListSubjectsResponse> observer) {
            observer.onNext(ListSubjectsResponse.newBuilder()
                    .addSubjects(SubjectInfo.newBuilder()
                            .setSubject("repo-document")
                            .setDocIdField("doc_id")
                            .addTextFields("title")
                            .addTextFields("search_metadata_body"))
                    .build());
            observer.onCompleted();
        }
    }

    @BeforeAll
    static void boot() throws Exception {
        door = InProcessServerBuilder.forName("console-test-door")
                .addService(new FakeDoor()).build().start();

        actions = HttpServer.create(new InetSocketAddress(0), 0);
        actions.createContext("/protomolt/actions", exchange -> {
            byte[] body = exchange.getRequestBody().readAllBytes();
            String path = exchange.getRequestURI().getPath();
            byte[] answer;
            int status = 200;
            if (path.endsWith("/actions")) {
                answer = "[{\"name\":\"list-jobs\"}]".getBytes(StandardCharsets.UTF_8);
            } else if (path.endsWith("/echo-input")) {
                answer = body;
            } else {
                status = 404;
                answer = "{\"error\":\"unknown-action\"}".getBytes(StandardCharsets.UTF_8);
            }
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(status, answer.length);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(answer);
            }
        });
        actions.start();

        console = new SearchConsoleServer(0, "inprocess:console-test-door",
                () -> "http://127.0.0.1:" + actions.getAddress().getPort()
                        + "/protomolt/actions");
        console.start();
        consoleWithoutOps = new SearchConsoleServer(0, "inprocess:console-test-door", () -> "");
        consoleWithoutOps.start();
        client = HttpClient.newHttpClient();
        json = new ObjectMapper();
    }

    @AfterAll
    static void shutdown() {
        console.close();
        consoleWithoutOps.close();
        actions.stop(0);
        door.shutdownNow();
    }

    static HttpResponse<String> get(SearchConsoleServer server, String path) throws Exception {
        return client.send(HttpRequest.newBuilder(
                        URI.create("http://127.0.0.1:" + server.port() + path)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
    }

    static HttpResponse<String> post(SearchConsoleServer server, String path, String body)
            throws Exception {
        return client.send(HttpRequest.newBuilder(
                        URI.create("http://127.0.0.1:" + server.port() + path))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(body)).build(),
                HttpResponse.BodyHandlers.ofString());
    }

    @Test
    void pageServesAtTheRootOnly() throws Exception {
        HttpResponse<String> page = get(console, "/");
        assertThat(page.statusCode()).isEqualTo(200);
        assertThat(page.headers().firstValue("Content-Type")).contains("text/html; charset=utf-8");
        assertThat(page.body()).contains("Search Console").contains("/subjects");
        assertThat(get(console, "/nowhere").statusCode()).isEqualTo(404);
    }

    @Test
    void subjectsBridgeTheDoorsSurfaceAsJson() throws Exception {
        HttpResponse<String> response = get(console, "/subjects");
        assertThat(response.statusCode()).isEqualTo(200);
        JsonNode subjects = json.readTree(response.body()).get("subjects");
        assertThat(subjects).hasSize(1);
        assertThat(subjects.get(0).get("subject").asText()).isEqualTo("repo-document");
        assertThat(subjects.get(0).get("textFields")).hasSize(2);
    }

    @Test
    void searchRoundTripsProto3Json() throws Exception {
        HttpResponse<String> response = post(console, "/search",
                "{\"mappingSubject\":\"repo-document\",\"query\":\"first\",\"k\":5,"
                        + "\"lane\":\"SEARCH_LANE_LEXICAL\"}");
        assertThat(response.statusCode()).isEqualTo(200);
        JsonNode hits = json.readTree(response.body()).get("hits");
        assertThat(hits).hasSize(1);
        assertThat(hits.get(0).get("docId").asText()).isEqualTo("doc-1");
        assertThat(hits.get(0).get("stored").get("title").get("stringValue").asText())
                .isEqualTo("First Document");

        SearchRequest sent = lastSearch.get();
        assertThat(sent.getQuery()).isEqualTo("first");
        assertThat(sent.getK()).isEqualTo(5);
        assertThat(sent.getLane()).isEqualTo(SearchLane.SEARCH_LANE_LEXICAL);
    }

    @Test
    void doorRefusalsPassThroughVerbatimWithHonestStatusCodes() throws Exception {
        HttpResponse<String> unknown = post(console, "/search",
                "{\"mappingSubject\":\"nope\",\"query\":\"x\",\"k\":1,"
                        + "\"lane\":\"SEARCH_LANE_LEXICAL\"}");
        assertThat(unknown.statusCode()).isEqualTo(400);
        assertThat(json.readTree(unknown.body()).get("error").asText())
                .contains("unknown mapping subject 'nope'")
                .contains("served subjects: repo-document");

        HttpResponse<String> noLane = post(console, "/search",
                "{\"mappingSubject\":\"repo-document\",\"query\":\"x\",\"k\":1,"
                        + "\"lane\":\"SEARCH_LANE_VECTOR\"}");
        assertThat(noLane.statusCode()).isEqualTo(409);
        assertThat(json.readTree(noLane.body()).get("error").asText())
                .contains("no vector lane");

        HttpResponse<String> garbage = post(console, "/search", "{\"lane\":\"NOT_A_LANE\"}");
        assertThat(garbage.statusCode()).isEqualTo(400);
        assertThat(json.readTree(garbage.body()).get("error").asText())
                .contains("not a valid SearchRequest");
    }

    @Test
    void operationsProxyForwardsToTheActionsRoute() throws Exception {
        HttpResponse<String> list = get(console, "/actions");
        assertThat(list.statusCode()).isEqualTo(200);
        assertThat(list.body()).contains("list-jobs");

        HttpResponse<String> echoed = post(console, "/actions/echo-input", "{\"a\":1}");
        assertThat(echoed.statusCode()).isEqualTo(200);
        assertThat(echoed.body()).isEqualTo("{\"a\":1}");

        HttpResponse<String> unknown = post(console, "/actions/never-heard-of-it", "{}");
        assertThat(unknown.statusCode()).isEqualTo(404);
        assertThat(get(console, "/actions/bad..name").statusCode()).isEqualTo(404);
    }

    @Test
    void consoleWithoutAnActionsTargetAnswers503() throws Exception {
        assertThat(get(consoleWithoutOps, "/actions").statusCode()).isEqualTo(503);
        assertThat(get(consoleWithoutOps, "/subjects").statusCode()).isEqualTo(200);
    }
}
