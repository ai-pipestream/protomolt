package ai.protomolt.proto.search.console;

import ai.protomolt.proto.actions.Caller;
import ai.protomolt.proto.actions.Scopes;
import ai.protomolt.proto.authz.CallerResolver;
import ai.protomolt.proto.authz.ConsoleSessions;
import ai.protomolt.proto.search.v1.ListSubjectsRequest;
import ai.protomolt.proto.search.v1.ListSubjectsResponse;
import ai.protomolt.proto.search.v1.SearchHit;
import ai.protomolt.proto.search.v1.SearchLane;
import ai.protomolt.proto.search.v1.SearchRequest;
import ai.protomolt.proto.search.v1.SearchResponse;
import ai.protomolt.proto.search.v1.SearchServiceGrpc;
import ai.protomolt.proto.search.v1.StoredValue;
import ai.protomolt.proto.search.v1.SubjectInfo;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
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
import java.time.Duration;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The console's HTTP-to-gRPC bridge against a fake search service and a fake actions route:
 * page and subjects serving, the search round trip, verbatim refusal pass-through with honest
 * status codes, the operations proxy, and the disabled-panel answer.
 */
class SearchConsoleServerTest {

    static final AtomicReference<SearchRequest> lastSearch = new AtomicReference<>();
    static final AtomicReference<String> lastActionsApiToken = new AtomicReference<>();
    static final AtomicReference<String> lastCallApiToken = new AtomicReference<>();
    static final Metadata.Key<String> API_TOKEN =
            Metadata.Key.of("api_token", Metadata.ASCII_STRING_MARSHALLER);

    static final CallerResolver RESOLVER = credential -> switch (credential) {
        case "querier-credential" -> Optional.of(
                Caller.scoped("querier", Set.of(Scopes.SEARCH_QUERY)));
        case "rebuilder-credential" -> Optional.of(
                Caller.scoped("rebuilder", Set.of(Scopes.METRICS_REBUILD)));
        default -> Optional.empty();
    };

    static Server service;
    static HttpServer actions;
    static SearchConsoleServer console;
    static SearchConsoleServer consoleWithoutOps;
    static SearchConsoleServer guarded;
    static HttpClient client;
    static ObjectMapper json;

    /** A service that refuses like the real one and answers one canned hit otherwise. */
    static final class FakeSearchService extends SearchServiceGrpc.SearchServiceImplBase {

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
        ServerInterceptor captureApiToken = new ServerInterceptor() {
            @Override
            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                    ServerCall<ReqT, RespT> call, Metadata headers,
                    ServerCallHandler<ReqT, RespT> next) {
                lastCallApiToken.set(headers.get(API_TOKEN));
                return Contexts.interceptCall(Context.current(), call, headers, next);
            }
        };
        service = InProcessServerBuilder.forName("console-test-service")
                .intercept(captureApiToken)
                .addService(new FakeSearchService()).build().start();

        actions = HttpServer.create(new InetSocketAddress(0), 0);
        actions.createContext("/protomolt/actions", exchange -> {
            lastActionsApiToken.set(exchange.getRequestHeaders().getFirst("api_token"));
            byte[] body = exchange.getRequestBody().readAllBytes();
            String path = exchange.getRequestURI().getPath();
            byte[] answer;
            int status = 200;
            if (path.endsWith("/actions")) {
                // The manifest shape the catalog tab renders from: name, description, schema.
                answer = ("[{\"name\":\"list-jobs\",\"description\":\"List jobs.\","
                        + "\"inputSchema\":{\"type\":\"object\",\"properties\":{}}}]")
                        .getBytes(StandardCharsets.UTF_8);
            } else if (path.endsWith("/echo-input")) {
                answer = body;
            } else if (path.endsWith("/describe-mapping")) {
                answer = ("{\"mappingSubject\":\"repo-document\","
                        + "\"messageType\":\"ai.pipestream.proto.repo.v1.Document\","
                        + "\"members\":[{\"name\":\"documents\",\"role\":\"MEMBER_ROLE_MEASURE\","
                        + "\"aggregate\":\"AGGREGATE_COUNT\"}],"
                        + "\"backends\":[\"METRIC_BACKEND_LUCENE\"]}")
                        .getBytes(StandardCharsets.UTF_8);
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

        console = new SearchConsoleServer(0, "inprocess:console-test-service",
                () -> "http://127.0.0.1:" + actions.getAddress().getPort()
                        + "/protomolt/actions");
        console.start();
        consoleWithoutOps = new SearchConsoleServer(0, "inprocess:console-test-service", () -> "");
        consoleWithoutOps.start();
        guarded = new SearchConsoleServer(0, "inprocess:console-test-service",
                () -> "http://127.0.0.1:" + actions.getAddress().getPort()
                        + "/protomolt/actions",
                ConsoleSessions.secured(SearchConsoleServer.COOKIE,
                        Duration.ofHours(1), RESOLVER));
        guarded.start();
        client = HttpClient.newHttpClient();
        json = new ObjectMapper();
    }

    @AfterAll
    static void shutdown() {
        console.close();
        consoleWithoutOps.close();
        guarded.close();
        actions.stop(0);
        service.shutdownNow();
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
    void theFourTabsAreDeclaredAndOnlySearchStartsVisible() throws Exception {
        String page = get(console, "/").body();
        for (String tab : new String[] {"search", "metrics", "catalog", "operations"}) {
            assertThat(page).as("tab button for " + tab).contains("data-tab=\"" + tab + "\"");
            assertThat(page).as("panel for " + tab).contains("id=\"panel-" + tab + "\"");
        }
        // Search is the landing tab, so its panel alone carries no hidden attribute.
        assertThat(page).contains("<section class=\"panel\" id=\"panel-search\">");
        assertThat(page).contains("id=\"panel-metrics\" hidden")
                .contains("id=\"panel-catalog\" hidden")
                .contains("id=\"panel-operations\" hidden");
    }

    /**
     * The metrics tab is purpose-built on three declared actions and must not invent a fourth.
     * Naming them here means a rename on the service side fails visibly rather than leaving a
     * tab that refuses at the first click.
     */
    @Test
    void theMetricsTabDrivesTheDeclaredMetricActions() throws Exception {
        String page = get(console, "/").body();
        assertThat(page).contains("'describe-mapping'")
                .contains("'query-metrics'")
                .contains("'rebuild-rollup'");
        // The query is built from the mapping's own members, so the roles it splits on are
        // the enum names the service returns.
        assertThat(page).contains("MEMBER_ROLE_MEASURE").contains("MEMBER_ROLE_DIMENSION");
    }

    /** The catalog form is rendered from each action's declared schema, not from a hardcoded list. */
    @Test
    void theCatalogRendersFromTheDeclaredInputSchema() throws Exception {
        String page = get(console, "/").body();
        assertThat(page).contains("inputSchema").contains("schema.required");
    }

    /**
     * The guard on the tab restructure: everything the page drove before still has its element
     * and still names the same action, so moving surfaces into tabs removed no functionality.
     */
    @Test
    void theSurfacesThatExistedBeforeTheTabsSurvive() throws Exception {
        String page = get(console, "/").body();
        assertThat(page).contains("'replay-documents'").contains("'list-jobs'");
        for (String id : new String[] {"searchForm", "subject", "lane", "k", "query", "hits",
                "status", "replayForm", "replayWorkflow", "replayDrive", "replayAccount",
                "jobsForm", "opsOut", "jobs", "loginForm", "credential", "signout"}) {
            assertThat(page).as("element " + id).contains("id=\"" + id + "\"");
        }
        assertThat(page).contains("/subjects").contains("/search").contains("/session");
    }

    /** A metrics call reaches the actions route by the same proxy the operations panel uses. */
    @Test
    void metricActionsReachTheActionsRouteThroughTheSameProxy() throws Exception {
        HttpResponse<String> described = post(console, "/actions/describe-mapping",
                "{\"mappingSubject\":\"repo-document\"}");
        assertThat(described.statusCode()).isEqualTo(200);
        assertThat(json.readTree(described.body()).get("mappingSubject").asText())
                .isEqualTo("repo-document");
    }

    @Test
    void subjectsBridgeTheServiceSurfaceAsJson() throws Exception {
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
    void serviceRefusalsPassThroughVerbatimWithHonestStatusCodes() throws Exception {
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

    // ------------------------------------------------------------ secured console

    static HttpResponse<String> getWithCookie(String path, String cookie) throws Exception {
        HttpRequest.Builder request = HttpRequest.newBuilder(
                URI.create("http://127.0.0.1:" + guarded.port() + path)).GET();
        if (cookie != null) {
            request.header("Cookie", cookie);
        }
        return client.send(request.build(), HttpResponse.BodyHandlers.ofString());
    }

    static HttpResponse<String> postWithCookie(String path, String body, String cookie)
            throws Exception {
        HttpRequest.Builder request = HttpRequest.newBuilder(
                        URI.create("http://127.0.0.1:" + guarded.port() + path))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body));
        if (cookie != null) {
            request.header("Cookie", cookie);
        }
        return client.send(request.build(), HttpResponse.BodyHandlers.ofString());
    }

    static String signIn(String credential) throws Exception {
        HttpResponse<String> login = client.send(HttpRequest.newBuilder(
                        URI.create("http://127.0.0.1:" + guarded.port() + "/session"))
                        .POST(HttpRequest.BodyPublishers.ofString(credential)).build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(login.statusCode()).isEqualTo(200);
        String setCookie = login.headers().firstValue("set-cookie").orElseThrow();
        assertThat(setCookie).contains("HttpOnly");
        return setCookie.substring(0, setCookie.indexOf(';'));
    }

    @Test
    void theGuardedConsoleRefusesEveryBridgeWithoutASession() throws Exception {
        assertThat(getWithCookie("/", null).statusCode()).isEqualTo(200);
        assertThat(getWithCookie("/health", null).statusCode()).isEqualTo(200);
        HttpResponse<String> session = getWithCookie("/session", null);
        assertThat(session.statusCode()).isEqualTo(401);
        assertThat(session.body()).contains("\"loginRequired\":true");
        assertThat(getWithCookie("/subjects", null).statusCode()).isEqualTo(401);
        assertThat(postWithCookie("/search", "{}", null).statusCode()).isEqualTo(401);
        assertThat(getWithCookie("/actions", null).statusCode()).isEqualTo(401);
    }

    @Test
    void aPolicyPrincipalSignsInSearchesAndItsCredentialRidesTheCall() throws Exception {
        String cookie = signIn("querier-credential");
        assertThat(getWithCookie("/session", cookie).body())
                .contains("\"authenticated\":true");
        assertThat(getWithCookie("/subjects", cookie).statusCode()).isEqualTo(200);
        lastCallApiToken.set(null);
        HttpResponse<String> hits = postWithCookie("/search",
                "{\"mappingSubject\":\"repo-document\",\"query\":\"first\",\"k\":1,"
                        + "\"lane\":\"SEARCH_LANE_LEXICAL\"}", cookie);
        assertThat(hits.statusCode()).isEqualTo(200);
        assertThat(lastCallApiToken.get()).isEqualTo("querier-credential");
    }

    @Test
    void aPrincipalWithoutSearchQueryIsRefusedByName() throws Exception {
        String cookie = signIn("rebuilder-credential");
        HttpResponse<String> refused = postWithCookie("/search",
                "{\"mappingSubject\":\"repo-document\",\"query\":\"x\",\"k\":1}", cookie);
        assertThat(refused.statusCode()).isEqualTo(403);
        assertThat(json.readTree(refused.body()).get("error").asText())
                .contains("rebuilder").contains("search-query");
        assertThat(getWithCookie("/subjects", cookie).statusCode()).isEqualTo(403);
    }

    @Test
    void theActionsProxyPresentsTheSessionsOwnCredential() throws Exception {
        String cookie = signIn("querier-credential");
        lastActionsApiToken.set(null);
        HttpResponse<String> echoed = postWithCookie("/actions/echo-input", "{\"a\":1}", cookie);
        assertThat(echoed.statusCode()).isEqualTo(200);
        assertThat(lastActionsApiToken.get()).isEqualTo("querier-credential");
    }

    @Test
    void unknownCredentialsNeverGetASession() throws Exception {
        HttpResponse<String> refused = client.send(HttpRequest.newBuilder(
                        URI.create("http://127.0.0.1:" + guarded.port() + "/session"))
                        .POST(HttpRequest.BodyPublishers.ofString("guessed")).build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(refused.statusCode()).isEqualTo(401);
        assertThat(refused.headers().firstValue("set-cookie")).isEmpty();
    }

    @Test
    void signingOutEndsTheSession() throws Exception {
        String cookie = signIn("querier-credential");
        assertThat(getWithCookie("/subjects", cookie).statusCode()).isEqualTo(200);
        HttpRequest signOut = HttpRequest.newBuilder(
                        URI.create("http://127.0.0.1:" + guarded.port() + "/session"))
                .header("Cookie", cookie).DELETE().build();
        assertThat(client.send(signOut, HttpResponse.BodyHandlers.ofString())
                .statusCode()).isEqualTo(204);
        assertThat(getWithCookie("/subjects", cookie).statusCode()).isEqualTo(401);
    }
}
