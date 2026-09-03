package ai.protomolt.proto.acquire.confluence;

import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * An in-process fake of the Confluence Cloud REST v2 API over
 * {@link HttpServer}, same idiom as the schema-registry stub tests: stubs are
 * keyed by request path (optionally {@code path?query}), requests are
 * recorded for assertions, and one-shot stubs model a 429-then-success
 * exchange. No live Atlassian calls anywhere.
 */
final class FakeConfluenceServer implements AutoCloseable {

    /** One canned response. */
    record Stub(int status, String body, Map<String, String> headers) {
        static Stub json(String body) {
            return new Stub(200, body, Map.of());
        }
    }

    /** One request the fake received. */
    record RecordedRequest(String method, String path, String query, String authorization) {
    }

    private final HttpServer server;
    private final Map<String, Stub> stubs = new ConcurrentHashMap<>();
    private final Map<String, Queue<Stub>> onceStubs = new ConcurrentHashMap<>();
    private final List<RecordedRequest> requests = new CopyOnWriteArrayList<>();

    private FakeConfluenceServer(HttpServer server) {
        this.server = server;
    }

    static FakeConfluenceServer start() throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        FakeConfluenceServer fake = new FakeConfluenceServer(server);
        server.createContext("/", exchange -> {
            try {
                fake.handle(exchange);
            } finally {
                exchange.close();
            }
        });
        server.start();
        return fake;
    }

    /** The base URL the crawler config takes, with the Cloud {@code /wiki} suffix. */
    String baseUrl() {
        return "http://127.0.0.1:" + server.getAddress().getPort() + "/wiki";
    }

    /** Stubs a JSON 200 for a path (or {@code path?query} when the query matters). */
    void stub(String pathOrPathQuery, String json) {
        stubs.put(pathOrPathQuery, Stub.json(json));
    }

    void stub(String pathOrPathQuery, Stub stub) {
        stubs.put(pathOrPathQuery, stub);
    }

    /** A response used exactly once for the path, before the permanent stub applies. */
    void stubOnce(String path, Stub stub) {
        onceStubs.computeIfAbsent(path, k -> new ConcurrentLinkedQueue<>()).add(stub);
    }

    List<RecordedRequest> requests() {
        return List.copyOf(requests);
    }

    List<RecordedRequest> requestsTo(String path) {
        return requests.stream().filter(r -> r.path().equals(path)).toList();
    }

    private void handle(com.sun.net.httpserver.HttpExchange exchange) throws IOException {
        String path = exchange.getRequestURI().getPath();
        String query = exchange.getRequestURI().getRawQuery();
        requests.add(new RecordedRequest(exchange.getRequestMethod(), path,
                query == null ? "" : query,
                exchange.getRequestHeaders().getFirst("authorization")));

        Stub stub = null;
        Queue<Stub> once = onceStubs.get(path);
        if (once != null) {
            stub = once.poll();
        }
        if (stub == null && query != null) {
            stub = stubs.get(path + "?" + query);
        }
        if (stub == null) {
            stub = stubs.get(path);
        }
        if (stub == null) {
            stub = new Stub(404, "{\"message\":\"not stubbed: " + path + "\"}", Map.of());
        }

        byte[] body = stub.body().getBytes(StandardCharsets.UTF_8);
        stub.headers().forEach((name, value) -> exchange.getResponseHeaders().add(name, value));
        if (stub.status() == 200) {
            exchange.getResponseHeaders().add("content-type", "application/json");
        }
        exchange.sendResponseHeaders(stub.status(), body.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(body);
        }
    }

    @Override
    public void close() {
        server.stop(0);
    }
}
