package ai.pipestream.proto.search.console;

import ai.pipestream.proto.search.v1.ListSubjectsRequest;
import ai.pipestream.proto.search.v1.SearchRequest;
import ai.pipestream.proto.search.v1.SearchResponse;
import ai.pipestream.proto.search.v1.SearchServiceGrpc;
import com.google.protobuf.util.JsonFormat;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.regex.Pattern;

/**
 * The search console: one pure-JDK HTTP server, one page, no build step (the playground
 * idiom). The page's fetches land here and bridge onto gRPC and HTTP peers:
 *
 * <ul>
 *   <li>{@code GET /} — the page</li>
 *   <li>{@code GET /subjects} — the service's served surface, proto3 JSON</li>
 *   <li>{@code POST /search} — a proto3-JSON {@code SearchRequest} in, hits out; the service's
 *       refusals pass through verbatim (they are written for humans)</li>
 *   <li>{@code GET /actions}, {@code POST /actions/<name>} — same-origin proxy onto the
 *       registry's actions route, so the operations panel needs no CORS; answers 503 when the
 *       console was mounted without an actions target</li>
 * </ul>
 */
public final class SearchConsoleServer implements AutoCloseable {

    /** In-process target prefix, shared vocabulary with the composer: {@value}. */
    public static final String INPROCESS_TARGET_PREFIX = "inprocess:";

    private static final Logger LOG = LoggerFactory.getLogger(SearchConsoleServer.class);
    private static final Pattern ACTION_NAME = Pattern.compile("[a-z][a-z0-9-]*");

    private final HttpServer http;
    private final ManagedChannel channel;
    private final SearchServiceGrpc.SearchServiceBlockingStub service;
    private final Supplier<String> actionsBaseUrl;
    private final HttpClient actionsClient = HttpClient.newHttpClient();

    /**
     * Creates the server (not yet started).
     *
     * @param port the HTTP port (0 for ephemeral)
     * @param serviceTarget the search service: a {@code host:port} authority or
     *        {@code inprocess:<name>}
     * @param actionsBaseUrl supplies the registry actions route to proxy operations to, e.g.
     *        {@code http://127.0.0.1:8081/protomolt/actions} — a supplier because a co-mounted
     *        registry's port is only known once it starts; a blank answer disables the panel
     */
    public SearchConsoleServer(int port, String serviceTarget, Supplier<String> actionsBaseUrl)
            throws IOException {
        if (serviceTarget == null || serviceTarget.isBlank()) {
            throw new IllegalArgumentException("serviceTarget must not be blank");
        }
        this.channel = serviceTarget.startsWith(INPROCESS_TARGET_PREFIX)
                ? InProcessChannelBuilder.forName(
                        serviceTarget.substring(INPROCESS_TARGET_PREFIX.length())).build()
                : NettyChannelBuilder.forTarget(serviceTarget).usePlaintext().build();
        this.service = SearchServiceGrpc.newBlockingStub(channel);
        this.actionsBaseUrl = actionsBaseUrl == null ? () -> "" : actionsBaseUrl;
        this.http = HttpServer.create(new InetSocketAddress(port), 0);
        http.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        http.createContext("/", this::servePage);
        http.createContext("/subjects", this::serveSubjects);
        http.createContext("/search", this::serveSearch);
        http.createContext("/actions", this::serveActions);
        http.createContext("/health", exchange ->
                respond(exchange, 200, "text/plain; charset=utf-8", "ok"));
    }

    /** Starts serving and returns the bound port. */
    public int start() {
        http.start();
        return http.getAddress().getPort();
    }

    /** The bound port. */
    public int port() {
        return http.getAddress().getPort();
    }

    @Override
    public void close() {
        http.stop(0);
        channel.shutdownNow();
        try {
            channel.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // ---------------------------------------------------------------- handlers

    private void servePage(HttpExchange exchange) throws IOException {
        if (!"/".equals(exchange.getRequestURI().getPath())) {
            respond(exchange, 404, "text/plain; charset=utf-8", "not found");
            return;
        }
        respond(exchange, 200, "text/html; charset=utf-8", SearchConsolePage.PAGE);
    }

    private void serveSubjects(HttpExchange exchange) throws IOException {
        try {
            String json = JsonFormat.printer().print(
                    service.listSubjects(ListSubjectsRequest.getDefaultInstance()));
            respond(exchange, 200, "application/json; charset=utf-8", json);
        } catch (StatusRuntimeException e) {
            respondServiceError(exchange, e);
        }
    }

    private void serveSearch(HttpExchange exchange) throws IOException {
        if (!"POST".equals(exchange.getRequestMethod())) {
            respond(exchange, 405, "text/plain; charset=utf-8", "POST only");
            return;
        }
        String body = new String(
                exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
        SearchRequest.Builder request = SearchRequest.newBuilder();
        try {
            JsonFormat.parser().merge(body, request);
        } catch (IOException e) {
            respond(exchange, 400, "application/json; charset=utf-8",
                    errorJson("the request is not a valid SearchRequest: " + e.getMessage()));
            return;
        }
        try {
            SearchResponse hits = service.search(request.build());
            respond(exchange, 200, "application/json; charset=utf-8",
                    JsonFormat.printer().print(hits));
        } catch (StatusRuntimeException e) {
            respondServiceError(exchange, e);
        }
    }

    private void serveActions(HttpExchange exchange) throws IOException {
        String base;
        try {
            base = stripTrailingSlash(actionsBaseUrl.get());
        } catch (RuntimeException e) {
            base = "";
        }
        if (base.isBlank()) {
            respond(exchange, 503, "application/json; charset=utf-8",
                    errorJson("the operations panel is not configured on this console"));
            return;
        }
        String path = exchange.getRequestURI().getPath();
        try {
            if ("/actions".equals(path) && "GET".equals(exchange.getRequestMethod())) {
                forward(exchange, HttpRequest.newBuilder(URI.create(base)).GET());
                return;
            }
            String name = path.startsWith("/actions/") ? path.substring("/actions/".length()) : "";
            if (!ACTION_NAME.matcher(name).matches()
                    || !"POST".equals(exchange.getRequestMethod())) {
                respond(exchange, 404, "text/plain; charset=utf-8", "not found");
                return;
            }
            forward(exchange, HttpRequest.newBuilder(URI.create(base + "/" + name))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofByteArray(
                            exchange.getRequestBody().readAllBytes())));
        } catch (IOException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            respond(exchange, 502, "application/json; charset=utf-8",
                    errorJson("the actions route did not answer: " + e.getMessage()));
        }
    }

    private void forward(HttpExchange exchange, HttpRequest.Builder request)
            throws IOException, InterruptedException {
        HttpResponse<byte[]> answer = actionsClient.send(
                request.build(), HttpResponse.BodyHandlers.ofByteArray());
        exchange.getResponseHeaders().set("Content-Type",
                answer.headers().firstValue("Content-Type")
                        .orElse("application/json; charset=utf-8"));
        exchange.sendResponseHeaders(answer.statusCode(), answer.body().length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(answer.body());
        }
    }

    /** The service's refusals reach the page verbatim; they are written for humans. */
    private void respondServiceError(HttpExchange exchange, StatusRuntimeException e)
            throws IOException {
        int status = switch (e.getStatus().getCode()) {
            case INVALID_ARGUMENT -> 400;
            case FAILED_PRECONDITION -> 409;
            case UNAVAILABLE -> 503;
            default -> 502;
        };
        if (status == 502) {
            LOG.error("search service call failed", e);
        }
        String description = e.getStatus().getDescription();
        respond(exchange, status, "application/json; charset=utf-8", errorJson(
                description == null ? e.getStatus().getCode().toString() : description));
    }

    private static String errorJson(String message) {
        return "{\"error\":" + quote(message) + "}";
    }

    private static String quote(String value) {
        StringBuilder quoted = new StringBuilder("\"");
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '"' -> quoted.append("\\\"");
                case '\\' -> quoted.append("\\\\");
                case '\n' -> quoted.append("\\n");
                case '\r' -> quoted.append("\\r");
                case '\t' -> quoted.append("\\t");
                default -> {
                    if (c < 0x20) {
                        quoted.append(String.format("\\u%04x", (int) c));
                    } else {
                        quoted.append(c);
                    }
                }
            }
        }
        return quoted.append('"').toString();
    }

    private static String stripTrailingSlash(String url) {
        if (url == null) {
            return "";
        }
        return url.endsWith("/") ? url.substring(0, url.length() - 1) : url;
    }

    private static void respond(HttpExchange exchange, int status, String contentType,
                                String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", contentType);
        exchange.getResponseHeaders().set("Cache-Control", "no-cache");
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
        }
    }
}
